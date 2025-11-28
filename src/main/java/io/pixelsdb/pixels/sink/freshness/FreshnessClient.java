/*
 * Copyright 2025 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */

package io.pixelsdb.pixels.sink.freshness;

import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * FreshnessClient is responsible for monitoring data freshness by periodically
 * querying the maximum timestamp from a set of dynamically configured tables via Trino JDBC.
 */
public class FreshnessClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(FreshnessClient.class);

    // Configuration parameters (should ideally be loaded from a config file)
    private final String trinoJdbcUrl = "jdbc:trino://realtime-pixels-coordinator:8080/pixels/pixels_bench_sf10x";
    private final String trinoUser = "pixels";
    private final String trinoPassword = "password";

    // Key modification: Use a thread-safe Set to maintain the list of tables to monitor dynamically.
    private final Set<String> monitoredTables;
    private static final int QUERY_INTERVAL_SECONDS = 1;

    private Connection connection;
    private final ScheduledExecutorService scheduler;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private static volatile FreshnessClient instance;

    private FreshnessClient() {
        // Initializes the set with thread safety wrapper
        this.monitoredTables = Collections.synchronizedSet(new HashSet<>());

        // Initializes a single-threaded scheduler for executing freshness queries
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("Freshness-Client-Scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    public static FreshnessClient getInstance() {
        if (instance == null) {
            // First check: Reduces synchronization overhead once the instance is created
            synchronized (FreshnessClient.class) {
                if (instance == null) {
                    // Second check: Only one thread proceeds to create the instance
                    instance = new FreshnessClient();
                }
            }
        }
        return instance;
    }

    // -------------------------------------------------------------------------------------------------
    // Connection Management
    // -------------------------------------------------------------------------------------------------

    /**
     * Establishes a new JDBC connection to the Trino coordinator.
     * @throws SQLException if the connection cannot be established.
     */
    private void establishConnection() throws SQLException
    {
        LOGGER.info("Attempting to connect to Trino via JDBC: {}", trinoJdbcUrl);
        this.connection = DriverManager.getConnection(trinoJdbcUrl, trinoUser, trinoPassword);
        LOGGER.info("Trino connection established successfully.");
    }

    /**
     * Ensures the current connection is valid, re-establishing it if necessary (closed, null, or invalid).
     * @throws SQLException if connection cannot be re-established.
     */
    private void ensureConnectionValid() throws SQLException {
        if (connection == null || connection.isClosed() || !connection.isValid(5)) {
            closeConnection();
            establishConnection();
        }
    }

    /**
     * Safely closes the current JDBC connection.
     */
    private void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
                LOGGER.info("Trino connection closed.");
            } catch (SQLException e) {
                LOGGER.warn("Error closing Trino connection.", e);
            }
            connection = null;
        }
    }

    // -------------------------------------------------------------------------------------------------
    // Dynamic Table List Management
    // -------------------------------------------------------------------------------------------------

    /**
     * Adds a table name to the monitoring list.
     * This method can be called by external components (e.g., config trigger).
     * @param tableName The name of the table to add.
     */
    public void addMonitoredTable(String tableName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            LOGGER.warn("Attempted to add null or empty table name to freshness monitor.");
            return;
        }
        if (monitoredTables.add(tableName)) {
            LOGGER.info("Table '{}' added to freshness monitor list.", tableName);
        } else {
            LOGGER.debug("Table '{}' is already in the freshness monitor list.", tableName);
        }
    }

    /**
     * Removes a table name from the monitoring list.
     * @param tableName The name of the table to remove.
     */
    public void removeMonitoredTable(String tableName) {
        if (monitoredTables.remove(tableName)) {
            LOGGER.info("Table '{}' removed from freshness monitor list.", tableName);
        } else {
            LOGGER.debug("Table '{}' was not found in the freshness monitor list.", tableName);
        }
    }

    // -------------------------------------------------------------------------------------------------
    // Scheduling and Execution
    // -------------------------------------------------------------------------------------------------

    /**
     * Starts the scheduled freshness monitoring task.
     */
    public void start() {
        try {
            ensureConnectionValid();
            LOGGER.info("Starting Freshness Client, querying every {} seconds.", QUERY_INTERVAL_SECONDS);

            scheduler.scheduleAtFixedRate(this::queryAndCalculateFreshness,
                    0, // Initial delay
                    QUERY_INTERVAL_SECONDS,
                    TimeUnit.SECONDS);
        } catch (SQLException e) {
            LOGGER.error("Failed to establish initial Trino connection. Freshness Client will not start.", e);
        }
    }


    /**
     * Stops the scheduled task and closes the JDBC connection.
     */
    public void stop() {
        LOGGER.info("Stopping Freshness Client.");
        scheduler.shutdownNow();
        closeConnection();
    }

    /**
     * The core scheduled task: queries max(freshness_ts) for all monitored tables
     * and calculates the freshness metric.
     */
    private void queryAndCalculateFreshness() {
        try {
            ensureConnectionValid();
        } catch (SQLException e) {
            LOGGER.error("Connection failed during scheduled run. Skipping this freshness cycle.", e);
            return;
        }

        // Take a snapshot of the tables to monitor for this cycle.
        // This prevents ConcurrentModificationException if a table is added/removed mid-iteration.
        Set<String> tablesSnapshot = new HashSet<>(monitoredTables);

        for (String tableName : tablesSnapshot) {

            // Timestamp when the query is sent (t_send)
            long tSendMillis = System.currentTimeMillis();

            // Query to find the latest timestamp in the table
            // Assumes 'freshness_ts' is a long-type epoch timestamp (milliseconds)
            String query = String.format("SELECT max(freshness_ts) FROM %s", tableName);

            try (Statement statement = connection.createStatement();
                 ResultSet rs = statement.executeQuery(query)) {

                // Timestamp when the result is received (t_receive)
                long tReceiveMillis = System.currentTimeMillis();
                long maxFreshnessTs = 0;

                if (rs.next()) {
                    // Read the maximum timestamp value
                    maxFreshnessTs = rs.getLong(1);
                }

                if (maxFreshnessTs > 0) {
                    // Freshness = t_receive - data_write_time (maxFreshnessTs)
                    // Result is in milliseconds
                    long freshnessMillis = tReceiveMillis - maxFreshnessTs;

                    LOGGER.debug("Table {}: Max Ts: {}, Freshness: {} ms (Query RTT: {} ms)",
                            tableName,
                            maxFreshnessTs,
                            freshnessMillis,
                            tReceiveMillis - tSendMillis);

                    // Record the calculated freshness using the MetricsFacade
                    metricsFacade.recordFreshness(freshnessMillis);

                } else {
                    LOGGER.warn("Table {} returned null or zero max(freshness_ts). Skipping freshness calculation.", tableName);
                }

            } catch (SQLException e) {
                // Handle database errors (e.g., table not found, query syntax error)
                LOGGER.error("Failed to execute query for table {}: {}", tableName, e.getMessage());
            } catch (Exception e) {
                // Catch potential runtime errors (e.g., in MetricsFacade)
                LOGGER.error("Error calculating or recording freshness for table {}.", tableName, e);
            }
        }
    }
}