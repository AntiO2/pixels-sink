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

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// We extend FreshnessClient to access the protected queryAndCalculateFreshness method
public class TestFreshnessClient
{

    // Mocks for JDBC dependencies
    private Connection mockConnection;
    private Statement mockStatement;
    private ResultSet mockResultSet;

    // Mocks for utility/config dependencies
    private PixelsSinkConfig mockConfig;
    private MetricsFacade mockMetricsFacade;
    private FreshnessClient client; // The instance of the client to test

    @BeforeAll
    public static void setUp() throws IOException
    {
        // Initialization as per the user's template
        PixelsSinkConfigFactory.initialize("/home/ubuntu/disk1/opt/pixels-sink/conf/pixels-sink.hudi.properties");
    }

    @Test
    public void testFreshnessCalculationSuccess() throws Exception
    {

        FreshnessClient freshnessClient = FreshnessClient.getInstance();
        freshnessClient.addMonitoredTable("customer");
        freshnessClient.start();
        while (true)
        {
        }
    }

    @Test
    public void testSnapshotTs() throws SQLException
    {
        FreshnessClient freshnessClient = FreshnessClient.getInstance();
        Connection connection = freshnessClient.createNewConnection(123456L);
        String query = String.format("SELECT max(freshness_ts) FROM company");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        resultSet.next();
    }

    @Test
    public void testLoanTransQueryPerformance() throws SQLException
    {
        FreshnessClient freshnessClient = FreshnessClient.getInstance();
        Connection connection = freshnessClient.createNewConnection(12345689100L);
        String query = "SELECT max(freshness_ts) FROM nation";
        String csvFileName = "loantrans_query_results.csv";
        int iterations = 1000;
        try (PrintWriter writer = new PrintWriter(new FileWriter(csvFileName)))
        {

            for (int i = 0; i < iterations; i++)
            {
                long startTime = System.currentTimeMillis();
                long startNano = System.nanoTime();

                long maxFreshnessTs = 0;
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(query))
                {

                    if (resultSet.next())
                    {
                        maxFreshnessTs = resultSet.getLong(1);
                    }
                } catch (SQLException e)
                {
                    System.err.println("Query failed at iteration " + i + ": " + e.getMessage());
                }

                long endNano = System.nanoTime();
                long durationMs = (endNano - startNano) / 1_000_000;
                writer.printf("%d,%d,%d%n", startTime, maxFreshnessTs, durationMs);
                writer.flush();
            }
            System.out.println("Test completed. Results saved to: " + csvFileName);
        } catch (IOException e)
        {
            e.printStackTrace();
        } finally
        {
            if (connection != null && !connection.isClosed())
            {
                connection.close();
            }
        }
    }
}