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
 
package io.pixelsdb.pixels.sink.writer.flink;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.util.FlushRateLimiter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * FlinkPollingWriter is a PixelsSinkWriter implementation designed for a long-polling pattern.
 * It maintains in-memory blocking queues per table, acting as a buffer between the upstream
 * data source (producer) and the gRPC service (consumer).
 * This class is thread-safe and integrates FlushRateLimiter to control ingress traffic.
 * It also manages the lifecycle of the gRPC server.
 */
public class FlinkPollingWriter implements PixelsSinkWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkPollingWriter.class);

    // Core data structure: A thread-safe map from table name to a thread-safe blocking queue.
    private final Map<String, BlockingQueue<SinkProto.RowRecord>> tableQueues;

    // Ingress rate limiter to control the data writing speed.
    private final FlushRateLimiter rateLimiter;

    // The gRPC server instance managed by this writer.
    private final PollingRpcServer pollingRpcServer;

    /**
     * Constructor for FlinkPollingWriter.
     * Initializes the data structures, rate limiter, and starts the gRPC server.
     */
    public FlinkPollingWriter() {
        this.tableQueues = new ConcurrentHashMap<>();
        // Get the global RateLimiter instance
        this.rateLimiter = FlushRateLimiter.getInstance();
        LOGGER.info("FlinkPollingWriter initialized with FlushRateLimiter.");

        // --- START: New logic to initialize and start the gRPC server ---
        try {
            // 1. Get configuration
            PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
            int rpcPort = config.getSinkFlinkServerPort();
            // 2. Create the gRPC service implementation first, passing a reference to this writer.
            PixelsPollingServiceImpl service = new PixelsPollingServiceImpl(this);
            
            // 3. Create the PollingRpcServer instance with the service and port.
            LOGGER.info("Attempting to start gRPC Polling Server on port {}...", rpcPort);
            this.pollingRpcServer = new PollingRpcServer(service, rpcPort);
            // 4. Start the server.
            this.pollingRpcServer.start();
            LOGGER.info("gRPC Polling Server successfully started and is managed by FlinkPollingWriter.");
        } catch (IOException e) {
            // If the server fails to start, the writer cannot function.
            // Throw a RuntimeException to fail the Flink task initialization.
            LOGGER.error("Failed to start gRPC server during FlinkPollingWriter initialization.", e);
            throw new RuntimeException("Could not start gRPC server", e);
        }
        // --- END: New logic ---
    }

    /**
     * [Producer side] Receives row change events from the data source, applies rate limiting,
     * converts them, and places them into the in-memory queue.
     *
     * @param event The row change event
     * @return always returns true, unless an interruption occurs.
     */
    @Override
    public boolean writeRow(RowChangeEvent event) {
        if (event == null) {
            LOGGER.warn("Received a null RowChangeEvent, skipping.");
            return false;
        }

        try {
            // 1. Acquire a token to respect the rate limit before processing data.
            //    This is a blocking operation that will effectively control the upstream write speed.
            rateLimiter.acquire(1);

            // 2. Convert Flink's RowChangeEvent to the gRPC RowRecord Protobuf object
            SinkProto.RowRecord rowRecord = event.getRowRecord();

            // 3. Find the corresponding queue for the table name, creating a new one atomically if it doesn't exist.
            BlockingQueue<SinkProto.RowRecord> queue = tableQueues.computeIfAbsent(
                    event.getFullTableName(),
                    k -> new LinkedBlockingQueue<>() // Default to an unbounded queue
            );

            // 4. Put the converted record into the queue.
            queue.put(rowRecord);

            LOGGER.debug("Enqueued a row for table '{}'. Queue size is now {}.", event.getFullTableName(), queue.size());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
            LOGGER.error("Thread was interrupted while writing row for table: " + event.getFullTableName(), e);
            return false;
        } catch (Exception e) {
            LOGGER.error("Failed to process and write row for table: " + event.getFullTableName(), e);
            return false;
        }

        return true;
    }

    /**
     * [Consumer side] The gRPC service calls this method to pull data.
     * Implements long-polling logic: if the queue is empty, it blocks for a specified timeout.
     * batchSize acts as an upper limit on the number of records pulled to prevent oversized RPC responses.
     *
     * @param tableName  The name of the table to pull data from
     * @param batchSize  The maximum number of records to pull
     * @param timeout    The maximum time to wait for data
     * @param unit       The time unit for the timeout
     * @return A list of RowRecords, which will be empty if no data is available before the timeout.
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public List<SinkProto.RowRecord> pollRecords(String tableName, int batchSize, long timeout, TimeUnit unit)
            throws InterruptedException {
        List<SinkProto.RowRecord> records = new ArrayList<>(batchSize);
        BlockingQueue<SinkProto.RowRecord> queue = tableQueues.get(tableName);

        if (queue == null) {
            // If the queue doesn't exist yet, wait for the specified timeout to simulate polling behavior.
            unit.sleep(timeout);
            return records;
        }

        // Wait for the first record for up to the timeout period.
        SinkProto.RowRecord firstRecord = queue.poll(timeout, unit);
        if (firstRecord == null) {
            // Timeout occurred, no records available.
            return records;
        }

        // At least one record was found, add it to the list.
        records.add(firstRecord);
        // Drain any remaining records up to the batch size limit without blocking.
        queue.drainTo(records, batchSize - 1);

        LOGGER.info("Polled {} records for table '{}'.", records.size(), tableName);
        return records;
    }

    /**
     * This implementation does not involve transactions, so this method is a no-op.
     */
    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata) {
        return true;
    }

    /**
     * This implementation uses an in-memory queue, so data is immediately available. flush is a no-op.
     */
    @Override
    public void flush() {
        // No-op
    }

    /**
     * Cleans up resources on close. This is where we stop the gRPC server.
     */
    @Override
    public void close() throws IOException 
    {
        LOGGER.info("Closing FlinkPollingWriter...");
        if (this.pollingRpcServer != null) {
            LOGGER.info("Attempting to shut down the gRPC Polling Server...");
            this.pollingRpcServer.stop();
            LOGGER.info("gRPC Polling Server shut down.");
        }
        LOGGER.info("Clearing all table queues.");
        tableQueues.clear();
        LOGGER.info("FlinkPollingWriter closed.");
    }
}
