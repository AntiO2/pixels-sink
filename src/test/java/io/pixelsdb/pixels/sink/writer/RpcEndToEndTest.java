/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.pixelsdb.pixels.sink.writer;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pixelsdb.pixels.sink.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.source.SinkSource;
import io.pixelsdb.pixels.sink.source.SinkSourceFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.retina.SinkContextManager;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RpcEndToEndTest
{
    private static final int TEST_PORT = 9091;
    private static final int RECORD_COUNT = 5;
    private static final String TEST_SCHEMA = "pixels_bench_sf10x";
    private static final String TEST_TABLE = "savingaccount";
    private static final String FULL_TABLE_NAME = TEST_SCHEMA + "." + TEST_TABLE;
    private static final String CONFIG_FILE_PATH = "conf/pixels-sink.aws.properties";
    private static SinkSource sinkSource; // Keep a reference to stop it later
    private static HTTPServer prometheusHttpServer;

    // This method contains the setup logic from PixelsSinkApp
    private static void startServer()
    {
        System.out.println("[SETUP] Starting server with full initialization sequence...");
        try
        {
            // === 1. Mimic init() method from PixelsSinkApp ===
            System.out.println("[SETUP] Initializing configuration from " + CONFIG_FILE_PATH + "...");
            PixelsSinkConfigFactory.initialize(CONFIG_FILE_PATH);

            System.out.println("[SETUP] Initializing MetricsFacade...");
            MetricsFacade.getInstance().setSinkContextManager(SinkContextManager.getInstance());

            // For determinism in testing, override the port from the config file
            System.setProperty("sink.flink.server.port", String.valueOf(TEST_PORT));
            // === 2. Mimic main() method from PixelsSinkApp ===
            PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
            System.out.println("[SETUP] Creating SinkSource application engine...");
            sinkSource = SinkSourceFactory.createSinkSource();
            System.out.println("[SETUP] Setting up Prometheus monitoring server...");
            if (config.isMonitorEnabled())
            {
                DefaultExports.initialize();
                // To avoid port conflicts during tests, you could use port 0 for a random port
                // For this example, we'll assume the config port is fine.
                prometheusHttpServer = new HTTPServer(config.getMonitorPort());
                System.out.println("[SETUP] Prometheus server started on port: " + config.getMonitorPort());
            }
            // === 3. THE MOST CRITICAL STEP: Start the processor ===
            System.out.println("[SETUP] Starting SinkSource processor threads...");
            sinkSource.start();
            System.out.println("[SETUP] Server is fully initialized and running.");
        } catch (IOException e)
        {
            // If setup fails, we cannot run the test.
            throw new RuntimeException("Failed to start the server for the test", e);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException
    {
        // ========== 1. SETUP PHASE ==========
        // Start the server components within this same process.
        startServer();

        // Give the server a moment to fully start up and bind to the port.
        Thread.sleep(2000); // 2 seconds, adjust if needed
        // [REFACTORED] To ensure the FlinkPollingWriter uses our test port,
        // we set it as a system property before the writer is created.
        // The PixelsSinkConfigFactory should be configured to read this property.
        System.setProperty("sink.flink.server.port", String.valueOf(TEST_PORT));

        // [REFACTORED] The setup is now dramatically simpler.
        // Instantiating FlinkPollingWriter is the ONLY step needed.
        // Its constructor automatically creates the service and starts the gRPC server.
        System.out.println("[SETUP] Initializing FlinkPollingWriter, which will start the gRPC server...");
        PixelsSinkWriter writer = PixelsSinkWriterFactory.getWriter();

        // [REFACTORED] The following lines are no longer needed and have been removed:
        // PixelsPollingServiceImpl serviceImpl = new PixelsPollingServiceImpl(writer);
        // PollingRpcServer server = new PollingRpcServer(serviceImpl, TEST_PORT);
        // server.start();

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try
        {
            // 3. Start the mock data producer (in a separate thread)
            executor.submit(() ->
            {
                try
                {
                    System.out.println("[PRODUCER] Starting mock data producer...");
                    // Simulate producing 5 INSERT records
                    for (int i = 1; i <= 5; i++)
                    {
                        Thread.sleep(1000); // Simulate data production interval

                        SinkProto.SourceInfo sourceInfo = SinkProto.SourceInfo.newBuilder()
                                .setDb(TEST_SCHEMA)
                                .setTable(TEST_TABLE)
                                .build();

                        byte[] idBytes = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
                        SinkProto.ColumnValue idColumnValue = SinkProto.ColumnValue.newBuilder()
                                .setValue(ByteString.copyFrom(idBytes))
                                .build();

                        SinkProto.RowValue afterImage = SinkProto.RowValue.newBuilder()
                                .addValues(idColumnValue)
                                .build();

                        SinkProto.RowRecord record = SinkProto.RowRecord.newBuilder()
                                .setSource(sourceInfo)
                                .setOp(SinkProto.OperationType.INSERT)
                                .setAfter(afterImage)
                                .build();

                        RowChangeEvent event = new RowChangeEvent(record);
                        System.out.printf("[PRODUCER] Writing INSERT record %d for table '%s.%s'%n", i, TEST_SCHEMA, TEST_TABLE);
                        writer.writeRow(event);
                    }
                    System.out.println("[PRODUCER] Finished writing data.");
                } catch (InterruptedException e)
                {
                    System.err.println("[PRODUCER] Producer thread was interrupted.");
                    Thread.currentThread().interrupt();
                } catch (SinkException e)
                {
                    System.err.println("[PRODUCER] SinkException occurred: %s" + e.getMessage());
                }
            });

            // 4. Start the gRPC client (in another thread)
            executor.submit(() ->
            {
                System.out.println("[CLIENT] Starting gRPC client...");
                ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", TEST_PORT)
                        .usePlaintext()
                        .build();

                try
                {
                    PixelsPollingServiceGrpc.PixelsPollingServiceBlockingStub client = PixelsPollingServiceGrpc.newBlockingStub(channel);
                    int recordsPolled = 0;
                    int maxRetries = 10;
                    int retryCount = 0;
                    while (recordsPolled < 5 && retryCount < maxRetries)
                    {
                        System.out.println("[CLIENT] Sending poll request for table '" + FULL_TABLE_NAME + "'...");
                        SinkProto.PollRequest request = SinkProto.PollRequest.newBuilder()
                                .setSchemaName(TEST_SCHEMA)
                                .setTableName(TEST_TABLE)
                                .build();
                        SinkProto.PollResponse response = client.pollEvents(request);

                        if (response.getRecordsCount() > 0)
                        {
                            System.out.printf("[CLIENT] SUCCESS: Polled %d record(s).%n", response.getRecordsCount());
                            response.getRecordsList().forEach(record -> System.out.println("  -> " + record.toString().trim()));
                            recordsPolled += response.getRecordsCount();
                        } else
                        {
                            System.out.println("[CLIENT] Polled 0 records, will retry...");
                            retryCount++;
                        }
                    }
                    if (recordsPolled == 5)
                    {
                        System.out.println("[CLIENT] Test finished successfully. Polled all 5 records.");
                    } else
                    {
                        System.err.println("[CLIENT] Test FAILED. Did not poll all records in time.");
                    }
                } finally
                {
                    channel.shutdownNow();
                }
            });
        } finally
        {
            // 5. Wait for the test to complete and clean up resources
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES))
            {
                System.err.println("[CLEANUP] Test timed out.");
                executor.shutdownNow();
            } else
            {
                System.out.println("[CLEANUP] Test completed.");
            }

            // [REFACTORED] The correct way to stop the server is now by closing the writer.
            writer.close();
            System.out.println("[CLEANUP] FlinkPollingWriter closed and server stopped.");
        }
        // ========== 3. CLEANUP PHASE ==========
        System.out.println("[CLEANUP] Tearing down server components...");
        if (sinkSource != null)
        {
            sinkSource.stopProcessor();
            System.out.println("[CLEANUP] SinkSource processor stopped.");
            // Note: The writer is part of sinkSource, and its resources
            // should be cleaned up by stopProcessor or an equivalent close method.
        }
        if (prometheusHttpServer != null)
        {
            prometheusHttpServer.close();
            System.out.println("[CLEANUP] Prometheus server stopped.");
        }
        System.out.println("[CLEANUP] Test finished.");
    }
}
