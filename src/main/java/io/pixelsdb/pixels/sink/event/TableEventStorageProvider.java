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


package io.pixelsdb.pixels.sink.event;


import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.processor.MetricsFacade;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * @package: io.pixelsdb.pixels.sink.event
 * @className: TableEventStorageProvider
 * @author: AntiO2
 * @date: 2025/9/26 10:45
 */
public class TableEventStorageProvider implements TableEventProvider {
    private final Logger LOGGER = Logger.getLogger(TableEventStorageProvider.class.getName());
    private final SchemaTableName schemaTableName;

    private final BlockingQueue<ByteBuffer> rawEventQueue = new LinkedBlockingQueue<>(10000);
    private final BlockingQueue<RowChangeEvent> eventQueue = new LinkedBlockingQueue<>(10000);
    private final Thread processorThread;

    private final MetricsFacade metricsFacade;

    private static final int BATCH_SIZE = 64;
    private static final int THREAD_NUM = 4;
    private static final long MAX_WAIT_MS = 5; // configurable
    private final ExecutorService decodeExecutor = Executors.newFixedThreadPool(THREAD_NUM);

    public TableEventStorageProvider(SchemaTableName schemaTableName)
    {
        this.schemaTableName = schemaTableName;
        this.processorThread = new Thread(this::processLoop, "TableEventStorageProvider-" + schemaTableName.getTableName());
        this.metricsFacade = MetricsFacade.getInstance();
    }

    public TableEventStorageProvider(int tableId)
    {
        this.schemaTableName = null;
        this.processorThread = new Thread(this::processLoop, "TableEventStorageProvider-" + tableId);
        this.metricsFacade = MetricsFacade.getInstance();
    }

    @Override
    public BlockingQueue<RowChangeEvent> getSourceEventQueue()
    {
        return eventQueue;
    }

    public void start() {
        processorThread.start();
    }

    public void put(ByteBuffer record) {
        try {
            rawEventQueue.put(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processLoop() {
        List<ByteBuffer> batch = new ArrayList<>(BATCH_SIZE);

        while (true) {
            try {
                batch.clear();

                // take first element (blocking)
                ByteBuffer first = rawEventQueue.take();
                batch.add(first);
                long startTime = System.nanoTime();

                // keep polling until batch full or timeout
                while (batch.size() < BATCH_SIZE) {
                    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
                    long remainingMs = MAX_WAIT_MS - elapsedMs;
                    if (remainingMs <= 0) {
                        break;
                    }

                    ByteBuffer next = rawEventQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
                    if (next == null) {
                        break;
                    }
                    batch.add(next);
                }

                // parallel decode
                List<Future<RowChangeEvent>> futures = new ArrayList<>(batch.size());
                for (ByteBuffer data : batch) {
                    futures.add(decodeExecutor.submit(() -> {
                        try {
                            SinkProto.RowRecord rowRecord = SinkProto.RowRecord.parseFrom(data);
                            return RowChangeEventStructDeserializer.convertToRowChangeEvent(rowRecord);
                        } catch (InvalidProtocolBufferException e) {
                            throw new RuntimeException(e);
                        } catch (SinkException e) {
                            LOGGER.warning(e.getMessage());
                            return null;
                        }
                    }));
                }

                // ordered put into queue
                for (Future<RowChangeEvent> future : futures) {
                    try {
                        RowChangeEvent event = future.get();
                        if (event != null) {
                            metricsFacade.recordSerdRowChange();
                            eventQueue.put(event);
                        }
                    } catch (ExecutionException e) {
                        LOGGER.warning("Decode failed: " + e.getCause());
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

}
