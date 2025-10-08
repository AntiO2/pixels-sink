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


package io.pixelsdb.pixels.sink.processor;


import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.*;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @package: io.pixelsdb.pixels.sink.processor
 * @className: SinkStorageProcessor
 * @author: AntiO2
 * @date: 2025/10/5 11:43
 */
public class FasterSinkStorageProcessor extends AbstractSinkStorageProcessor implements MainProcessor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FasterSinkStorageProcessor.class);

    private final TransactionEventEngineProvider transactionEventProvider = TransactionEventEngineProvider.INSTANCE;
    private final TableStoragePipelineManager tablePipelineManager = new TableStoragePipelineManager();
    private final TransactionProcessor transactionProcessor = new TransactionProcessor(transactionEventProvider);
    private final Thread transactionProcessorThread;
    private final Thread transAdapterThread;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final Map<Integer, BlockingQueue<CompletableFuture<ByteBuffer>>> queueMap = new ConcurrentHashMap<>();
    private final Map<Integer, Thread> consumerThreads = new ConcurrentHashMap<>();
    private final int maxQueueCapacity = 10000;


    private final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    public FasterSinkStorageProcessor()
    {
        this.transactionProcessorThread = new Thread(transactionProcessor, "debezium-processor");
        this.transAdapterThread = new Thread(this::processTransactionSourceRecord, "transaction-adapter");

    }

    @Override
    ProtoType getProtoType(int i)
    {
        if(i == -1)
        {
            return ProtoType.TRANS;
        }
        return ProtoType.ROW;
    }

    private final CompletableFuture<ByteBuffer> POISON_PILL = new CompletableFuture<>();

    @Override
    public void start()
    {
        this.transactionProcessorThread.start();
        this.transAdapterThread.start();
        ExecutorService transExecutor = Executors.newSingleThreadExecutor();
        ExecutorService rowExecutor = Executors.newSingleThreadExecutor();
        for(String file:files)
        {
            Storage.Scheme scheme = Storage.Scheme.fromPath(file);
            LOGGER.info("Start read from file {}", file);
            try(PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(scheme, file))
            {
                long offset = 0;
                BlockingQueue<Pair<ByteBuffer, CompletableFuture<ByteBuffer>>> rowQueue = new LinkedBlockingQueue<>();
                BlockingQueue<CompletableFuture<ByteBuffer>> transQueue = new LinkedBlockingQueue<>();
                while(true)
                {
                    try {
                        int key, valueLen;
                        reader.seek(offset);
                        try {
                            key = reader.readInt(ByteOrder.BIG_ENDIAN);
                            valueLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                        } catch (IOException e) {
                            // EOF
                            break;
                        }

                        ProtoType protoType = getProtoType(key);
                        offset += Integer.BYTES * 2;
                        CompletableFuture<ByteBuffer> valueFuture = reader.readAsync(offset, valueLen)
                                .thenApply(this::copyToHeap)
                                .thenApply(buf -> buf.order(ByteOrder.BIG_ENDIAN));
                        // move offset for next record
                        offset += valueLen;


                        // Get or create queue
                        BlockingQueue<CompletableFuture<ByteBuffer>> queue =
                                queueMap.computeIfAbsent(key,
                                        k -> new LinkedBlockingQueue<>(maxQueueCapacity));

                        // Put future in queue
                        queue.put(valueFuture);

                        // Start consumer thread if not exists
                        consumerThreads.computeIfAbsent(key, k -> {
                            Thread t = new Thread(() -> consumeQueue(k, queue, protoType));
                            t.setName("consumer-" + key);
                            t.start();
                            return t;
                        });
                    } catch (IOException | InterruptedException e) {
                        break;
                    }
                }
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }

        }

        // signal all queues to stop
        queueMap.values().forEach(q -> {
            try {
                q.put(POISON_PILL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // wait all consumers to finish
        consumerThreads.values().forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private ByteBuffer copyToHeap(ByteBuffer directBuffer) {
        ByteBuffer duplicate = directBuffer.duplicate();
        ByteBuffer heapBuffer = ByteBuffer.allocate(duplicate.remaining());
        heapBuffer.put(duplicate);
        heapBuffer.flip();
        return heapBuffer;
    }
    private static String readString(ByteBuffer buffer, int len) {
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes);
    }

    private void consumeQueue(int key, BlockingQueue<CompletableFuture<ByteBuffer>> queue, ProtoType protoType) {
        try {
            while (true) {
                CompletableFuture<ByteBuffer> value = queue.take();
                if(value == POISON_PILL)
                {
                    break;
                }
                ByteBuffer valueBuffer = value.get();
                metricsFacade.recordDebeziumEvent();
                switch (protoType) {
                    case ROW -> handleRowChangeSourceRecord(key, valueBuffer);
                    case TRANS -> handleTransactionSourceRecord(valueBuffer);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error("Error in async processing", e);
        }
    }
    static SchemaTableName transactionSchemaTableName = new SchemaTableName("freak", "transaction");

    private void handleRowChangeSourceRecord(int key, ByteBuffer dataBuffer)
    {
        tablePipelineManager.routeRecord(key, dataBuffer);
    }

    private void processTransactionSourceRecord() {
        while (true) {
            ByteBuffer data = null;
            try
            {
                data = rawTransactionQueue.take();
            } catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            try {
                SinkProto.TransactionMetadata tx = SinkProto.TransactionMetadata.parseFrom(data);
                metricsFacade.recordSerdTxChange();
                if (tx != null) {
                    transactionEventProvider.getEventQueue().put(tx);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public boolean isRunning()
    {
        return false;
    }

    @Override
    public void stopProcessor()
    {
        transAdapterThread.interrupt();
        transactionProcessor.stopProcessor();
    }
}
