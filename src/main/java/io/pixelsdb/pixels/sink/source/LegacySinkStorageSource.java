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


package io.pixelsdb.pixels.sink.source;


import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.provider.TableProviderAndProcessorPipelineManager;
import io.pixelsdb.pixels.sink.provider.TransactionEventEngineProvider;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @package: io.pixelsdb.pixels.sink.processor
 * @className: LegacySinkStorageSource
 * @author: AntiO2
 * @date: 2025/10/5 11:43
 */
@Deprecated
public class LegacySinkStorageSource extends AbstractSinkStorageSource implements SinkSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LegacySinkStorageSource.class);
    static SchemaTableName transactionSchemaTableName = new SchemaTableName("freak", "transaction");
    private final TransactionEventEngineProvider transactionEventProvider = TransactionEventEngineProvider.INSTANCE;

    private final TableProviderAndProcessorPipelineManager<ByteBuffer> tableProvidersManagerImpl = new TableProviderAndProcessorPipelineManager<>();
    private final TransactionProcessor transactionProcessor = new TransactionProcessor(transactionEventProvider);
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final Map<SchemaTableName, BlockingQueue<CompletableFuture<ByteBuffer>>> queueMap = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, Thread> consumerThreads = new ConcurrentHashMap<>();
    private final int maxQueueCapacity = 10000;
    private final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    private final CompletableFuture<ByteBuffer> POISON_PILL = new CompletableFuture<>();


    private static String readString(ByteBuffer buffer, int len)
    {
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes);
    }

    @Override
    ProtoType getProtoType(int i)
    {
        return ProtoType.fromInt(i);
    }

    @Override
    public void start()
    {

        for (String file : files)
        {
            Storage.Scheme scheme = Storage.Scheme.fromPath(file);
            LOGGER.info("Start read from file {}", file);
            try (PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(scheme, file))
            {
                long offset = 0;
                BlockingQueue<Pair<ByteBuffer, CompletableFuture<ByteBuffer>>> rowQueue = new LinkedBlockingQueue<>();
                BlockingQueue<CompletableFuture<ByteBuffer>> transQueue = new LinkedBlockingQueue<>();
                while (true)
                {
                    try
                    {
                        int keyLen, valueLen;
                        reader.seek(offset);
                        try
                        {
                            keyLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                            valueLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                        } catch (IOException e)
                        {
                            // EOF
                            break;
                        }

                        ByteBuffer keyBuffer = copyToHeap(reader.readFully(keyLen)).order(ByteOrder.BIG_ENDIAN);
                        ProtoType protoType = getProtoType(keyBuffer.getInt());
                        offset += Integer.BYTES * 2 + keyLen;
                        CompletableFuture<ByteBuffer> valueFuture = reader.readAsync(offset, valueLen)
                                .thenApply(this::copyToHeap)
                                .thenApply(buf -> buf.order(ByteOrder.BIG_ENDIAN));
                        // move offset for next record
                        offset += valueLen;

                        // Compute queue key (for example: schemaName + tableName or protoType)
                        SchemaTableName queueKey = computeQueueKey(keyBuffer, protoType);

                        // Get or create queue
                        BlockingQueue<CompletableFuture<ByteBuffer>> queue =
                                queueMap.computeIfAbsent(queueKey,
                                        k -> new LinkedBlockingQueue<>(maxQueueCapacity));

                        // Put future in queue
                        queue.put(valueFuture);

                        // Start consumer thread if not exists
                        consumerThreads.computeIfAbsent(queueKey, k ->
                        {
                            Thread t = new Thread(() -> consumeQueue(k, queue, protoType));
                            t.setName("consumer-" + queueKey);
                            t.start();
                            return t;
                        });
                    } catch (IOException | InterruptedException e)
                    {
                        break;
                    }
                }
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }

        }

        // signal all queues to stop
        queueMap.values().forEach(q ->
        {
            try
            {
                q.put(POISON_PILL);
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });

        // wait all consumers to finish
        consumerThreads.values().forEach(t ->
        {
            try
            {
                t.join();
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });
    }

    private ByteBuffer copyToHeap(ByteBuffer directBuffer)
    {
        ByteBuffer duplicate = directBuffer.duplicate();
        ByteBuffer heapBuffer = ByteBuffer.allocate(duplicate.remaining());
        heapBuffer.put(duplicate);
        heapBuffer.flip();
        return heapBuffer;
    }

    private void consumeQueue(SchemaTableName key, BlockingQueue<CompletableFuture<ByteBuffer>> queue, ProtoType protoType)
    {
        try
        {
            while (true)
            {
                CompletableFuture<ByteBuffer> value = queue.take();
                if (value == POISON_PILL)
                {
                    break;
                }
                ByteBuffer valueBuffer = value.get();
                metricsFacade.recordDebeziumEvent();
                switch (protoType)
                {
                    case ROW -> handleRowChangeSourceRecord(key, valueBuffer);
                    case TRANS -> handleTransactionSourceRecord(valueBuffer);
                }
            }
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e)
        {
            LOGGER.error("Error in async processing", e);
        }
    }

    private SchemaTableName computeQueueKey(ByteBuffer keyBuffer, ProtoType protoType)
    {
        switch (protoType)
        {
            case ROW ->
            {
                int schemaLen = keyBuffer.getInt();
                int tableLen = keyBuffer.getInt();
                String schemaName = readString(keyBuffer, schemaLen);
                String tableName = readString(keyBuffer, tableLen);
                return new SchemaTableName(schemaName, tableName);
            }
            case TRANS ->
            {
                return transactionSchemaTableName;
            }
            default ->
            {
                throw new IllegalArgumentException("Proto type " + protoType.toString());
            }
        }
    }

    private void handleRowChangeSourceRecord(SchemaTableName schemaTableName, ByteBuffer dataBuffer)
    {
        tableProvidersManagerImpl.routeRecord(schemaTableName, dataBuffer);
    }

    private void handleRowChangeSourceRecord(ByteBuffer keyBuffer, ByteBuffer dataBuffer)
    {
        {
            // CODE BLOCK VERSION 2
//            long tableId = keyBuffer.getLong();
//            try
//            {
//                schemaTableName = tableMetadataRegistry.getSchemaTableName(tableId);
//            } catch (SinkException e)
//            {
//                throw new RuntimeException(e);
//            }
        }

//        tableProvidersManagerImpl.routeRecord(schemaTableName, dataBuffer);
    }
}
