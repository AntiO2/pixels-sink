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
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.ProtoType;
import io.pixelsdb.pixels.sink.event.TableEnginePipelineManager;
import io.pixelsdb.pixels.sink.event.TransactionEventEngineProvider;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.util.EtcdFileRegistry;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @package: io.pixelsdb.pixels.sink.processor
 * @className: SinkStorageProcessor
 * @author: AntiO2
 * @date: 2025/10/5 11:43
 */
public class SinkStorageProcessor implements MainProcessor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkStorageProcessor.class);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final String topic;
    private final String baseDir;
    private final EtcdFileRegistry etcdFileRegistry;
    private final List<String> files;

    private final TransactionEventEngineProvider transactionEventProvider = TransactionEventEngineProvider.INSTANCE;
    private final TableEnginePipelineManager tableEnginePipelineManager = new TableEnginePipelineManager();
    private final TransactionProcessor transactionProcessor = new TransactionProcessor(transactionEventProvider);
    private final Thread transactionProcessorThread;
    private final Thread transAdapterThread;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();

    private final BlockingQueue<ByteBuffer> rawTransactionQueue = new LinkedBlockingQueue<>(10000);
    private final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    public SinkStorageProcessor()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.topic = pixelsSinkConfig.getSinkProtoData();
        this.baseDir = pixelsSinkConfig.getSinkProtoDir();
        this.etcdFileRegistry = new EtcdFileRegistry(topic, baseDir);
        this.files = this.etcdFileRegistry.listAllFiles();

        this.transactionProcessorThread = new Thread(transactionProcessor, "debezium-processor");
        this.transAdapterThread = new Thread(this::processTransactionSourceRecord, "transaction-adapter");

    }

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
                while(true)
                {
                    try {
                        int keyLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                        int valueLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                        ByteBuffer keyBuffer = copyToHeap(reader.readFully(keyLen)).order(ByteOrder.BIG_ENDIAN);
                        ByteBuffer valueBuffer = copyToHeap(reader.readFully(valueLen)).order(ByteOrder.BIG_ENDIAN);
                        ProtoType protoType = ProtoType.fromInt(keyBuffer.getInt());
                        try {
                            switch (protoType)
                            {
                                case ROW -> {
                                    rowExecutor.submit(() -> {
                                        metricsFacade.recordRowEvent();
                                        handleRowChangeSourceRecord(keyBuffer, valueBuffer);
                                    });
                                }
                                case TRANS -> {
                                    transExecutor.submit(() -> {
                                        metricsFacade.recordTransaction();
                                        try
                                        {
                                            handleTransactionSourceRecord(valueBuffer);
                                        } catch (InterruptedException e)
                                        {
                                            throw new RuntimeException(e);
                                        }
                                    });
                                }
                            }
                        } catch (Exception e) {
                            LOGGER.error("Error processing record", e);
                        }
                    } catch (IOException e) {
                        break;
                    }
                }
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }

        }
    }

    private void handleTransactionSourceRecord(ByteBuffer sourceRecord) throws InterruptedException
    {
        rawTransactionQueue.put(sourceRecord);
    }


    public static ByteBuffer copyToHeap(ByteBuffer directBuffer) {
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

    private void handleRowChangeSourceRecord(ByteBuffer keyBuffer, ByteBuffer dataBuffer)
    {
        SchemaTableName schemaTableName = null;
        {
            // CODE BLOCK VERSION 1
            int schemaLen = keyBuffer.getInt();
            int tableLen = keyBuffer.getInt();
            String schemaName = readString(keyBuffer, schemaLen);
            String tableName = readString(keyBuffer, tableLen);
            schemaTableName = new SchemaTableName(schemaName, tableName);
        }
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

        tableEnginePipelineManager.routeRecord(schemaTableName, dataBuffer);
    }

    private void processTransactionSourceRecord() {
        while (true) {
            try {
                ByteBuffer data = rawTransactionQueue.take();
                SinkProto.TransactionMetadata tx = SinkProto.TransactionMetadata.parseFrom(data);
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
