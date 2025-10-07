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


package io.pixelsdb.pixels.sink;


import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.deserializer.TransactionStructMessageDeserializer;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.TableEnginePipelineManager;
import io.pixelsdb.pixels.sink.event.TablePipelineManager;
import io.pixelsdb.pixels.sink.event.TransactionEventEngineProvider;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.processor.MetricsFacade;
import io.pixelsdb.pixels.sink.processor.StoppableProcessor;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.sink.PixelsSinkMode;
import io.pixelsdb.pixels.sink.sink.ProtoWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @package: io.pixelsdb.pixels.sink
 * @className: PixelsDebeziumConsumer
 * @author: AntiO2
 * @date: 2025/9/25 12:51
 */
public class PixelsDebeziumConsumer implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>, StoppableProcessor
{
    PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
    private final BlockingQueue<SourceRecord> rawTransactionQueue = new LinkedBlockingQueue<>(10000);
    private final String checkTransactionTopic;
    private final TransactionEventEngineProvider transactionEventProvider = TransactionEventEngineProvider.INSTANCE;
    private final TableEnginePipelineManager tableEnginePipelineManager = new TableEnginePipelineManager();
    private final TransactionProcessor processor = new TransactionProcessor(transactionEventProvider);
    private final Thread processorThread;
    private final Thread adapterThread;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final PixelsSinkMode pixelsSinkMode;
    private final ProtoWriter protoWriter;
    public PixelsDebeziumConsumer()
    {
        this.checkTransactionTopic = pixelsSinkConfig.getDebeziumTopicPrefix() + ".transaction";
        adapterThread = new Thread(this::processTransactionSourceRecord, "transaction-adapter");
        adapterThread.start();
        processorThread = new Thread(processor, "debezium-processor");
        processorThread.start();
        pixelsSinkMode = pixelsSinkConfig.getPixelsSinkMode();

        if(pixelsSinkMode == PixelsSinkMode.PROTO)
        {
            try
            {
                this.protoWriter = new ProtoWriter();
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        } else
        {
            this.protoWriter = null;
        }
    }


    public void handleBatch(List<RecordChangeEvent<SourceRecord>> event,
                            DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException {
        for(RecordChangeEvent<SourceRecord> record:event)
        {
            try
            {
                SourceRecord sourceRecord = record.record();
                if(sourceRecord == null)
                {
                    continue;
                }

                metricsFacade.recordDebeziumEvent();
                if(isTransactionEvent(sourceRecord))
                {
                    switch (pixelsSinkMode)
                    {
                        case RETINA ->
                        {
                            handleTransactionSourceRecord(sourceRecord);
                        }
                        case PROTO ->
                        {
                            metricsFacade.recordTransaction();
                            SinkProto.TransactionMetadata transactionMetadata = TransactionStructMessageDeserializer.convertToTransactionMetadata(sourceRecord);
                            protoWriter.writeTrans(transactionMetadata);
                        }
                        default ->
                        {
                            throw new RuntimeException("Sink Mode " + pixelsSinkMode.toString() + "is not supported");
                        }
                    }
                } else {
                    switch (pixelsSinkMode)
                    {
                        case RETINA ->
                        {
                            handleRowChangeSourceRecord(sourceRecord);
                        }
                        case PROTO ->
                        {
                            try
                            {
                                RowChangeEvent rowChangeEvent = RowChangeEventStructDeserializer.convertToRowChangeEvent(sourceRecord);
                                protoWriter.write(rowChangeEvent);
                                metricsFacade.recordRowEvent();
                            } catch (SinkException e)
                            {
                                throw new RuntimeException(e);
                            }

                        }
                        default ->
                        {
                            throw new RuntimeException("Sink Mode " + pixelsSinkMode.toString() + "is not supported");
                        }
                    }
                }
            }
            finally
            {
                committer.markProcessed(record);
            }

        }
        committer.markBatchFinished();
    }

    private void handleTransactionSourceRecord(SourceRecord sourceRecord) throws InterruptedException
    {
        rawTransactionQueue.put(sourceRecord);
    }

    private void handleRowChangeSourceRecord(SourceRecord sourceRecord)
    {
        Struct value = (Struct) sourceRecord.value();
        Struct source = (Struct) value.get("source");
        String schemaName = source.get("db").toString();
        String tableName = source.get("table").toString();
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        tableEnginePipelineManager.routeRecord(schemaTableName, sourceRecord);
    }

    private void processTransactionSourceRecord() {
        while (true) {
            try {
                SourceRecord sourceRecord = rawTransactionQueue.take();
                SinkProto.TransactionMetadata tx = transactionEventProvider.convert(sourceRecord);
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

    private boolean isTransactionEvent(SourceRecord sourceRecord)
    {
        return checkTransactionTopic.equals(sourceRecord.topic());
    }

    @Override
    public void stopProcessor()
    {
        adapterThread.interrupt();
        processor.stopProcessor();
        if(protoWriter != null)
        {
            try
            {
                protoWriter.close();
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
