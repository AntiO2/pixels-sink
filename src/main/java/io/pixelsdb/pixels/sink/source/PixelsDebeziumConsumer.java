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


import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.processor.StoppableProcessor;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.provider.TableProviderAndProcessorPipelineManager;
import io.pixelsdb.pixels.sink.provider.TransactionEventEngineProvider;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @package: io.pixelsdb.pixels.source
 * @className: PixelsDebeziumConsumer
 * @author: AntiO2
 * @date: 2025/9/25 12:51
 */
public class PixelsDebeziumConsumer implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>, StoppableProcessor
{
    private final String checkTransactionTopic;
    private final TransactionEventEngineProvider<SourceRecord> transactionEventProvider = TransactionEventEngineProvider.INSTANCE;
    private final TableProviderAndProcessorPipelineManager<SourceRecord> tableProvidersManagerImpl = new TableProviderAndProcessorPipelineManager<>();
    private final TransactionProcessor processor = new TransactionProcessor(transactionEventProvider);
    private final Thread transactionProviderThread;
    private final Thread transactionProcessorThread;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();

    public PixelsDebeziumConsumer()
    {
        this.checkTransactionTopic = pixelsSinkConfig.getDebeziumTopicPrefix() + ".transaction";
        this.transactionProviderThread = new Thread(this.transactionEventProvider, "transaction-adapter");
        this.transactionProcessorThread = new Thread(this.processor, "transaction-processor");

        this.transactionProcessorThread.start();
        this.transactionProviderThread.start();
    }


    public void handleBatch(List<RecordChangeEvent<SourceRecord>> event,
                            DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException
    {
        for (RecordChangeEvent<SourceRecord> record : event)
        {
            try
            {
                SourceRecord sourceRecord = record.record();
                if (sourceRecord == null)
                {
                    continue;
                }

                metricsFacade.recordDebeziumEvent();
                if (isTransactionEvent(sourceRecord))
                {
                    handleTransactionSourceRecord(sourceRecord);
                } else
                {
                    handleRowChangeSourceRecord(sourceRecord);
                }
            } finally
            {
                committer.markProcessed(record);
            }

        }
        committer.markBatchFinished();
    }

    private void handleTransactionSourceRecord(SourceRecord sourceRecord) throws InterruptedException
    {
        transactionEventProvider.putTransRawEvent(sourceRecord);
    }

    private void handleRowChangeSourceRecord(SourceRecord sourceRecord)
    {
        Struct value = (Struct) sourceRecord.value();
        Struct source = (Struct) value.get("source");
        String schemaName = source.get("db").toString();
        String tableName = source.get("table").toString();
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        tableProvidersManagerImpl.routeRecord(schemaTableName, sourceRecord);
    }

    private boolean isTransactionEvent(SourceRecord sourceRecord)
    {
        return checkTransactionTopic.equals(sourceRecord.topic());
    }

    @Override
    public void stopProcessor()
    {
        transactionProviderThread.interrupt();
        processor.stopProcessor();
        transactionProcessorThread.interrupt();
    }
}
