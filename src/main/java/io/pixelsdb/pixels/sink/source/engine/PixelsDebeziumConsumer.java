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
 
package io.pixelsdb.pixels.sink.source.engine;


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
