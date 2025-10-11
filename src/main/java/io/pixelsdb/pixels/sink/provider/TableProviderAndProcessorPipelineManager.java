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


package io.pixelsdb.pixels.sink.provider;


import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.processor.TableProcessor;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @package: io.pixelsdb.pixels.sink.provider
 * @className: TableProviderAndProcessorPipelineManager
 * @author: AntiO2
 * @date: 2025/9/26 10:44
 */
public class TableProviderAndProcessorPipelineManager<SOURCE_RECORD_T>
{
    protected final Map<Integer, TableProcessor> activeTableProcessors = new ConcurrentHashMap<>();
    protected final Map<SchemaTableName, Integer> tableIds = new ConcurrentHashMap<>();
    private final Map<Integer, TableEventProvider<SOURCE_RECORD_T>> tableProviders = new ConcurrentHashMap<>();
    private final AtomicInteger nextTableId = new AtomicInteger();


    public void routeRecord(SchemaTableName schemaTableName, SOURCE_RECORD_T record)
    {
        routeRecord(getTableId(schemaTableName), record);
    }

    public void routeRecord(Integer tableId, SOURCE_RECORD_T record)
    {
        TableEventProvider<SOURCE_RECORD_T> pipeline = tableProviders.computeIfAbsent(tableId, k ->
        {
            TableEventProvider<SOURCE_RECORD_T> newPipeline = createProvider(record);
            TableProcessor tableProcessor = activeTableProcessors.computeIfAbsent(tableId, k2 ->
                    new TableProcessor(newPipeline)
            );
            tableProcessor.run();
            return newPipeline;
        });
        pipeline.putRawEvent(record);
    }

    private TableEventProvider<SOURCE_RECORD_T> createProvider(SOURCE_RECORD_T record)
    {
        Class<?> recordType = record.getClass();
        if (recordType == SourceRecord.class)
        {
            return new TableEventEngineProvider<>();
        } else if (recordType == ByteBuffer.class)
        {
            return new TableEventStorageProvider<>();
        } else
        {
            throw new IllegalArgumentException("Unsupported record type: " + recordType.getName());
        }
    }

    private Integer getTableId(SchemaTableName schemaTableName)
    {
        return tableIds.computeIfAbsent(schemaTableName, k -> allocateTableId());
    }

    private Integer allocateTableId()
    {
        return nextTableId.getAndIncrement();
    }
}
