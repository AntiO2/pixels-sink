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
 
package io.pixelsdb.pixels.sink.provider;


import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
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
            newPipeline.run();
            return newPipeline;
        });
        pipeline.putRawEvent(record);
    }

    private TableEventProvider<SOURCE_RECORD_T> createProvider(SOURCE_RECORD_T record)
    {
        Class<?> recordType = record.getClass();
        if (recordType == Pair.class)
        {
            return new TableEventStorageLoopProvider<>();
        }
        if (recordType == SourceRecord.class)
        {
            return new TableEventEngineProvider<>();
        } else if (ByteBuffer.class.isAssignableFrom(recordType))
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
