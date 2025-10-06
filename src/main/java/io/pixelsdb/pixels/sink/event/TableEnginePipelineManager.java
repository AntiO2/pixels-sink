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


import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.processor.TableProcessor;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @package: io.pixelsdb.pixels.sink.event
 * @className: TableEnginePipelineManager
 * @author: AntiO2
 * @date: 2025/9/26 10:44
 */
public class TableEnginePipelineManager extends TablePipelineManager
{
    private final Map<SchemaTableName, TableEventEngineProvider> enginePipelines = new ConcurrentHashMap<>();
    private final Map<SchemaTableName, TableEventStorageProvider> storagePipelines = new ConcurrentHashMap<>();

    public void routeRecord(SchemaTableName schemaTableName, SourceRecord record) {
        TableEventEngineProvider tableEventEngineProvider = enginePipelines.computeIfAbsent(schemaTableName,
                k->
                {
                    TableEventEngineProvider newProvider = createEnginePipeline(k);
                    TableProcessor tableProcessor = activeTableProcessors.computeIfAbsent(schemaTableName, k2 ->
                            new TableProcessor(newProvider, schemaTableName));
                    tableProcessor.run();
                    return newProvider;
                });
        tableEventEngineProvider.put(record);
    }

    public void routeRecord(SchemaTableName schemaTableName, ByteBuffer record) {
        TableEventStorageProvider tableEventStorageProvider = storagePipelines.computeIfAbsent(schemaTableName,
                k->
                {
                    TableEventStorageProvider newProvider = createStoragePipeline(k);
                    TableProcessor tableProcessor = activeTableProcessors.computeIfAbsent(schemaTableName, k2 ->
                            new TableProcessor(newProvider, schemaTableName));
                    tableProcessor.run();
                    return newProvider;
                });
        tableEventStorageProvider.put(record);
    }

    private TableEventEngineProvider createEnginePipeline(SchemaTableName schemaTableName) {
        TableEventEngineProvider pipeline = new TableEventEngineProvider(schemaTableName);
        pipeline.start();
        return pipeline;
    }

    private TableEventStorageProvider createStoragePipeline(SchemaTableName schemaTableName) {
        TableEventStorageProvider pipeline = new TableEventStorageProvider(schemaTableName);
        pipeline.start();
        return pipeline;
    }
}
