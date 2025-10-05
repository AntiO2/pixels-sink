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
import io.pixelsdb.pixels.sink.processor.TableProcessor;
import org.apache.kafka.connect.source.SourceRecord;

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
    private final Map<SchemaTableName, TableEventEngineProvider> pipelines = new ConcurrentHashMap<>();

    public void routeRecord(SchemaTableName schemaTableName, SourceRecord record) {
        TableEventEngineProvider tableEventEngineProvider = pipelines.computeIfAbsent(schemaTableName,
                k->
                {
                    TableEventEngineProvider newProvider = createPipeline(k);
                    TableProcessor tableProcessor = activeTableProcessors.computeIfAbsent(schemaTableName, k2 ->
                            new TableProcessor(newProvider, schemaTableName));
                    tableProcessor.run();
                    return newProvider;
                });
        tableEventEngineProvider.put(record);
    }

    private TableEventEngineProvider createPipeline(SchemaTableName schemaTableName) {
        TableEventEngineProvider pipeline = new TableEventEngineProvider(schemaTableName);
        pipeline.start();
        return pipeline;
    }
}
