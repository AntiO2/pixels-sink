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
import io.pixelsdb.pixels.sink.processor.TableMonitor;
import io.pixelsdb.pixels.sink.processor.TableProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @package: io.pixelsdb.pixels.sink.event
 * @className: TablePipelineManager
 * @author: AntiO2
 * @date: 2025/9/26 10:44
 */
abstract public class TablePipelineManager
{
    protected final ExecutorService executorService = Executors.newCachedThreadPool();
    protected final Map<SchemaTableName, TableProcessor> activeTableProcessors = new ConcurrentHashMap<>();
    protected final Map<Integer, TableProcessor> id2activeTableProcessors = new ConcurrentHashMap<>();
}
