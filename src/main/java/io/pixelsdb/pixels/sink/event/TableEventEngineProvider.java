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
import io.pixelsdb.pixels.sink.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * @package: io.pixelsdb.pixels.sink.event
 * @className: TableEventEngineProvider
 * @author: AntiO2
 * @date: 2025/9/26 10:45
 */
public class TableEventEngineProvider implements TableEventProvider {
    private final Logger LOGGER = Logger.getLogger(TableEventEngineProvider.class.getName());
    private final SchemaTableName schemaTableName;

    private final BlockingQueue<SourceRecord> rawEventQueue = new LinkedBlockingQueue<>(10000);
    private final BlockingQueue<RowChangeEvent> eventQueue = new LinkedBlockingQueue<>(10000);
    private final Thread processorThread;

    public TableEventEngineProvider(SchemaTableName schemaTableName)
    {
        this.schemaTableName = schemaTableName;
        this.processorThread = new Thread(this::processLoop, "TableEventEngineProvider-" + schemaTableName.getTableName());
    }

    @Override
    public BlockingQueue<RowChangeEvent> getSourceEventQueue()
    {
        return eventQueue;
    }

    public void start() {
        processorThread.start();
    }

    public void put(SourceRecord record) {
        try {
            rawEventQueue.put(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processLoop() {
        while (true) {
            try {
                SourceRecord record = rawEventQueue.take();
                RowChangeEvent rowChangeEvent = null;
                try
                {
                    rowChangeEvent = RowChangeEventStructDeserializer.convertToRowChangeEvent(record);
                } catch (SinkException e)
                {
                    LOGGER.warning(e.getMessage());
                    continue;
                }
                eventQueue.put(rowChangeEvent);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

}
