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


import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.apache.kafka.connect.source.SourceRecord;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * @package: io.pixelsdb.pixels.sink.event
 * @className: TableEventStorageProvider
 * @author: AntiO2
 * @date: 2025/9/26 10:45
 */
public class TableEventStorageProvider implements TableEventProvider {
    private final Logger LOGGER = Logger.getLogger(TableEventStorageProvider.class.getName());
    private final SchemaTableName schemaTableName;

    private final BlockingQueue<ByteBuffer> rawEventQueue = new LinkedBlockingQueue<>(10000);
    private final BlockingQueue<RowChangeEvent> eventQueue = new LinkedBlockingQueue<>(10000);
    private final Thread processorThread;

    public TableEventStorageProvider(SchemaTableName schemaTableName)
    {
        this.schemaTableName = schemaTableName;
        this.processorThread = new Thread(this::processLoop, "TableEventStorageProvider-" + schemaTableName.getTableName());
    }

    @Override
    public BlockingQueue<RowChangeEvent> getSourceEventQueue()
    {
        return eventQueue;
    }

    public void start() {
        processorThread.start();
    }

    public void put(ByteBuffer record) {
        try {
            rawEventQueue.put(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processLoop() {
        while (true) {
            try {
                ByteBuffer data = rawEventQueue.take();
                SinkProto.RowRecord rowRecord = null;
                try
                {
                    rowRecord = SinkProto.RowRecord.parseFrom(data);
                } catch (InvalidProtocolBufferException e)
                {
                    throw new RuntimeException(e);
                }
                RowChangeEvent rowChangeEvent = null;
                try
                {
                    rowChangeEvent = RowChangeEventStructDeserializer.convertToRowChangeEvent(rowRecord);
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
