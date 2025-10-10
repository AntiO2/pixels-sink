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


import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @package: io.pixelsdb.pixels.sink.event
 * @className: TableEventEngineProvider
 * @author: AntiO2
 * @date: 2025/9/26 10:45
 */
public class TableEventEngineProvider<T> extends TableEventProvider<T>
{
    private final Logger LOGGER = LoggerFactory.getLogger(TableEventEngineProvider.class.getName());

    @Override
    RowChangeEvent convertToTargetRecord(T record)
    {
        SourceRecord sourceRecord = (SourceRecord) record;
        try
        {
            return RowChangeEventStructDeserializer.convertToRowChangeEvent(sourceRecord);
        } catch (SinkException e)
        {
            LOGGER.warn("Failed to convert RowChangeEvent to RowChangeEventStruct {}", e.getMessage());
            return null;
        }
    }
}
