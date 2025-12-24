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


import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @package: io.pixelsdb.pixels.sink.provider
 * @className: TableEventEngineProvider
 * @author: AntiO2
 * @date: 2025/9/26 10:45
 */
public class TableEventEngineProvider<T> extends TableEventProvider<T> {
    private final Logger LOGGER = LoggerFactory.getLogger(TableEventEngineProvider.class.getName());

    @Override
    RowChangeEvent convertToTargetRecord(T record) {
        SourceRecord sourceRecord = (SourceRecord) record;
        try {
            return RowChangeEventStructDeserializer.convertToRowChangeEvent(sourceRecord);
        } catch (SinkException e) {
            LOGGER.warn("Failed to convert RowChangeEvent to RowChangeEventStruct {}", e.getMessage());
            return null;
        }
    }
}
