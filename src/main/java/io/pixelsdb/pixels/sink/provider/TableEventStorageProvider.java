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


import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.deserializer.RowChangeEventStructDeserializer;
import io.pixelsdb.pixels.sink.exception.SinkException;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

/**
 * @package: io.pixelsdb.pixels.sink.event
 * @className: TableEventStorageProvider
 * @author: AntiO2
 * @date: 2025/9/26 10:45
 */
public class TableEventStorageProvider<T> extends TableEventProvider<T>
{
    private final Logger LOGGER = Logger.getLogger(TableEventStorageProvider.class.getName());

    protected TableEventStorageProvider()
    {
        super();
    }

    @Override
    RowChangeEvent convertToTargetRecord(T record)
    {
        ByteBuffer sourceRecord = (ByteBuffer) record;
        try
        {
            SinkProto.RowRecord rowRecord = SinkProto.RowRecord.parseFrom(sourceRecord);
            return RowChangeEventStructDeserializer.convertToRowChangeEvent(rowRecord);
        } catch (InvalidProtocolBufferException | SinkException e)
        {
            LOGGER.warning(e.getMessage());
            return null;
        }
    }
}
