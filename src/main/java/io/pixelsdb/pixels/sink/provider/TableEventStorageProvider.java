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
