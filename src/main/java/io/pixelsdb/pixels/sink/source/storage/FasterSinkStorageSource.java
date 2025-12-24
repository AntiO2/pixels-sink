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
 
package io.pixelsdb.pixels.sink.source.storage;


import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.source.SinkSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @package: io.pixelsdb.pixels.sink.source
 * @className: LegacySinkStorageSource
 * @author: AntiO2
 * @date: 2025/10/5 11:43
 */
public class FasterSinkStorageSource extends AbstractMemorySinkStorageSource implements SinkSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FasterSinkStorageSource.class);
    static SchemaTableName transactionSchemaTableName = new SchemaTableName("freak", "transaction");

    public FasterSinkStorageSource()
    {
        super();
    }

    private static String readString(ByteBuffer buffer, int len)
    {
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes);
    }

    @Override
    ProtoType getProtoType(int i)
    {
        if (i == -1)
        {
            return ProtoType.TRANS;
        }
        return ProtoType.ROW;
    }

}
