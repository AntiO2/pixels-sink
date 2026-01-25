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

package io.pixelsdb.pixels.sink.event;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RowChangeEventTest
{
    private static Logger LOGGER = LoggerFactory.getLogger(RowChangeEventTest.class);

    @BeforeAll
    public static void init() throws IOException
    {
        PixelsSinkConfigFactory.initialize("/home/ubuntu/pixels-sink/conf/pixels-sink.aws.properties");
    }


    @Test
    public void testSameHash()
    {
        for (int i = 0; i < 10; ++i)
        {
            ByteString indexKey = getIndexKey(0);
            int bucket = RowChangeEvent.getBucketIdFromByteBuffer(indexKey);
            LOGGER.info("Bucket: {}", bucket);
        }
    }

    private ByteString getIndexKey(int key)
    {
        int keySize = Integer.BYTES;
        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);
        byteBuffer.putInt(key);
        return ByteString.copyFrom(byteBuffer.rewind());
    }
}
