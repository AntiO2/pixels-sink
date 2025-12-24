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

package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEventTest;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TableWriterProxyTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableWriterProxyTest.class);


    String tableName = "test";

    @BeforeAll
    public static void init() throws IOException
    {
        PixelsSinkConfigFactory.initialize("/home/ubuntu/pixels-sink/conf/pixels-sink.aws.properties");
    }

    @Test
    public void testGetSameTableWriter() throws IOException
    {
        TableWriterProxy tableWriterProxy = TableWriterProxy.getInstance();

        for(int i = 0; i < 10 ; i++)
        {
            TableWriter tableWriter = tableWriterProxy.getTableWriter(tableName, 0, 0);
        }
    }
}
