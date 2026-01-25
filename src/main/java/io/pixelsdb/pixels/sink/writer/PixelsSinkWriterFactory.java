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

package io.pixelsdb.pixels.sink.writer;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.writer.csv.CsvWriter;
import io.pixelsdb.pixels.sink.writer.flink.FlinkPollingWriter;
import io.pixelsdb.pixels.sink.writer.proto.ProtoWriter;
import io.pixelsdb.pixels.sink.writer.retina.RetinaWriter;

import java.io.IOException;

public class PixelsSinkWriterFactory
{
    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

    private static volatile PixelsSinkWriter writer = null;


    static public PixelsSinkWriter getWriter()
    {
        if (writer == null)
        {
            synchronized (PixelsSinkWriterFactory.class)
            {
                if (writer == null)
                {
                    try
                    {
                        switch (config.getPixelsSinkMode())
                        {
                            case CSV:
                                writer = new CsvWriter();
                                break;
                            case RETINA:
                                writer = new RetinaWriter();
                                break;
                            case PROTO:
                                writer = new ProtoWriter();
                                break;
                            case FLINK:
                                writer = new FlinkPollingWriter();
                                break;
                            case NONE:
                                writer = new NoneWriter();
                                break;
                        }
                    } catch (IOException e)
                    {
                        throw new RuntimeException("Can't create writer", e);
                    }
                }
            }
        }
        return writer;
    }
}
