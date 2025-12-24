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
 

package io.pixelsdb.pixels.sink.source;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.engine.SinkEngineSource;
import io.pixelsdb.pixels.sink.source.kafka.SinkKafkaSource;
import io.pixelsdb.pixels.sink.source.storage.FasterSinkStorageSource;

public class SinkSourceFactory
{
    public static SinkSource createSinkSource()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        return switch (config.getDataSource())
        {
            case "kafka" -> new SinkKafkaSource();
            case "engine" -> new SinkEngineSource();
            case "storage" -> new FasterSinkStorageSource();
            default -> throw new IllegalStateException("Unsupported data source type: " + config.getDataSource());
        };
    }
}
