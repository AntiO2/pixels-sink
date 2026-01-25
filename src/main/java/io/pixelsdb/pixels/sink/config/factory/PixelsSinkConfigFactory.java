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

package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;

import java.io.IOException;

public class PixelsSinkConfigFactory
{
    private static volatile PixelsSinkConfig instance;
    private static String configFilePath;
    private static ConfigFactory config;

    private PixelsSinkConfigFactory()
    {
    }


    public static synchronized void initialize(String configFilePath) throws IOException
    {
        if (instance != null)
        {
            throw new IllegalStateException("PixelsSinkConfig is already initialized!");
        }
        instance = new PixelsSinkConfig(configFilePath);
        PixelsSinkConfigFactory.configFilePath = configFilePath;
    }

    public static synchronized void initialize(ConfigFactory config)
    {
        PixelsSinkConfigFactory.config = config;
        instance = new PixelsSinkConfig(config);
    }

    public static PixelsSinkConfig getInstance()
    {
        if (instance == null)
        {
            throw new IllegalStateException("PixelsSinkConfig is not initialized! Call initialize() first.");
        }
        return instance;
    }

    public static synchronized void reset()
    {
        instance = null;
        configFilePath = null;
    }
}
