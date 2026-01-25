/*
 * Copyright 2026 PixelsDB.
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

package io.pixelsdb.pixels.sink.util.rateLimiter;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

public class FlushRateLimiterFactory
{
    private static volatile FlushRateLimiter instance;

    public static FlushRateLimiter getInstance()
    {
        if (instance == null)
        {
            synchronized (FlushRateLimiterFactory.class)
            {
                if (instance == null)
                {
                    instance = createLimiter();
                }
            }
        }
        return instance;
    }

    public static FlushRateLimiter getNewInstance()
    {
        return createLimiter();
    }

    private static FlushRateLimiter createLimiter()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

        if (!config.isEnableSourceRateLimit() || config.getSourceRateLimit() <= 0)
        {
            return new NoOpFlushRateLimiter();
        }

        String type = config.getRateLimiterType().toLowerCase();

        switch (type)
        {
            case "guava":
                return new GuavaFlushRateLimiter(config);
            case "semaphore":
            default:
                return new SemaphoreFlushRateLimiter(config);
        }
    }
}