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

import com.google.common.util.concurrent.RateLimiter;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GuavaFlushRateLimiter implements FlushRateLimiter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(GuavaFlushRateLimiter.class);
    private final RateLimiter rateLimiter;

    public GuavaFlushRateLimiter(PixelsSinkConfig config)
    {
        this.rateLimiter = RateLimiter.create(config.getSourceRateLimit());
        LOGGER.info("GuavaRateLimiter initialized. Rate: {}/s", config.getSourceRateLimit());
    }

    @Override
    public void acquire(int num)
    {
        rateLimiter.acquire(num);
    }

    @Override
    public void shutdown()
    {
    }
}