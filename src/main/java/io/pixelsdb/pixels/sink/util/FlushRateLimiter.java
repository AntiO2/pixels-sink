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

package io.pixelsdb.pixels.sink.util;

import com.google.common.util.concurrent.RateLimiter;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

public class FlushRateLimiter {
    private final RateLimiter rateLimiter;
    private final boolean enableRateLimiter;

    private static volatile FlushRateLimiter instance;

    private FlushRateLimiter()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        int sourceRateLimit = pixelsSinkConfig.getSourceRateLimit();
        this.rateLimiter = RateLimiter.create(sourceRateLimit);
        this.enableRateLimiter = pixelsSinkConfig.isEnableSourceRateLimit();
    }

    public static FlushRateLimiter getInstance()
    {
        if(instance == null)
        {
            synchronized (FlushRateLimiter.class)
            {
                if(instance == null)
                {
                    instance = new FlushRateLimiter();
                }
            }
        }
        return instance;
    }

    public void acquire(int num)
    {
        if (enableRateLimiter) {
            rateLimiter.acquire(num);
        }
    }

}
