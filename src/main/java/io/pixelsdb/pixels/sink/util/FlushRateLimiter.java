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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class FlushRateLimiter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlushRateLimiter.class);

    private final Semaphore semaphore;
    private final boolean enableRateLimiter;
    private final ScheduledExecutorService scheduler;

    // Configuration derived parameters
    private static final long REFRESH_PERIOD_MS = 10;
    private final int replenishmentAmount;

    private static volatile FlushRateLimiter instance;

    private FlushRateLimiter()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        int sourceRateLimit = pixelsSinkConfig.getSourceRateLimit();
        this.enableRateLimiter = pixelsSinkConfig.isEnableSourceRateLimit();

        if (sourceRateLimit <= 0 || !enableRateLimiter) {
            this.semaphore = null;
            this.replenishmentAmount = 0;
            this.scheduler = null;
            return;
        }

        double replenishmentPerMillisecond = (double) sourceRateLimit / 1000.0;
        this.replenishmentAmount = (int) Math.max(1, Math.round(replenishmentPerMillisecond * REFRESH_PERIOD_MS));

        this.semaphore = new Semaphore(this.replenishmentAmount);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("Rate-Limiter-Replenish");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleAtFixedRate(
                this::replenishTokens,
                REFRESH_PERIOD_MS,
                REFRESH_PERIOD_MS,
                TimeUnit.MILLISECONDS
        );

        LOGGER.info("FlushRateLimiter initialized. Rate: {}/s, Replenishment: {} tokens every {}ms.",
                sourceRateLimit, this.replenishmentAmount, REFRESH_PERIOD_MS);
    }

    private void replenishTokens() {
        if (semaphore != null) {
            semaphore.release(replenishmentAmount);
        }
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
        if (enableRateLimiter && semaphore != null) {
            try {
                semaphore.acquire(num);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("FlushRateLimiter acquire interrupted.", e);
            }
        }
    }

    public void shutdown() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            LOGGER.info("FlushRateLimiter scheduler stopped.");
        }
    }
}