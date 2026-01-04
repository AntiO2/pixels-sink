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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SemaphoreFlushRateLimiter implements FlushRateLimiter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SemaphoreFlushRateLimiter.class);
    private static final long REFRESH_PERIOD_MS = 10;

    private final Semaphore semaphore;
    private final ScheduledExecutorService scheduler;
    private final int replenishmentAmount;
    private final int rateLimit;

    public SemaphoreFlushRateLimiter(PixelsSinkConfig config)
    {
        int sourceRateLimit = config.getSourceRateLimit();
        this.rateLimit = sourceRateLimit;

        double replenishmentPerMillisecond = (double) sourceRateLimit / 1000.0;
        this.replenishmentAmount = (int) Math.max(1, Math.round(replenishmentPerMillisecond * REFRESH_PERIOD_MS));
        this.semaphore = new Semaphore(this.replenishmentAmount);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r ->
        {
            Thread t = new Thread(r, "Rate-Limiter-Replenish-Semaphore");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleAtFixedRate(this::replenishTokens, REFRESH_PERIOD_MS, REFRESH_PERIOD_MS, TimeUnit.MILLISECONDS);
        LOGGER.info("SemaphoreRateLimiter initialized. Rate: {}/s", sourceRateLimit);
    }

    private void replenishTokens()
    {
        if (semaphore.availablePermits() < rateLimit)
        {
            semaphore.release(replenishmentAmount);
        }
    }

    @Override
    public void acquire(int num)
    {
        try
        {
            semaphore.acquire(num);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void shutdown()
    {
        if (scheduler != null) scheduler.shutdownNow();
    }
}