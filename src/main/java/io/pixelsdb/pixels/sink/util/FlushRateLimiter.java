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

package io.pixelsdb.pixels.sink.util;

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
    // Configuration derived parameters
    private static final long REFRESH_PERIOD_MS = 10;
    private static volatile FlushRateLimiter instance;
    private final Semaphore semaphore;
    private final boolean enableRateLimiter;
    private final ScheduledExecutorService scheduler;
    private final int replenishmentAmount;

    private FlushRateLimiter() {
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

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r ->
        {
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

    public static FlushRateLimiter getInstance() {
        if (instance == null) {
            synchronized (FlushRateLimiter.class) {
                if (instance == null) {
                    instance = new FlushRateLimiter();
                }
            }
        }
        return instance;
    }

    public static FlushRateLimiter getNewInstance() {
        return new FlushRateLimiter();
    }

    private void replenishTokens() {
        if (semaphore != null) {
            semaphore.release(replenishmentAmount);
        }
    }

    public void acquire(int num) {
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