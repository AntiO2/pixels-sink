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

import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class LatencySimulator {
    private static final Random RANDOM = ThreadLocalRandom.current();
    private static final double longTailProb = 0.05;
    private static final double longTailScale = 30;
    private static final double tailVariance = 0.1;
    private static final double normalVariance = 0.4;

    private static long generateLongTailDelay(long baseDelayMs) {
        if (RANDOM.nextDouble() < longTailProb) {
            double variance = 1 + (RANDOM.nextDouble() * 2 - 1) * tailVariance;
            return (long) (baseDelayMs * longTailScale * variance);
        }

        return (long) (baseDelayMs * (1 + (RANDOM.nextDouble() - 0.5) * normalVariance));
    }

    public static void smartDelay() {
        try {
            TimeUnit.MILLISECONDS.sleep(generateLongTailDelay(PixelsSinkConfigFactory.getInstance().getMockRpcDelay()));
        } catch (InterruptedException ignored) {

        }
    }
}