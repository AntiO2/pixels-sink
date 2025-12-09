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
 
package io.pixelsdb.pixels.sink.freshness;

import java.util.ArrayDeque;
import java.util.Deque;

public class OneSecondAverage
{
    /**
     * Time window in milliseconds
     */
    private final int windowMillis;

    /**
     * Sliding window storing timestamped values
     */
    private final Deque<TimedValue> window = new ArrayDeque<>();

    /**
     * Constructor with configurable window size (milliseconds)
     */
    public OneSecondAverage(int windowMillis)
    {
        this.windowMillis = windowMillis;
    }

    /**
     * Record a new data point
     */
    public synchronized void record(double v)
    {
        long now = System.currentTimeMillis();
        window.addLast(new TimedValue(now, v));
        evictOld(now);
    }

    /**
     * Remove all values older than windowMillis
     */
    private void evictOld(long now)
    {
        while (!window.isEmpty() && now - window.peekFirst().timestamp > windowMillis)
        {
            window.removeFirst();
        }
    }

    /**
     * Compute average of values in the time window
     */
    public synchronized double getWindowAverage()
    {
        long now = System.currentTimeMillis();
        evictOld(now);

        if (window.isEmpty())
        {
            return Double.NaN;
        }

        double sum = 0;
        for (TimedValue tv : window)
        {
            sum += tv.value;
        }
        return sum / window.size();
    }

    /**
     * Timestamped data point
     */
    private record TimedValue(long timestamp, double value)
    {
    }
}
