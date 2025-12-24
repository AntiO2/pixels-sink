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

/**
 * Inner class to hold and manage per-table transaction row counts.
 */
public class TableCounters {
    private final int totalCount;           // The expected total number of rows
    // currentCount is volatile for visibility across threads, as it's incremented during writeRow.
    private volatile int currentCount = 0;

    public TableCounters(int totalCount) {
        this.totalCount = totalCount;
    }

    public void increment() {
        currentCount++;
    }

    public boolean isComplete() {
        // Checks if the processed count meets or exceeds the expected total count.
        return currentCount >= totalCount;
    }

    public int getCurrentCount() {
        return currentCount;
    }

    public int getTotalCount() {
        return totalCount;
    }
}
