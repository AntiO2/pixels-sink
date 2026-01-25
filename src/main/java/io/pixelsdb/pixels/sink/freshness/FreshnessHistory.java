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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FreshnessHistory {
    private final ConcurrentLinkedQueue<Record> history = new ConcurrentLinkedQueue<>();

    public void record(double freshnessMill) {
        history.offer(new Record(
                System.currentTimeMillis(),
                freshnessMill,
                null
        ));
    }
    public void record(double freshnessMill, double queryTimeMill) {
        history.offer(new Record(
                System.currentTimeMillis(),
                freshnessMill,
                queryTimeMill
        ));
    }
    public List<Record> pollAll() {
        if (history.isEmpty()) {
            return Collections.emptyList();
        }
        List<Record> records = new ArrayList<>();
        Record record;
        while ((record = history.poll()) != null) {
            records.add(record);
        }
        return records;
    }

    public record Record(
            long timestamp,
            double freshness,
            Double queryTimeMillis
    ) {
        @Override
        public String toString() {
            if (queryTimeMillis == null) {
                return timestamp + "," + freshness;
            }
            return timestamp + "," + freshness + "," + queryTimeMillis;
        }
    }

}