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

package io.pixelsdb.pixels.sink.config;

public final class PixelsSinkConstants {
    public static final String ROW_RECORD_KAFKA_PROP_FACTORY = "row-record";
    public static final String TRANSACTION_KAFKA_PROP_FACTORY = "transaction";
    public static final int MONITOR_NUM = 2;
    public static final int MAX_QUEUE_SIZE = 100_000;
    public static final String SNAPSHOT_TX_PREFIX = "SNAPSHOT-";

    private PixelsSinkConstants() {
    }
}
