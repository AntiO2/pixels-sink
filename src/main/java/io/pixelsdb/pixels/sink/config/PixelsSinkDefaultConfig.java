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

public class PixelsSinkDefaultConfig
{
    public static final String DATA_SOURCE = "kafka";
    public static final String PROPERTIES_PATH = "pixels-sink.properties";
    public static final String CSV_SINK_PATH = "./data";

    public static final String SINK_MODE = "csv";

    public static final int SINK_CSV_RECORD_FLUSH = 1000;

    public static final int SINK_THREAD = 32;
    public static final int SINK_CONSUMER_THREAD = 8;

    // Transaction Service
    public static final int TRANSACTION_BATCH_SIZE = 100;

    // REMOTE BUFFER
    public static final String SINK_REMOTE_HOST = "localhost";
    public static final short SINK_REMOTE_PORT = 22942;
    public static final int SINK_BATCH_SIZE = 100;
    public static final int SINK_TIMEOUT_MS = 5000;
    public static final int SINK_FLUSH_INTERVAL_MS = 5000;
    public static final int SINK_MAX_RETRIES = 3;
    public static final boolean SINK_CSV_ENABLE_HEADER = false;
    public static final String SINK_RETINA_MODE = "stub";

    // Monitor Config
    public static final boolean SINK_MONITOR_ENABLED = true;
    public static final short SINK_MONITOR_PORT = 9464;

    // Mock RPC
    public static final boolean SINK_RPC_ENABLED = true;
    public static final int MOCK_RPC_DELAY = 100;
    public static final String MAX_RECORDS_PER_FILE = "100000";
}
