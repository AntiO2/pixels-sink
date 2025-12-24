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

package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.common.sink.SinkProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.SinkSource;
import io.pixelsdb.pixels.sink.source.SinkSourceFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;

public class PixelsSinkProvider implements SinkProvider {
    private SinkSource sinkSource;

    public void start(ConfigFactory config) {
        PixelsSinkConfigFactory.initialize(config);
        MetricsFacade.getInstance();
        sinkSource = SinkSourceFactory.createSinkSource();
        sinkSource.start();
    }

    @Override
    public void shutdown() {
        sinkSource.stopProcessor();
    }

    @Override
    public boolean isRunning() {
        return sinkSource.isRunning();
    }
}
