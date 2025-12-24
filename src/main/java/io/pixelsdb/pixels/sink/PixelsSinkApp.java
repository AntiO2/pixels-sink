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

import io.pixelsdb.pixels.sink.config.CommandLineConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.pixelsdb.pixels.sink.source.SinkSource;
import io.pixelsdb.pixels.sink.source.SinkSourceFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;
import io.pixelsdb.pixels.sink.writer.retina.SinkContextManager;
import io.pixelsdb.pixels.sink.writer.retina.TransactionProxy;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Run PixelsSink as a server
 */
public class PixelsSinkApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsSinkApp.class);
    private static SinkSource sinkSource;
    private static HTTPServer prometheusHttpServer;
    private static FreshnessClient freshnessClient;

    public static void main(String[] args) throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
            TransactionProxy.staticClose();
            sinkSource.stopProcessor();
            LOGGER.info("Pixels Sink Server shutdown complete");
            if (config.getSinkMonitorFreshnessLevel().equals("embed") && freshnessClient != null) {
                freshnessClient.stop();
            }
            if (prometheusHttpServer != null) {
                prometheusHttpServer.close();
            }
            MetricsFacade.getInstance().stop();
            PixelsSinkWriter pixelsSinkWriter = PixelsSinkWriterFactory.getWriter();
            if (pixelsSinkWriter != null) {
                try {
                    pixelsSinkWriter.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }));

        init(args);
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        sinkSource = SinkSourceFactory.createSinkSource();

        try {
            if (config.isMonitorEnabled()) {
                DefaultExports.initialize();
                prometheusHttpServer = new HTTPServer(config.getMonitorPort());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (config.getSinkMonitorFreshnessLevel().equals("embed")) {
            freshnessClient = FreshnessClient.getInstance();
            freshnessClient.start();
        }
        sinkSource.start();
    }

    private static void init(String[] args) throws IOException {
        CommandLineConfig cmdLineConfig = new CommandLineConfig(args);
        PixelsSinkConfigFactory.initialize(cmdLineConfig.getConfigPath());
        MetricsFacade metricsFacade = MetricsFacade.getInstance();
        metricsFacade.setSinkContextManager(SinkContextManager.getInstance());
    }
}
