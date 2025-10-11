/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.sink.config.CommandLineConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.SinkSource;
import io.pixelsdb.pixels.sink.source.SinkSourceFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.retina.TransactionProxy;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Run PixelsSink as a server
 */
public class PixelsSinkApp
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsSinkApp.class);
    private static SinkSource sinkSource;
    private static HTTPServer prometheusHttpServer;


    public static void main(String[] args) throws IOException
    {
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            TransactionProxy.Instance().close();
            sinkSource.stopProcessor();
            LOGGER.info("Pixels Sink Server shutdown complete");
            if (prometheusHttpServer != null)
            {
                prometheusHttpServer.close();
            }
            MetricsFacade.getInstance().stop();
        }));

        init(args);
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        sinkSource = SinkSourceFactory.createSinkSource();

        try
        {
            if (config.isMonitorEnabled())
            {
                DefaultExports.initialize();
                prometheusHttpServer = new HTTPServer(config.getMonitorPort());
            }
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        sinkSource.start();
    }

    private static void init(String[] args) throws IOException
    {
        CommandLineConfig cmdLineConfig = new CommandLineConfig(args);
        PixelsSinkConfigFactory.initialize(cmdLineConfig.getConfigPath());
        MetricsFacade.getInstance();
    }
}
