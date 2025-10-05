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

import io.pixelsdb.pixels.common.sink.SinkProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.processor.MainProcessor;
import io.pixelsdb.pixels.sink.processor.MetricsFacade;
import io.pixelsdb.pixels.sink.processor.SinkEngineProcessor;
import io.pixelsdb.pixels.sink.processor.SinkKafkaProcessor;

public class PixelsSinkProvider implements SinkProvider
{
    private MainProcessor mainProcessor;

    public void start(ConfigFactory config)
    {
        PixelsSinkConfigFactory.initialize(config);
        MetricsFacade.getInstance();
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        String dataSource = pixelsSinkConfig.getDataSource();
        if(dataSource.equals("kafka"))
        {
            mainProcessor = new SinkKafkaProcessor();
        } else if(dataSource.equals("engine"))
        {
            mainProcessor = new SinkEngineProcessor();
        } else
        {
            throw new IllegalStateException("Unsupported data source type: " + dataSource);
        }
        mainProcessor.start();
    }

    @Override
    public void shutdown()
    {
        mainProcessor.stopProcessor();
    }

    @Override
    public boolean isRunning()
    {
        return  mainProcessor.isRunning();
    }
}
