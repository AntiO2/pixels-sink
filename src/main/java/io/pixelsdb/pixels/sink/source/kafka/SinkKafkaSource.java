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

package io.pixelsdb.pixels.sink.source.kafka;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.config.factory.KafkaPropFactorySelector;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.processor.MonitorThreadManager;
import io.pixelsdb.pixels.sink.processor.TopicProcessor;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.source.SinkSource;

import java.util.Properties;

public class SinkKafkaSource implements SinkSource
{
    private MonitorThreadManager manager;
    private volatile boolean running = true;

    @Override
    public void start()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        KafkaPropFactorySelector kafkaPropFactorySelector = new KafkaPropFactorySelector();

        Properties transactionKafkaProperties = kafkaPropFactorySelector
                .getFactory(PixelsSinkConstants.TRANSACTION_KAFKA_PROP_FACTORY)
                .createKafkaProperties(pixelsSinkConfig);
        TransactionProcessor transactionProcessor = null; // TODO: new TransactionProcessor();

        Properties topicKafkaProperties = kafkaPropFactorySelector
                .getFactory(PixelsSinkConstants.ROW_RECORD_KAFKA_PROP_FACTORY)
                .createKafkaProperties(pixelsSinkConfig);
        TopicProcessor topicMonitor = new TopicProcessor(pixelsSinkConfig, topicKafkaProperties);

        manager = new MonitorThreadManager();
        manager.startMonitor(transactionProcessor);
        manager.startMonitor(topicMonitor);
    }


    @Override
    public void stopProcessor()
    {
        manager.shutdown();
        running = false;
    }

    @Override
    public boolean isRunning()
    {
        return running;
    }
}
