package io.pixelsdb.pixels.sink.source;/*
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


import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SinkEngineSource implements SinkSource
{
    private final PixelsDebeziumConsumer consumer;
    private DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private ExecutorService executor;
    private volatile boolean running = true;

    public SinkEngineSource()
    {
        this.consumer = new PixelsDebeziumConsumer();
    }

    public void start()
    {
        Properties debeziumProps = PixelsSinkConfigFactory.getInstance()
                .getConfig().extractPropertiesByPrefix("debezium.", true);

        this.engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(debeziumProps)
                .notifying(consumer)
                .build();

        this.executor = Executors.newSingleThreadExecutor();
        this.executor.execute(engine);
    }

    @Override
    public void stopProcessor()
    {
        try
        {
            if (engine != null)
            {
                engine.close();
            }
            if (executor != null)
            {
                executor.shutdown();
            }
            consumer.stopProcessor();
        } catch (Exception e)
        {
            throw new RuntimeException("Failed to stop PixelsSinkEngine", e);
        } finally
        {
            running = false;
        }
    }

    @Override
    public boolean isRunning()
    {
        return running;
    }
}
