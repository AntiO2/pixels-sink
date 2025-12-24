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

package io.pixelsdb.pixels.sink.source.engine;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.SinkSource;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SinkEngineSource implements SinkSource {
    private final PixelsDebeziumConsumer consumer;
    private DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private ExecutorService executor;
    private volatile boolean running = true;

    public SinkEngineSource() {
        this.consumer = new PixelsDebeziumConsumer();
    }

    public void start() {
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
    public void stopProcessor() {
        try {
            if (engine != null) {
                engine.close();
            }
            if (executor != null) {
                executor.shutdown();
            }
            consumer.stopProcessor();
        } catch (Exception e) {
            throw new RuntimeException("Failed to stop PixelsSinkEngine", e);
        } finally {
            running = false;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
