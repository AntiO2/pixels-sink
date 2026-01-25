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

package io.pixelsdb.pixels.sink.provider;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.util.DataTransform;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class TableEventKafkaProvider<T> extends TableEventProvider<Void>
{
    private static final Logger log = LoggerFactory.getLogger(TableEventKafkaProvider.class);
    private final Properties kafkaProperties;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String tableName;
    private KafkaConsumer<String, RowChangeEvent> consumer;

    public TableEventKafkaProvider(Properties kafkaProperties, String topic) throws IOException
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
        this.kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId() + "-" + topic);
        this.kafkaProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        this.kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        this.tableName = DataTransform.extractTableName(topic);
    }

    @Override
    protected void processLoop()
    {
        try
        {
            consumer = new KafkaConsumer<>(kafkaProperties);
            consumer.subscribe(Collections.singleton(topic));

            while (running.get())
            {
                try
                {
                    ConsumerRecords<String, RowChangeEvent> records = consumer.poll(Duration.ofSeconds(5));
                    if (!records.isEmpty())
                    {
                        log.info("{} Consumer poll returned {} records", tableName, records.count());
                        records.forEach(record ->
                        {
                            if (record.value() == null)
                            {
                                return;
                            }
                            metricsFacade.recordSerdRowChange();
                            putRowChangeEvent(record.value());
                        });
                    }
                } catch (InterruptException ignored)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (WakeupException e)
        {
            log.info("Consumer wakeup triggered for {}", tableName);
        } catch (Exception e)
        {
            log.info("Exception: {}", e.getMessage());
        } finally
        {
            if (consumer != null)
            {
                consumer.close(Duration.ofSeconds(5));
                log.info("Kafka consumer closed for {}", tableName);
            }
        }
    }

    @Override
    RowChangeEvent convertToTargetRecord(Void record)
    {
        throw new UnsupportedOperationException();
    }
}
