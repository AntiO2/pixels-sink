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


import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @package: io.pixelsdb.pixels.sink.provider
 * @className: TransactionEventKafkaProvider
 * @author: AntiO2
 * @date: 2025/9/25 13:40
 */
public class TransactionEventKafkaProvider<T> extends TransactionEventProvider<T>
{
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String transactionTopic;
    private final KafkaConsumer<String, SinkProto.TransactionMetadata> consumer;

    private TransactionEventKafkaProvider()
    {
        Properties kafkaProperties = new Properties();
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.transactionTopic = pixelsSinkConfig.getTopicPrefix() + "." + pixelsSinkConfig.getTransactionTopicSuffix();
        this.consumer = new KafkaConsumer<>(kafkaProperties);
    }


    @Override
    public void processLoop()
    {
        consumer.subscribe(Collections.singletonList(transactionTopic));
        while (running.get())
        {
            try
            {

                ConsumerRecords<String, SinkProto.TransactionMetadata> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, SinkProto.TransactionMetadata> record : records)
                {
                    if (record.value() == null)
                    {
                        continue;
                    }
                    putTargetEvent(record.value());
                }
            } catch (WakeupException e)
            {
                if (running.get())
                {
                    // LOGGER.warn("Consumer wakeup unexpectedly", e);
                }
            } catch (Exception e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    SinkProto.TransactionMetadata convertToTargetRecord(T record)
    {
        throw new UnsupportedOperationException();
    }

}
