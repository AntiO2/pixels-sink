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


package io.pixelsdb.pixels.sink.event;


import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @package: io.pixelsdb.pixels.sink.event
 * @className: TransactionEventKafkaProvider
 * @author: AntiO2
 * @date: 2025/9/25 13:40
 */
public class TransactionEventKafkaProvider implements TransactionEventProvider, Runnable, Closeable
{
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String transactionTopic;
    private final KafkaConsumer<String, SinkProto.TransactionMetadata> consumer;
    private final BlockingQueue<SinkProto.TransactionMetadata> eventQueue = new LinkedBlockingQueue<>(10000);
    private TransactionEventKafkaProvider()
    {
        Properties kafkaProperties = new Properties();
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();

        this.transactionTopic = pixelsSinkConfig.getTopicPrefix() + "." + pixelsSinkConfig.getTransactionTopicSuffix();
        this.consumer = new KafkaConsumer<>(kafkaProperties);
    }

    @Override
    public BlockingQueue<SinkProto.TransactionMetadata> getEventQueue()
    {
        return eventQueue;
    }

    @Override
    public void run()
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
                    try
                    {
                        eventQueue.put(record.value());
                    } catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                    }
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
    public void close() throws IOException
    {
        running.set(true);
    }
}
