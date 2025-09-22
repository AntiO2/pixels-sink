/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinator;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinatorFactory;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionMonitor implements Runnable, StoppableMonitor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionMonitor.class);

    private final String transactionTopic;
    private final KafkaConsumer<String, SinkProto.TransactionMetadata> consumer;
    private final TransactionCoordinator transactionCoordinator;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final BlockingQueue<SinkProto.TransactionMetadata> eventQueue = new LinkedBlockingQueue<>(10000);
    private Thread processorThread;

    public TransactionMonitor(PixelsSinkConfig pixelsSinkConfig, Properties kafkaProperties)
    {
        this.transactionTopic = pixelsSinkConfig.getTopicPrefix() + "." + pixelsSinkConfig.getTransactionTopicSuffix();
        this.consumer = new KafkaConsumer<>(kafkaProperties);
        this.transactionCoordinator = TransactionCoordinatorFactory.getCoordinator();
    }

    @Override
    public void run()
    {
        try
        {
            consumer.subscribe(Collections.singletonList(transactionTopic));
            LOGGER.info("Started transaction monitor for topic: {}", transactionTopic);

            processorThread = new Thread(this::processLoop, "processor-" + transactionTopic);
            processorThread.start();

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
                        LOGGER.warn("Consumer wakeup unexpectedly", e);
                    }
                } catch (Exception e)
                {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        } finally
        {
            closeResources();
            if (processorThread != null)
            {
                processorThread.interrupt();
                try
                {
                    processorThread.join();
                } catch (InterruptedException ignored)
                {
                }
            }
            LOGGER.info("Transaction monitor stopped");
        }
    }

    private void processLoop()
    {
        while (running.get() || !eventQueue.isEmpty())
        {
            try
            {
                SinkProto.TransactionMetadata transaction = eventQueue.take();
                try
                {
                    LOGGER.debug("Processing transaction event: {}", transaction.getId());
                    transactionCoordinator.processTransactionEvent(transaction);
                } catch (SinkException e)
                {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("Processor thread exited for {}", transactionTopic);
    }

    @Override
    public void stopMonitor()
    {
        LOGGER.info("Stopping transaction monitor");
        running.set(false);
        consumer.wakeup();
    }

    private void closeResources()
    {
        try
        {
            if (consumer != null)
            {
                consumer.close(Duration.ofSeconds(5));
                LOGGER.debug("Kafka consumer closed");
            }
        } catch (Exception e)
        {
            LOGGER.warn("Error closing Kafka consumer", e);
        }
    }
}
