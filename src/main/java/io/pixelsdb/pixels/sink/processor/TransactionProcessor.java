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

package io.pixelsdb.pixels.sink.processor;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinator;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinatorFactory;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.event.TransactionEventProvider;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class TransactionProcessor implements Runnable, StoppableProcessor
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionProcessor.class);
    private final TransactionCoordinator transactionCoordinator;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final TransactionEventProvider transactionEventProvider;

    public TransactionProcessor(TransactionEventProvider transactionEventProvider)
    {
        this.transactionEventProvider = transactionEventProvider;
        this.transactionCoordinator = TransactionCoordinatorFactory.getCoordinator();
    }

    @Override
    public void run()
    {
        while (running.get())
        {
            try
            {
                SinkProto.TransactionMetadata transaction = transactionEventProvider.getEventQueue().take();
                try
                {
                    LOGGER.trace("Processing transaction event: {}", transaction.getId());
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
        LOGGER.info("Processor thread exited for transaction");
    }

    @Override
    public void stopProcessor()
    {
        LOGGER.info("Stopping transaction monitor");
        running.set(false);
    }
}
