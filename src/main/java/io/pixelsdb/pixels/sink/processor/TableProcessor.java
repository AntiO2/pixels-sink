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


package io.pixelsdb.pixels.sink.processor;


import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinator;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinatorFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.TableEventProvider;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @package: io.pixelsdb.pixels.sink.processor
 * @className: TableProcessor
 * @author: AntiO2
 * @date: 2025/9/26 11:01
 */
public class TableProcessor implements StoppableProcessor, Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableProcessor.class);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread processorThread;
    private final TransactionCoordinator transactionCoordinator;
    private final TableEventProvider tableEventProvider;
    private final SchemaTableName schemaTableName;
    public TableProcessor(TableEventProvider tableEventProvider, SchemaTableName schemaTableName)
    {
        this.transactionCoordinator = TransactionCoordinatorFactory.getCoordinator();
        this.tableEventProvider = tableEventProvider;
        this.schemaTableName = schemaTableName;
    }

    @Override
    public void run()
    {
        processorThread = new Thread(this::processLoop, "processor-table" + schemaTableName.getTableName());
        processorThread.start();
    }

    private void processLoop()
    {
        while (running.get())
        {
            try
            {
                RowChangeEvent event = tableEventProvider.getSourceEventQueue().take();
                try
                {
                    transactionCoordinator.processRowEvent(event);
                } catch (SinkException e)
                {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
        LOGGER.info("Processor thread exited for {}", schemaTableName.toString());
    }

    @Override
    public void stopProcessor()
    {
        LOGGER.info("Stopping transaction monitor");
        running.set(false);
    }
}
