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
 
package io.pixelsdb.pixels.sink.processor;


import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.provider.TableEventProvider;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;
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
    private final PixelsSinkWriter pixelsSinkWriter;
    private final TableEventProvider<?> tableEventProvider;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private Thread processorThread;
    private boolean tableAdded = false;
    public TableProcessor(TableEventProvider<?> tableEventProvider)
    {
        this.pixelsSinkWriter = PixelsSinkWriterFactory.getWriter();
        this.tableEventProvider = tableEventProvider;
    }

    @Override
    public void run()
    {
        processorThread = new Thread(this::processLoop);
        processorThread.start();
    }

    private void processLoop()
    {
        while (running.get())
        {
            RowChangeEvent event = tableEventProvider.getRowChangeEvent();
            if (event == null)
            {
                continue;
            }
            if(!tableAdded &&
                    PixelsSinkConfigFactory.getInstance().getSinkMonitorFreshnessLevel().equals("embed"))
            {
                tableAdded = true;
                FreshnessClient.getInstance().addMonitoredTable(event.getTable());
            }
            pixelsSinkWriter.writeRow(event);
        }
        LOGGER.info("Processor thread exited");
    }

    @Override
    public void stopProcessor()
    {
        LOGGER.info("Stopping transaction monitor");
        running.set(false);
        processorThread.interrupt();
    }
}
