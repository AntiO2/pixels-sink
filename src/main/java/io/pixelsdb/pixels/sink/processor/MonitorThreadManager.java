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

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MonitorThreadManager
{
    private final List<Runnable> monitors = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(PixelsSinkConstants.MONITOR_NUM);

    public void startMonitor(Runnable monitor)
    {
        monitors.add(monitor);
        executor.submit(monitor);
    }

    public void shutdown()
    {
        stopMonitors();
        shutdownExecutor();
        awaitTermination();
    }

    private void stopMonitors()
    {
        monitors.forEach(monitor ->
        {
            if (monitor instanceof StoppableProcessor)
            {
                ((StoppableProcessor) monitor).stopProcessor();
            }
        });
    }

    private void shutdownExecutor()
    {
        executor.shutdown();
    }

    private void awaitTermination()
    {
        try
        {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS))
            {
                executor.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            executor.shutdownNow();
        }
    }
}
