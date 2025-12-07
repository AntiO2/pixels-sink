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

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public abstract class EventProvider<SOURCE_RECORD_T, TARGET_RECORD_T> implements Runnable, Closeable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(EventProvider.class);

    private static final int BATCH_SIZE = 64;
    private static final int THREAD_NUM = 4;
    private static final long MAX_WAIT_MS = 5; // configurable
    protected final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final BlockingQueue<SOURCE_RECORD_T> rawEventQueue = new LinkedBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE);
    private final BlockingQueue<TARGET_RECORD_T> eventQueue = new LinkedBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE);
    private final ExecutorService decodeExecutor = Executors.newFixedThreadPool(THREAD_NUM);

    private Thread providerThread;


    @Override
    public void run()
    {
        providerThread = new Thread(this::processLoop);
        providerThread.start();
    }

    @Override
    public void close()
    {
        this.providerThread.interrupt();
        decodeExecutor.shutdown();
    }

    protected void processLoop()
    {
        List<SOURCE_RECORD_T> sourceBatch = new ArrayList<>(BATCH_SIZE);
        while (true)
        {
            try
            {
                sourceBatch.clear();
                // take first element (blocking)
                SOURCE_RECORD_T first = getRawEvent();
                sourceBatch.add(first);
                long startTime = System.nanoTime();

                // keep polling until sourceBatch full or timeout
                while (sourceBatch.size() < BATCH_SIZE)
                {
                    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
                    long remainingMs = MAX_WAIT_MS - elapsedMs;
                    if (remainingMs <= 0)
                    {
                        break;
                    }

                    SOURCE_RECORD_T next = pollRawEvent(remainingMs);
                    if (next == null)
                    {
                        break;
                    }
                    sourceBatch.add(next);
                }

                // parallel decode
                List<Future<TARGET_RECORD_T>> futures = new ArrayList<>(sourceBatch.size());
                for (SOURCE_RECORD_T data : sourceBatch)
                {
                    futures.add(decodeExecutor.submit(() ->
                            convertToTargetRecord(data)));
                }

                // ordered put into queue
                for (Future<TARGET_RECORD_T> future : futures)
                {
                    try
                    {
                        TARGET_RECORD_T event = future.get();
                        if (event != null)
                        {
                            recordSerdEvent();
                            putTargetEvent(event);
                        }
                    } catch (ExecutionException e)
                    {
                        LOGGER.warn("Decode failed: {}", String.valueOf(e.getCause()));
                    }
                }
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    abstract TARGET_RECORD_T convertToTargetRecord(SOURCE_RECORD_T record);

    protected TARGET_RECORD_T getTargetEvent()
    {
        try
        {
            return eventQueue.take();
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        return null;
    }

    protected void putTargetEvent(TARGET_RECORD_T event)
    {
        try
        {
            eventQueue.put(event);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    protected void putRawEvent(SOURCE_RECORD_T record)
    {
        try
        {
            rawEventQueue.put(record);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    protected SOURCE_RECORD_T getRawEvent()
    {
        try
        {
            return rawEventQueue.take();
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    protected SOURCE_RECORD_T pollRawEvent(long remainingMs)
    {
        try
        {
            return rawEventQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e)
        {
            return null;
        }
    }

    abstract protected void recordSerdEvent();
}
