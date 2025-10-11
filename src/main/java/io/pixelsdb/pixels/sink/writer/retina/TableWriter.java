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


package io.pixelsdb.pixels.sink.writer.retina;


import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @package: io.pixelsdb.pixels.sink.writer.retina
 * @className: TableWriter
 * @author: AntiO2
 * @date: 2025/9/27 09:58
 */
public abstract class TableWriter
{

    protected final RetinaServiceProxy delegate; // physical writer
    protected final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    protected final ReentrantLock lock = new ReentrantLock();
    protected final String tableName;
    protected final long flushInterval;
    // Shared state (protected by lock)
    protected List<RowChangeEvent> buffer = new LinkedList<>();
    protected volatile String currentTxId = null;
    protected String txId = null;
    protected ScheduledFuture<?> flushTask = null;
    protected String fullTableName;
    protected PixelsSinkConfig config;
    protected MetricsFacade metricsFacade = MetricsFacade.getInstance();


    protected TableWriter(String tableName)
    {
        this.config = PixelsSinkConfigFactory.getInstance();
        this.tableName = tableName;
        this.delegate = new RetinaServiceProxy();
        this.flushInterval = config.getFlushIntervalMs();

    }

    /**
     * Helper: add insert/delete data into proto builder.
     */
    protected static void addUpdateData(RowChangeEvent rowChangeEvent,
                                        RetinaProto.TableUpdateData.Builder builder) throws SinkException
    {
        if (rowChangeEvent.hasBeforeData())
        {
            RetinaProto.DeleteData.Builder deleteDataBuilder = RetinaProto.DeleteData.newBuilder();
            deleteDataBuilder.addIndexKeys(rowChangeEvent.getBeforeKey());
            builder.addDeleteData(deleteDataBuilder);
        }

        if (rowChangeEvent.hasAfterData())
        {
            RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
            insertDataBuilder.addIndexKeys(rowChangeEvent.getAfterKey());
            insertDataBuilder.addAllColValues(rowChangeEvent.getAfterData());
            builder.addInsertData(insertDataBuilder);
        }
    }

    protected abstract Logger getLOGGER();

    public boolean write(RowChangeEvent event, SinkContext ctx)
    {
        try
        {
            lock.lock();
            try
            {
                txId = ctx.getSourceTxId();
                // If this is a new transaction, flush the old one
                if (needFlush())
                {
                    if (flushTask != null)
                    {
                        flushTask.cancel(false);
                    }
                    flush();

                }
                currentTxId = txId;
                if (fullTableName == null)
                {
                    fullTableName = event.getFullTableName();
                }
                buffer.add(event);

                // Reset scheduled flush: cancel old one and reschedule
                if (flushTask != null && !flushTask.isDone())
                {
                    flushTask.cancel(false);
                }
                flushTask = scheduler.schedule(() ->
                {
                    try
                    {
                        lock.lock();
                        try
                        {
                            if (txId.equals(currentTxId))
                            {
                                flush();
                            }
                        } finally
                        {
                            lock.unlock();
                        }
                    } catch (Exception e)
                    {
                        getLOGGER().error("Scheduled flush failed for table {}", tableName, e);
                    }
                }, flushInterval, TimeUnit.MILLISECONDS);
            } finally
            {
                lock.unlock();
            }
            return true;
        } catch (Exception e)
        {
            getLOGGER().error("Write failed for table {}", tableName, e);
            return false;
        }
    }

    public abstract void flush();

    protected abstract boolean needFlush();

    public void close()
    {
        scheduler.shutdown();
        try
        {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            delegate.close();
        } catch (InterruptedException ignored)
        {
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
