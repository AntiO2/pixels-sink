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
 
package io.pixelsdb.pixels.sink.writer.retina;


import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.util.FlushRateLimiter;
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
    protected final ReentrantLock bufferLock = new ReentrantLock();
    protected final String tableName;
    protected final long flushInterval;
    protected final FlushRateLimiter flushRateLimiter;
    protected final SinkContextManager sinkContextManager;
    protected final String freshnessLevel;
    // Shared state (protected by lock)
    protected List<RowChangeEvent> buffer = new LinkedList<>();
    protected volatile String currentTxId = null;
    protected String txId = null;
    protected ScheduledFuture<?> flushTask = null;
    protected String fullTableName;
    protected PixelsSinkConfig config;
    protected MetricsFacade metricsFacade = MetricsFacade.getInstance();
    protected TransactionMode transactionMode;
    protected TableWriter(String tableName, int bucketId)
    {
        this.config = PixelsSinkConfigFactory.getInstance();
        this.tableName = tableName;
        this.flushInterval = config.getFlushIntervalMs();
        this.flushRateLimiter = FlushRateLimiter.getInstance();
        this.sinkContextManager = SinkContextManager.getInstance();
        this.freshnessLevel = config.getSinkMonitorFreshnessLevel();
        this.delegate = new RetinaServiceProxy(bucketId);
        this.transactionMode = config.getTransactionMode();
    }

    /**
     * Helper: add insert/delete data into proto builder.
     */
    protected static void addUpdateData(RowChangeEvent rowChangeEvent,
                                        RetinaProto.TableUpdateData.Builder builder) throws SinkException
    {
        switch (rowChangeEvent.getOp())
        {
            case SNAPSHOT, INSERT ->
            {
                RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
                insertDataBuilder.addIndexKeys(rowChangeEvent.getAfterKey());
                insertDataBuilder.addAllColValues(rowChangeEvent.getAfterData());
                builder.addInsertData(insertDataBuilder);
            }
            case UPDATE ->
            {
                RetinaProto.UpdateData.Builder updateDataBuilder = RetinaProto.UpdateData.newBuilder();
                updateDataBuilder.addIndexKeys(rowChangeEvent.getAfterKey());
                updateDataBuilder.addAllColValues(rowChangeEvent.getAfterData());
                builder.addUpdateData(updateDataBuilder);
            }
            case DELETE ->
            {
                RetinaProto.DeleteData.Builder deleteDataBuilder = RetinaProto.DeleteData.newBuilder();
                deleteDataBuilder.addIndexKeys(rowChangeEvent.getBeforeKey());
                builder.addDeleteData(deleteDataBuilder);
            }
            case UNRECOGNIZED ->
            {
                throw new SinkException("Unrecognized op: " + rowChangeEvent.getOp());
            }
        }
    }

    protected abstract Logger getLOGGER();

    public boolean write(RowChangeEvent event, SinkContext ctx)
    {
        try
        {
            bufferLock.lock();
            try
            {
                if(!transactionMode.equals(TransactionMode.RECORD))
                {
                    txId = ctx.getSourceTxId();
                }
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
                        bufferLock.lock();
                        try
                        {
                            if (transactionMode.equals(TransactionMode.RECORD) || txId.equals(currentTxId))
                            {
                                flush();
                            }
                        } finally
                        {
                            bufferLock.unlock();
                        }
                    } catch (Exception e)
                    {
                        getLOGGER().error("Scheduled flush failed for table {}", tableName, e);
                    }
                }, flushInterval, TimeUnit.MILLISECONDS);
            } finally
            {
                bufferLock.unlock();
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
