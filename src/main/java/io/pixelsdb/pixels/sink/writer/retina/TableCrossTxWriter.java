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
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @package: io.pixelsdb.pixels.sink.writer.retina
 * @className: TableCrossTxWriter
 * @author: AntiO2
 * @date: 2025/9/27 09:36
 */
public class TableCrossTxWriter extends TableWriter
{
    @Getter
    private final Logger LOGGER = LoggerFactory.getLogger(TableCrossTxWriter.class);
    private final int flushBatchSize;
    protected final ReentrantLock writeLock = new ReentrantLock();
    private final int bucketId;

    public TableCrossTxWriter(String t, int bucketId)
    {
        super(t, bucketId);
        flushBatchSize = config.getFlushBatchSize();
        this.bucketId = bucketId;
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush(List<RowChangeEvent> batch)
    {
        writeLock.lock();
        try
        {
            String txId = null;
            List<RowChangeEvent> smallBatch = null;
            List<String> txIds = new ArrayList<>();
            List<String> fullTableName = new ArrayList<>();
            List<RetinaProto.TableUpdateData.Builder> tableUpdateDataBuilderList = new LinkedList<>();
            List<Integer> tableUpdateCount = new ArrayList<>();
            for (RowChangeEvent event : batch)
            {
                String currTxId = event.getTransaction().getId();
                if (!currTxId.equals(txId))
                {
                    if (smallBatch != null && !smallBatch.isEmpty())
                    {
                        RetinaProto.TableUpdateData.Builder builder = buildTableUpdateDataFromBatch(txId, smallBatch);
                        if (builder == null)
                        {
                            continue;
                        }
                        tableUpdateDataBuilderList.add(builder);
                        tableUpdateCount.add(smallBatch.size());
                    }
                    txIds.add(currTxId);
                    fullTableName.add(event.getFullTableName());
                    txId = currTxId;
                    smallBatch = new LinkedList<>();
                }
                smallBatch.add(event);
            }

            if (smallBatch != null)
            {
                RetinaProto.TableUpdateData.Builder builder = buildTableUpdateDataFromBatch(txId, smallBatch);
                if (builder != null)
                {
                    tableUpdateDataBuilderList.add(buildTableUpdateDataFromBatch(txId, smallBatch));
                    tableUpdateCount.add(smallBatch.size());
                }
            }

            // flushRateLimiter.acquire(batch.size());
            long txStartTime = System.currentTimeMillis();

//            if(freshnessLevel.equals("embed"))
//            {
//                long freshness_ts = txStartTime * 1000;
//                FreshnessClient.getInstance().addMonitoredTable(tableName);
//                DataTransform.updateTimeStamp(tableUpdateDataBuilderList, freshness_ts);
//            }

            List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>(tableUpdateDataBuilderList.size());
            for (RetinaProto.TableUpdateData.Builder tableUpdateDataItem : tableUpdateDataBuilderList)
            {
                tableUpdateData.add(tableUpdateDataItem.build());
            }
            CompletableFuture<RetinaProto.UpdateRecordResponse> updateRecordResponseCompletableFuture = delegate.writeBatchAsync(batch.get(0).getSchemaName(), tableUpdateData);

            updateRecordResponseCompletableFuture.thenAccept(
                    resp ->
                    {
                        if (resp.getHeader().getErrorCode() != 0)
                        {
                            failCtxs(txIds);
                        } else
                        {
                            long txEndTime = System.currentTimeMillis();
                            if (freshnessLevel.equals("row"))
                            {
                                metricsFacade.recordFreshness(txEndTime - txStartTime);
                            }
                            updateCtxCounters(txIds, fullTableName, tableUpdateCount);
                        }
                    }
            );
        } finally
        {
            writeLock.unlock();
        }
    }

    private void failCtxs(List<String> txIds)
    {
        for (String writeTxId : txIds)
        {
            SinkContext sinkContext = SinkContextManager.getInstance().getSinkContext(writeTxId);
            if (sinkContext != null)
            {
                sinkContext.setFailed(true);
            }
        }
    }

    private void updateCtxCounters(List<String> txIds, List<String> fullTableName, List<Integer> tableUpdateCount)
    {
        writeLock.lock();
        for (int i = 0; i < txIds.size(); i++)
        {
            metricsFacade.recordRowEvent(tableUpdateCount.get(i));
            String writeTxId = txIds.get(i);
            SinkContext sinkContext = SinkContextManager.getInstance().getSinkContext(writeTxId);

            try
            {
                sinkContext.tableCounterLock.lock();
                sinkContext.recordTimestamp(fullTableName.get(i), LocalDateTime.now());
                sinkContext.updateCounter(fullTableName.get(i), tableUpdateCount.get(i));
                if(sinkContext.isCompleted())
                {
                    SinkContextManager.getInstance().endTransaction(sinkContext);
                }
            } finally
            {
                sinkContext.tableCounterLock.unlock();
            }
        }
        writeLock.unlock();
    }

    protected RetinaProto.TableUpdateData.Builder buildTableUpdateDataFromBatch(String txId, List<RowChangeEvent> smallBatch)
    {
        SinkContext sinkContext = SinkContextManager.getInstance().getSinkContext(txId);
        if (sinkContext == null)
        {
            return null;
        }
        try
        {
            sinkContext.getLock().lock();
            while (sinkContext.getPixelsTransCtx() == null)
            {
                LOGGER.warn("Wait for tx to begin trans: {}", txId); // CODE SHOULD NOT REACH HERE
                sinkContext.getCond().await();
            }
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        } finally
        {
            sinkContext.getLock().unlock();
        }
        RowChangeEvent event1 = smallBatch.get(0);

        RetinaProto.TableUpdateData.Builder builder = RetinaProto.TableUpdateData.newBuilder()
                .setTimestamp(sinkContext.getTimestamp())
                .setPrimaryIndexId(event1.getTableMetadata().getPrimaryIndexKeyId())
                .setTableName(tableName);
        try
        {
            for (RowChangeEvent smallEvent : smallBatch)
            {
                addUpdateData(smallEvent, builder);
            }
        } catch (SinkException e)
        {
            throw new RuntimeException("Flush failed for table " + tableName, e);
        }
        return builder;
    }

    @Override
    protected boolean needFlush()
    {
        return buffer.size() >= flushBatchSize;
    }
}
