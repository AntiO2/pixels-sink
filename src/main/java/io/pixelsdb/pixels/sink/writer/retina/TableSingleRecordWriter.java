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

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.prometheus.client.Summary;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TableSingleRecordWriter extends TableCrossTxWriter
{
    @Getter
    private final Logger LOGGER = LoggerFactory.getLogger(TableSingleRecordWriter.class);
    private final TransactionProxy transactionProxy;

    public TableSingleRecordWriter(String t, int bucketId)
    {
        super(t, bucketId);
        this.transactionProxy = TransactionProxy.Instance();
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush(List<RowChangeEvent> batch)
    {
        TransContext pixelsTransContext = transactionProxy.getNewTransContext(tableName);
        writeLock.lock();
        try
        {
            List<RetinaProto.TableUpdateData.Builder> tableUpdateDataBuilderList = new LinkedList<>();
            for (RowChangeEvent event : batch)
            {
                event.setTimeStamp(pixelsTransContext.getTimestamp());
                event.updateIndexKey();
            }

            RetinaProto.TableUpdateData.Builder builder = buildTableUpdateDataFromBatch(pixelsTransContext, batch);
            if (builder != null)
            {
                tableUpdateDataBuilderList.add(builder);
            }

            // flushRateLimiter.acquire(batch.size());
            long txStartTime = System.currentTimeMillis();

            List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>(tableUpdateDataBuilderList.size());
            for (RetinaProto.TableUpdateData.Builder tableUpdateDataItem : tableUpdateDataBuilderList)
            {
                tableUpdateData.add(tableUpdateDataItem.build());
            }

            final Summary.Timer startWriteLatencyTimer = metricsFacade.startWriteLatencyTimer(tableName);
            CompletableFuture<RetinaProto.UpdateRecordResponse> updateRecordResponseCompletableFuture = delegate.writeBatchAsync(batch.get(0).getSchemaName(), tableUpdateData);

            updateRecordResponseCompletableFuture.thenAccept(
                    resp ->
                    {
                        if(freshness_embed)
                        {
                            FreshnessClient.getInstance().addMonitoredTable(tableName);
                        }

                        if (resp.getHeader().getErrorCode() != 0)
                        {
                            transactionProxy.rollbackTrans(pixelsTransContext);
                        } else
                        {
                            metricsFacade.recordRowEvent(batch.size());
                            long txEndTime = System.currentTimeMillis();
                            if (freshnessLevel.equals("row"))
                            {
                                metricsFacade.recordFreshness(txEndTime - txStartTime);
                            }
                            transactionProxy.commitTrans(pixelsTransContext);
                            if(startWriteLatencyTimer != null)
                            {
                                startWriteLatencyTimer.observeDuration();
                            }
                        }
                    }
            );
        } catch (SinkException e)
        {
            throw new RuntimeException(e);
        } finally
        {
            writeLock.unlock();
        }
    }

    protected RetinaProto.TableUpdateData.Builder buildTableUpdateDataFromBatch(TransContext transContext, List<RowChangeEvent> smallBatch)
    {
        RowChangeEvent event1 = smallBatch.get(0);
        RetinaProto.TableUpdateData.Builder builder = RetinaProto.TableUpdateData.newBuilder()
                .setTimestamp(transContext.getTimestamp())
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
}
