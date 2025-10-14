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
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final ReentrantLock writeLock = new ReentrantLock();
    private final int bucketId;

    public TableCrossTxWriter(String t, int bucketId)
    {
        super(t);
        flushBatchSize = config.getFlushBatchSize();
        this.bucketId = bucketId;
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush()
    {
        List<RowChangeEvent> batch;
        lock.lock();
        try
        {
            if (buffer.isEmpty())
            {
                return;
            }
            // Swap buffers quickly under lock
            batch = buffer;
            buffer = new LinkedList<>();
        } finally
        {
            lock.unlock();
        }

        writeLock.lock();
        try
        {
            String txId = null;
            String schemaName = null;
            List<RowChangeEvent> smallBatch = null;
            List<String> txIds = new ArrayList<>();
            List<String> fullTableName = new ArrayList<>();
            List<RetinaProto.TableUpdateData> tableUpdateData = new LinkedList<>();
            List<Integer> tableUpdateCount = new ArrayList<>();
            for (RowChangeEvent event : batch)
            {
                String currTxId = event.getTransaction().getId();
                if (!currTxId.equals(txId))
                {
                    if (smallBatch != null && !smallBatch.isEmpty())
                    {
                        tableUpdateData.add(buildTableUpdateDataFromBatch(txId, smallBatch).setBucket(bucketId).build());
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
                tableUpdateData.add(buildTableUpdateDataFromBatch(txId, smallBatch).setBucket(bucketId).build());
                tableUpdateCount.add(smallBatch.size());
            }

            CompletableFuture<RetinaProto.UpdateRecordResponse> updateRecordResponseCompletableFuture = delegate.writeBatchAsync(batch.get(0).getSchemaName(), tableUpdateData);

            updateRecordResponseCompletableFuture.thenAccept(
                    resp ->
                    {
                        updateCtxCounters(txIds, fullTableName, tableUpdateCount);
                    }
            );
        } finally
        {
            writeLock.unlock();
        }
    }

    private void updateCtxCounters(List<String> txIds, List<String> fullTableName, List<Integer> tableUpdateCount)
    {
        for (int i = 0; i < txIds.size(); i++)
        {
            metricsFacade.recordRowEvent(tableUpdateCount.get(i));
            String writeTxId = txIds.get(i);
            SinkContext sinkContext = SinkContextManager.getInstance().getSinkContext(writeTxId);
            sinkContext.updateCounter(fullTableName.get(i), tableUpdateCount.get(i));
        }
    }

    private RetinaProto.TableUpdateData.Builder buildTableUpdateDataFromBatch(String txId, List<RowChangeEvent> smallBatch)
    {
        SinkContext sinkContext = SinkContextManager.getInstance().getSinkContext(txId);
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
