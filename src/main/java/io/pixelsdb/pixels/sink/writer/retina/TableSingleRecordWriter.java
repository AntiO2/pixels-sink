package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.pixelsdb.pixels.sink.util.DataTransform;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class TableSingleRecordWriter extends TableCrossTxWriter
{
    private final TransactionProxy transactionProxy;

    public TableSingleRecordWriter(String t, int bucketId)
    {
        super(t, bucketId);
        this.transactionProxy = TransactionProxy.Instance();
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush()
    {
        List<RowChangeEvent> batch;
        bufferLock.lock();
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
            bufferLock.unlock();
        }

        TransContext pixelsTransContext = transactionProxy.getNewTransContext();

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

            flushRateLimiter.acquire(batch.size());
            long txStartTime = System.currentTimeMillis();

//            if(freshnessLevel.equals("embed"))
            if (true)
            {
                long freshness_ts = txStartTime * 1000;
                FreshnessClient.getInstance().addMonitoredTable(tableName);
                DataTransform.updateTimeStamp(tableUpdateDataBuilderList, freshness_ts);
            }

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
