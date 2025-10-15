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

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class SinkContextManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkContextManager.class);
    private static final Logger BUCKET_TRACE_LOGGER = LoggerFactory.getLogger("bucket_trace");

    private final static SinkContextManager INSTANCE = new SinkContextManager();

    private final ConcurrentMap<String, SinkContext> activeTxContexts = new ConcurrentHashMap<>();

    private final TransactionProxy transactionProxy = TransactionProxy.Instance();
    private final TableWriterProxy tableWriterProxy;

    private SinkContextManager()
    {
        this.tableWriterProxy = TableWriterProxy.getInstance();
    }

    public static SinkContextManager getInstance()
    {
        return INSTANCE;
    }

    protected SinkContext getActiveTxContext(RowChangeEvent event, AtomicBoolean canWrite)
    {
        String txId = event.getTransaction().getId();
        return activeTxContexts.compute(txId, (sourceTxId, sinkContext) ->
        {
            if (sinkContext == null)
            {
                SinkContext newSinkContext = new SinkContext(sourceTxId);
                newSinkContext.bufferOrphanedEvent(event);
                return newSinkContext;
            } else
            {
                if (sinkContext.getPixelsTransCtx() == null)
                {
                    sinkContext.bufferOrphanedEvent(event);
                    return sinkContext;
                }
                canWrite.set(true);
                return sinkContext;
            }
        });
    }

    protected void startTransSync(String sourceTxId)
    {
        activeTxContexts.compute(
                sourceTxId,
                (k, oldCtx) ->
                {
                    if (oldCtx == null)
                    {
                        return new SinkContext(sourceTxId, transactionProxy.getNewTransContext());
                    } else
                    {
                        oldCtx.getLock().lock();
                        try
                        {
                            oldCtx.setPixelsTransCtx(transactionProxy.getNewTransContext());
                            handleOrphanEvents(oldCtx);
                            oldCtx.getCond().signalAll();
                        } catch (SinkException e)
                        {
                            throw new RuntimeException(e);
                        } finally
                        {
                            oldCtx.getLock().unlock();
                        }
                        return oldCtx;
                    }
                }
        );
        LOGGER.trace("Begin Tx Sync: {}", sourceTxId);
    }

    private void handleOrphanEvents(SinkContext ctx) throws SinkException
    {
        Queue<RowChangeEvent> buffered = ctx.getOrphanEvent();

        if (buffered != null)
        {
            for (RowChangeEvent event : buffered)
            {
                writeRowChangeEvent(ctx, event);
            }
        }
    }

    protected void writeRowChangeEvent(SinkContext ctx, RowChangeEvent event) throws SinkException
    {
        event.initIndexKey();
        switch (event.getOp())
        {
            case UPDATE ->
            {
                if(!event.isPkChanged())
                {
                    writeBeforeEvent(ctx, event);
                } else
                {
                    TypeDescription typeDescription = event.getSchema();
                    ctx.updateCounter(event.getFullTableName(), -1L);

                    SinkProto.RowRecord.Builder deleteBuilder = event.getRowRecord().toBuilder()
                            .clearAfter().setOp(SinkProto.OperationType.DELETE);
                    RowChangeEvent deleteEvent = new RowChangeEvent(deleteBuilder.build(), typeDescription);
                    deleteEvent.initIndexKey();
                    writeBeforeEvent(ctx, deleteEvent);

                    SinkProto.RowRecord.Builder insertBuilder = event.getRowRecord().toBuilder()
                            .clearBefore().setOp(SinkProto.OperationType.INSERT);
                    RowChangeEvent insertEvent = new RowChangeEvent(insertBuilder.build(), typeDescription);
                    insertEvent.initIndexKey();
                    writeAfterEvent(ctx, deleteEvent);
                }
            }
            case DELETE ->
            {
                writeBeforeEvent(ctx, event);
            }
            case INSERT, SNAPSHOT ->
            {
                writeAfterEvent(ctx, event);
            }
            case UNRECOGNIZED ->
            {
                return;
            }
        }
    }

    private boolean writeBeforeEvent(SinkContext ctx, RowChangeEvent event)
    {
        String table = event.getTable();
        event.setTimeStamp(ctx.getTimestamp());
        long tableId = event.getTableId();
        int beforeBucketFromIndex = event.getBeforeBucketFromIndex();
        return tableWriterProxy.getTableWriter(table, tableId, beforeBucketFromIndex).write(event, ctx);
    }

    private boolean writeAfterEvent(SinkContext ctx, RowChangeEvent event)
    {
        String table = event.getTable();
        event.setTimeStamp(ctx.getTimestamp());
        long tableId = event.getTableId();
        int afterBucketFromIndex = event.getAfterBucketFromIndex();
        return tableWriterProxy.getTableWriter(table, tableId, afterBucketFromIndex).write(event, ctx);
    }


    protected SinkContext getSinkContext(String txId)
    {
        return activeTxContexts.get(txId);
    }

    protected void removeSinkContext(String txId)
    {
        activeTxContexts.remove(txId);
    }

    protected void writeRowChangeEvent(String randomId, RowChangeEvent event) throws SinkException
    {
        writeRowChangeEvent(getSinkContext(randomId), event);
    }
}
