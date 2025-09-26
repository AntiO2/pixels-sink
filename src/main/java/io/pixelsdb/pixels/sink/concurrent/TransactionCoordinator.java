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

package io.pixelsdb.pixels.sink.concurrent;

import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.processor.MetricsFacade;
import io.pixelsdb.pixels.sink.sink.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.sink.PixelsSinkWriterFactory;
import io.pixelsdb.pixels.sink.sink.TableWriter;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class TransactionCoordinator
{
    public static final int INITIAL_CAPACITY = 11;
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionCoordinator.class);
    private static final int MAX_ACTIVE_TX = 1000;
    final ConcurrentMap<String, SinkContext> activeTxContexts = new ConcurrentHashMap<>();
    final ExecutorService dispatchExecutor = Executors.newCachedThreadPool();
    private final PixelsSinkWriter writer;
    private final ExecutorService transactionExecutor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor();
    private final TransactionManager transactionManager = TransactionManager.Instance();
    private final TransService transService;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
    // private final BlockingQueue<RowChangeEvent> nonTxQueue = new LinkedBlockingQueue<>();
    private long TX_TIMEOUT_MS = PixelsSinkConfigFactory.getInstance().getTransactionTimeout();

    TransactionCoordinator()
    {
        try
        {
            this.writer = PixelsSinkWriterFactory.getWriter();
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        transService = TransService.Instance();
        // startDispatchWorker();
        startTimeoutChecker();
    }

    public void processTransactionEvent(SinkProto.TransactionMetadata txMeta) throws SinkException
    {
        if (txMeta.getStatus() == SinkProto.TransactionStatus.BEGIN)
        {
            handleTxBegin(txMeta);
        } else if (txMeta.getStatus() == SinkProto.TransactionStatus.END)
        {
            handleTxEnd(txMeta);
            metricsFacade.recordTransaction();
        }
    }

    public void processRowEvent(RowChangeEvent event) throws SinkException
    {
        if (event == null)
        {
            return;
        }

        metricsFacade.recordRowChange(event.getTable(), event.getOp());
        event.startLatencyTimer();
        if (event.getTransaction() == null || event.getTransaction().getId().isEmpty())
        {
            handleNonTxEvent(event);
            return;
        }

        String txId = event.getTransaction().getId();
        String table = event.getFullTableName();

        long collectionOrder = event.getTransaction().getDataCollectionOrder();
        long totalOrder = event.getTransaction().getTotalOrder();

        LOGGER.debug("Receive event {} {}/{} {}/{} ", event.getOp().toString(), txId, totalOrder, table, collectionOrder);
        AtomicBoolean canWrite = new AtomicBoolean(false);
        SinkContext ctx = activeTxContexts.compute(txId, (sourceTxId, sinkContext) ->
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
        if(canWrite.get())
        {
            processRowChangeEvent(ctx, event);
        }
    }

    private void handleTxBegin(SinkProto.TransactionMetadata txBegin) throws SinkException
    {
        // startTrans(txBegin.getId()).get();
        try
        {
            startTransSync(txBegin.getId());
        } catch (SinkException e)
        {
            throw new SinkException("Failed to start trans", e);
        }

    }

    private void startTransSync(String sourceTxId) throws SinkException
    {
        activeTxContexts.compute(
                sourceTxId,
                (k, oldCtx) ->
                {
                    if (oldCtx == null)
                    {
                        return new SinkContext(sourceTxId, transactionManager.getTransContext());
                    } else
                    {
                        oldCtx.getLock().lock();
                        try
                        {
                            oldCtx.setPixelsTransCtx(transactionManager.getTransContext());
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
        LOGGER.info("Begin Tx Sync: {}", sourceTxId);
    }

    private void handleOrphanEvents(SinkContext ctx) throws SinkException
    {
        Queue<RowChangeEvent> buffered = ctx.getOrphanEvent();

        if (buffered != null)
        {
            for (RowChangeEvent event : buffered)
            {
                processRowChangeEvent(ctx, event);
            }
        }
    }

    private void handleTxEnd(SinkProto.TransactionMetadata txEnd)
    {
        String txId = txEnd.getId();
        SinkContext ctx = getSinkContext(txId);

        transactionExecutor.submit(() ->
                {
                    processTxCommit(txEnd, txId, ctx);
                }
        );
        switch (pixelsSinkConfig.getTransactionMode())
        {

//            case BATCH ->
//            {
//                processTxCommit(txEnd, txId, ctx);
//            }
//            case RECORD ->
//            {
//                transactionExecutor.submit(() ->
//                        {
//                            processTxCommit(txEnd, txId, ctx);
//                        }
//                );
//            }
        }
    }

    private void processTxCommit(SinkProto.TransactionMetadata txEnd, String txId, SinkContext ctx)
    {
        LOGGER.info("Begin to Commit transaction: {}, total event {}; Data Collection {}", txId, txEnd.getEventCount(),
                txEnd.getDataCollectionsList().stream()
                        .map(dc -> dc.getDataCollection() + "=" +
                                ctx.tableCursors.getOrDefault(dc.getDataCollection(), 0L) +
                                "/" + dc.getEventCount())
                        .collect(Collectors.joining(", ")));
        if (ctx == null)
        {
            LOGGER.warn("Sink Context is null");
            return;
        }

        try
        {
            while (!ctx.isCompleted(txEnd))
            {
                LOGGER.debug("TX End Get Lock {}", txId);
                LOGGER.debug("Waiting for events in TX {}: {}", txId,
                        txEnd.getDataCollectionsList().stream()
                                .map(dc -> dc.getDataCollection() + "=" +
                                        ctx.tableCounters.getOrDefault(dc.getDataCollection(), 0L) +
                                        "/" + dc.getEventCount())
                                .collect(Collectors.joining(", ")));
//                ctx.cond.await(100, TimeUnit.MILLISECONDS);
                Thread.sleep(100);
            }

            activeTxContexts.remove(txId);
            boolean res = true;
            if (res)
            {
                LOGGER.info("Committed transaction: {}", txId);
                Summary.Timer transLatencyTimer = metricsFacade.startTransLatencyTimer();
                transactionManager.commitTransAsync(ctx.getPixelsTransCtx());
            } else
            {
                LOGGER.info("Abort transaction: {}", txId);
                Summary.Timer transLatencyTimer = metricsFacade.startTransLatencyTimer();
                CompletableFuture.runAsync(() ->
                {
                    try
                    {
                        transService.rollbackTrans(ctx.getPixelsTransCtx().getTransId());
                    } catch (TransException e)
                    {
                        throw new RuntimeException(e);
                    }
                }).whenComplete((v, ex) ->
                {
                    transLatencyTimer.close();
                    if (ex != null)
                    {
                        LOGGER.error("Rollback failed", ex);
                    }
                });
            }
        } catch (InterruptedException e)
        {
            try
            {
                LOGGER.info("Catch Exception, Abort transaction: {}", txId);
                transService.rollbackTrans(ctx.getPixelsTransCtx().getTransId());
            } catch (TransException ex)
            {
                LOGGER.error("Failed to abort transaction {}", txId);
                ex.printStackTrace();
                LOGGER.error(ex.getMessage());
                throw new RuntimeException(ex);
            }
            LOGGER.error(e.getMessage());
            LOGGER.error("Failed to commit transaction {}", txId, e);
        }
    }

    private void processRowChangeEvent(SinkContext ctx, RowChangeEvent event) throws SinkException
    {
        String table = event.getTable();
        event.setTimeStamp(ctx.getTimestamp());
        event.initIndexKey();
        switch (pixelsSinkConfig.getTransactionMode())
        {
            case BATCH ->
            {
                TableWriter.getTableWriter(table).write(event, ctx);
            }
            case RECORD ->
            {
                dispatchImmediately(event, ctx);
            }
        }
    }

    public SinkContext getSinkContext(String txId)
    {
        return activeTxContexts.get(txId);
    }

    protected void dispatchImmediately(RowChangeEvent event, SinkContext ctx)
    {
        dispatchExecutor.execute(() ->
        {
            try
            {
                LOGGER.debug("Dispatching [{}] {}.{} (Order: {}/{}) TX: {}",
                        event.getOp().name(),
                        event.getDb(),
                        event.getTable(),
                        event.getTransaction() != null ?
                                event.getTransaction().getDataCollectionOrder() : "N/A",
                        event.getTransaction() != null ?
                                event.getTransaction().getTotalOrder() : "N/A",
                        event.getTransaction().getId());
                Summary.Timer writeLatencyTimer = metricsFacade.startWriteLatencyTimer();
                boolean success = writer.write(event);
                writeLatencyTimer.close();
                if (success)
                {
                    metricsFacade.recordTotalLatency(event);
                    metricsFacade.recordRowChange(event.getTable(), event.getOp());
                    event.endLatencyTimer();
                } else
                {
                    // TODO retry?
                }
            } finally
            {
                if (ctx != null)
                {
                    ctx.updateCounter(event.getFullTableName());
                    if (ctx.pendingEvents.decrementAndGet() == 0 && ctx.completed)
                    {
                        ctx.completionFuture.complete(null);
                    }
                }
            }
        });
    }

    private void startTimeoutChecker()
    {
        timeoutScheduler.scheduleAtFixedRate(() ->
        {
            activeTxContexts.entrySet().removeIf(entry ->
            {
                SinkContext ctx = entry.getValue();
                if (ctx.isExpired())
                {
                    LOGGER.warn("Transaction timeout: {}", entry.getKey());
                    return true;
                }
                return false;
            });
        }, 10, 10, TimeUnit.SECONDS);
    }

    private void handleNonTxEvent(RowChangeEvent event) throws SinkException
    {
        switch (pixelsSinkConfig.getTransactionMode())
        {
            case BATCH ->
            {
                SinkContext sinkContext = new SinkContext("-1");
                TransContext transContext = transactionManager.getTransContext();
                sinkContext.setPixelsTransCtx(transContext);
                RetinaProto.TableUpdateData.Builder builder = RetinaProto.TableUpdateData.newBuilder();
                TableWriter.addUpdateData(event, builder);
                List<RetinaProto.TableUpdateData> tableUpdateDataList = List.of(builder.build());
                writer.writeTrans(pixelsSinkConfig.getCaptureDatabase(), tableUpdateDataList, transContext.getTimestamp());
                transactionManager.commitTransAsync(transContext);
            }
            case RECORD ->
            {
                dispatchImmediately(event, null);
            }
        }
    }

    public void shutdown()
    {
        dispatchExecutor.shutdown();
        timeoutScheduler.shutdown();
    }

    public void setTxTimeoutMs(long txTimeoutMs)
    {
        TX_TIMEOUT_MS = txTimeoutMs;
    }

    private static class OrderedEvent
    {
        final RowChangeEvent event;
        final String table;
        final long collectionOrder;
        final long totalOrder;

        OrderedEvent(RowChangeEvent event, long collectionOrder, long totalOrder)
        {
            this.event = event;
            this.table = event.getFullTableName();
            this.collectionOrder = collectionOrder;
            this.totalOrder = totalOrder;
        }

        String getTable()
        {
            return table;
        }

        long getCollectionOrder()
        {
            return collectionOrder;
        }
    }

    @Deprecated
    private static class BufferedEvent
    {
        final RowChangeEvent event;
        final long collectionOrder;
        final long totalOrder;

        BufferedEvent(RowChangeEvent event, long collectionOrder, long totalOrder)
        {
            this.event = event;
            this.collectionOrder = collectionOrder;
            this.totalOrder = totalOrder;
        }

        long getTotalOrder()
        {
            return totalOrder;
        }
    }

}
