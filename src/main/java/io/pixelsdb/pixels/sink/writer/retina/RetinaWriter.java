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

import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.prometheus.client.Summary;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class RetinaWriter implements PixelsSinkWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaWriter.class);
    final ExecutorService dispatchExecutor = Executors.newCachedThreadPool();
    private final ExecutorService transactionExecutor = Executors.newFixedThreadPool(1024);
    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor();
    private final TransactionProxy transactionProxy = TransactionProxy.Instance();
    private final TransService transService;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();

    private final SinkContextManager sinkContextManager;

    public RetinaWriter()
    {
        transService = TransService.Instance();
        this.sinkContextManager = SinkContextManager.getInstance();
    }

    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata txMeta)
    {
        try
        {
            if (txMeta.getStatus() == SinkProto.TransactionStatus.BEGIN)
            {
                handleTxBegin(txMeta);
            } else if (txMeta.getStatus() == SinkProto.TransactionStatus.END)
            {
                handleTxEnd(txMeta);
            }
        } catch (SinkException e)
        {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    @Override
    public boolean writeRow(RowChangeEvent event)
    {
        try
        {
            if (event == null)
            {
                return false;
            }

            metricsFacade.recordRowChange(event.getTable(), event.getOp());
            event.startLatencyTimer();
            if (event.getTransaction() == null || event.getTransaction().getId().isEmpty())
            {
                handleNonTxEvent(event);
                return true;
            }


            String table = event.getFullTableName();

            long collectionOrder = event.getTransaction().getDataCollectionOrder();
            long totalOrder = event.getTransaction().getTotalOrder();

            AtomicBoolean canWrite = new AtomicBoolean(false);
            SinkContext ctx = sinkContextManager.getActiveTxContext(event, canWrite);

            if (canWrite.get())
            {
                sinkContextManager.writeRowChangeEvent(ctx, event);
            }
        } catch (SinkException e)
        {
            LOGGER.error(e.getMessage(), e);
            return false;
        }

        return true;
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
        sinkContextManager.startTransSync(sourceTxId);
    }

    private void handleTxEnd(SinkProto.TransactionMetadata txEnd)
    {
        String txId = txEnd.getId();
        SinkContext ctx = sinkContextManager.getSinkContext(txId);

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
        LOGGER.trace("Begin to Commit transaction: {}, total event {}; Data Collection {}", txId, txEnd.getEventCount(),
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
            try
            {
                ctx.tableCounterLock.lock();
                while (!ctx.isCompleted(txEnd))
                {
                    LOGGER.debug("TX End Get Lock {}", txId);
                    LOGGER.debug("Waiting for events in TX {}: {}", txId,
                            txEnd.getDataCollectionsList().stream()
                                    .map(dc -> dc.getDataCollection() + "=" +
                                            ctx.tableCounters.getOrDefault(dc.getDataCollection(), 0L) +
                                            "/" + dc.getEventCount())
                                    .collect(Collectors.joining(", ")));
                    ctx.tableCounterCond.await();
                }
            } finally
            {
                ctx.tableCounterLock.unlock();
            }


            sinkContextManager.removeSinkContext(txId);
            boolean res = true;
            if (res)
            {
                LOGGER.trace("Committed transaction: {}", txId);
                Summary.Timer transLatencyTimer = metricsFacade.startTransLatencyTimer();
                transactionProxy.commitTransAsync(ctx.getPixelsTransCtx());
            } else
            {
                LOGGER.info("Abort transaction: {}", txId);
                Summary.Timer transLatencyTimer = metricsFacade.startTransLatencyTimer();
                CompletableFuture.runAsync(() ->
                {
                    try
                    {
                        transService.rollbackTrans(ctx.getPixelsTransCtx().getTransId(), false);
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
                transService.rollbackTrans(ctx.getPixelsTransCtx().getTransId(), false);
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


    private void handleNonTxEvent(RowChangeEvent event) throws SinkException
    {
        // virtual tx
        String randomId = Long.toString(System.currentTimeMillis()) + RandomUtils.nextLong();
        writeTrans(buildBeginTransactionMetadata(randomId));
        sinkContextManager.writeRowChangeEvent(randomId, event);
        writeTrans(buildEndTransactionMetadata(event.getFullTableName(), randomId));
    }

    public void shutdown()
    {
        dispatchExecutor.shutdown();
        timeoutScheduler.shutdown();
    }

    @Override
    public void close() throws IOException
    {

    }

    @Override
    public void flush()
    {

    }

    private SinkProto.TransactionMetadata buildBeginTransactionMetadata(String id)
    {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();
        builder.setStatus(SinkProto.TransactionStatus.BEGIN)
                .setId(id);
        return builder.build();
    }

    private SinkProto.TransactionMetadata buildEndTransactionMetadata(String fullTableName, String id)
    {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();
        builder.setStatus(SinkProto.TransactionStatus.END)
                .setId(id)
                .setEventCount(1L);

        SinkProto.DataCollection.Builder dataCollectionBuilder = SinkProto.DataCollection.newBuilder();
        dataCollectionBuilder.setDataCollection(fullTableName)
                .setEventCount(1L);
        builder.addDataCollections(dataCollectionBuilder);
        return builder.build();
    }
}
