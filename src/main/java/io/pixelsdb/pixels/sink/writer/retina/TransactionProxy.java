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

import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class if for pixels trans service
 *
 * @author AntiO2
 */
public class TransactionProxy
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionProxy.class);
    private static volatile TransactionProxy instance;
    private final TransService transService;
    private final Queue<TransContext> transContextQueue;
    private final Object batchLock = new Object();
    private final ExecutorService batchCommitExecutor;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final BlockingQueue<SinkContext> toCommitTransContextQueue;
    private final String freshnessLevel;
    private final int BATCH_SIZE;
    private final int REQUEST_BATCH_SIZE;
    private final boolean REQUEST_BATCH;
    private final int WORKER_COUNT;
    private final int MAX_WAIT_MS;

    private AtomicInteger beginCount = new AtomicInteger(0);
    private AtomicInteger commitCount = new AtomicInteger(0);

    private TransactionProxy()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        BATCH_SIZE = pixelsSinkConfig.getCommitBatchSize();
        WORKER_COUNT = pixelsSinkConfig.getCommitBatchWorkers();
        MAX_WAIT_MS = pixelsSinkConfig.getCommitBatchDelay();

        REQUEST_BATCH_SIZE = pixelsSinkConfig.getRetinaTransRequestBatchSize();
        REQUEST_BATCH = pixelsSinkConfig.isRetinaTransRequestBatch();
        this.transService = TransService.Instance();
        this.transContextQueue = new ConcurrentLinkedDeque<>();
        this.toCommitTransContextQueue = new LinkedBlockingQueue<>();
        this.batchCommitExecutor = Executors.newFixedThreadPool(
                WORKER_COUNT,
                r ->
                {
                    Thread t = new Thread(r);
                    t.setName("commit-trans-batch-thread");
                    t.setDaemon(true);
                    return t;
                }
        );
        for (int i = 0; i < WORKER_COUNT; i++)
        {
            batchCommitExecutor.submit(this::batchCommitWorker);
        }

        this.freshnessLevel = pixelsSinkConfig.getSinkMonitorFreshnessLevel();
    }

    public static TransactionProxy Instance()
    {
        if (instance == null)
        {
            synchronized (TransactionProxy.class)
            {
                if (instance == null)
                {
                    instance = new TransactionProxy();
                }
            }
        }
        return instance;
    }

    public static void staticClose()
    {
        if (instance != null)
        {
            instance.close();
        }
    }

    private void requestTransactions()
    {
        try
        {
            List<TransContext> newContexts = transService.beginTransBatch(REQUEST_BATCH_SIZE, false);
            transContextQueue.addAll(newContexts);
        } catch (TransException e)
        {
            throw new RuntimeException("Batch request failed", e);
        }
    }

    @Deprecated
    public TransContext getNewTransContext()
    {
        return getNewTransContext("None");
    }

    public TransContext getNewTransContext(String txId)
    {
        beginCount.incrementAndGet();
        if (!REQUEST_BATCH)
        {
            try
            {
                TransContext transContext = transService.beginTrans(false);
                LOGGER.trace("{} begin {}", txId, transContext.getTransId());
                return transContext;
            } catch (TransException e)
            {
                return null;
            }
        }

        TransContext ctx = transContextQueue.poll();
        if (ctx != null)
        {
            return ctx;
        }
        synchronized (batchLock)
        {
            ctx = transContextQueue.poll();
            if (ctx == null)
            {
                requestTransactions();
                ctx = transContextQueue.poll();
                if (ctx == null)
                {
                    throw new IllegalStateException("No contexts available");
                }
            }
            return ctx;
        }
    }

    public void commitTransAsync(SinkContext transContext)
    {
        toCommitTransContextQueue.add(transContext);
    }

    public void commitTransSync(SinkContext transContext)
    {
        commitTrans(transContext.getPixelsTransCtx());
        metricsFacade.recordTransaction();
        long txEndTime = System.currentTimeMillis();

        if (freshnessLevel.equals("txn"))
        {
            metricsFacade.recordFreshness(txEndTime - transContext.getStartTime());
        }
    }

    public void commitTrans(TransContext ctx)
    {
        commitCount.incrementAndGet();
        try
        {
            transService.commitTrans(ctx.getTransId(), false);
        } catch (TransException e)
        {
            LOGGER.error("Batch commit failed: {}", e.getMessage(), e);
        }
    }

    public void rollbackTrans(TransContext ctx)
    {
        try
        {
            transService.rollbackTrans(ctx.getTransId(), false);
        } catch (TransException e)
        {
            LOGGER.error("Rollback transaction failed: {}", e.getMessage(), e);
        }
    }

    private void batchCommitWorker()
    {
        List<Long> batchTransIds = new ArrayList<>(BATCH_SIZE);
        List<TransContext> batchContexts = new ArrayList<>(BATCH_SIZE);
        List<Long> txStartTimes = new ArrayList<>(BATCH_SIZE);
        while (true)
        {
            try
            {
                batchContexts.clear();
                batchTransIds.clear();
                txStartTimes.clear();

                SinkContext firstSinkContext = toCommitTransContextQueue.take();
                TransContext transContext = firstSinkContext.getPixelsTransCtx();
                batchContexts.add(transContext);
                batchTransIds.add(transContext.getTransId());
                txStartTimes.add(firstSinkContext.getStartTime());
                long startTime = System.nanoTime();

                while (batchContexts.size() < BATCH_SIZE)
                {
                    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
                    long remainingMs = MAX_WAIT_MS - elapsedMs;
                    if (remainingMs <= 0)
                    {
                        break;
                    }

                    SinkContext ctx = toCommitTransContextQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
                    if (ctx == null)
                    {
                        break;
                    }
                    transContext = ctx.getPixelsTransCtx();
                    batchContexts.add(transContext);
                    batchTransIds.add(transContext.getTransId());
                    txStartTimes.add(ctx.getStartTime());
                }

                transService.commitTransBatch(batchTransIds, false);
                metricsFacade.recordTransaction(batchTransIds.size());
                long txEndTime = System.currentTimeMillis();

                if (freshnessLevel.equals("txn"))
                {
                    txStartTimes.forEach(
                            txStartTime ->
                            {
                                metricsFacade.recordFreshness(txEndTime - txStartTime);
                            }
                    );
                }
                if (LOGGER.isTraceEnabled())
                {
                    LOGGER.trace("[{}] Batch committed {} transactions ({} waited ms)",
                            Thread.currentThread().getName(),
                            batchTransIds.size(),
                            (System.nanoTime() - startTime) / 1_000_000);
                }
            } catch (InterruptedException ie)
            {
                LOGGER.warn("Batch commit worker interrupted, exiting...");
                Thread.currentThread().interrupt();
                break;
            } catch (TransException e)
            {
                LOGGER.error("Batch commit failed: {}", e.getMessage(), e);
            } catch (Exception e)
            {
                LOGGER.error("Unexpected error in batch commit worker", e);
            }
        }
    }

    public void close()
    {
        synchronized (batchLock)
        {
            while (true)
            {
                TransContext ctx = transContextQueue.poll();
                if (ctx == null)
                {
                    break;
                }
                try
                {
                    transService.rollbackTrans(ctx.getTransId(), false);
                } catch (TransException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
