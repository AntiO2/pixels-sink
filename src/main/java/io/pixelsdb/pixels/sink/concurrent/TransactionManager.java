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

import io.debezium.pipeline.txmetadata.TransactionContext;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.sink.processor.MetricsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * This class if for
 *
 * @author AntiO2
 */
public class TransactionManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionManager.class);
    private final static TransactionManager instance = new TransactionManager();
    private final TransService transService;
    private final Queue<TransContext> transContextQueue;
    private final Object batchLock = new Object();
    private final ExecutorService batchCommitExecutor;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final BlockingQueue<TransContext> toCommitTransContextQueue;

    private static final int BATCH_SIZE = 100;
    private static final int WORKER_COUNT = 16;
    private static final int MAX_WAIT_MS = 100;


    TransactionManager()
    {
        this.transService = TransService.Instance();
        this.transContextQueue = new ConcurrentLinkedDeque<>();
        this.toCommitTransContextQueue = new LinkedBlockingQueue<>();
        this.batchCommitExecutor = Executors.newFixedThreadPool(
                WORKER_COUNT,
                r -> {
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
    }

    public static TransactionManager Instance()
    {
        return instance;
    }

    private void requestTransactions()
    {
        try
        {
            List<TransContext> newContexts = transService.beginTransBatch(1000, false);
            transContextQueue.addAll(newContexts);
        } catch (TransException e)
        {
            throw new RuntimeException("Batch request failed", e);
        }
    }

    public TransContext getTransContext()
    {
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

    public void commitTransAsync(TransContext transContext)
    {
        toCommitTransContextQueue.add(transContext);
    }

    private void batchCommitWorker()
    {
        List<Long> batchTransIds = new ArrayList<>(BATCH_SIZE);
        List<TransContext> batchContexts = new ArrayList<>(BATCH_SIZE);

        while (true)
        {
            try
            {
                batchContexts.clear();
                batchTransIds.clear();

                TransContext first = toCommitTransContextQueue.take();
                batchContexts.add(first);
                batchTransIds.add(first.getTransId());

                long startTime = System.nanoTime();

                while (batchContexts.size() < BATCH_SIZE)
                {
                    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
                    long remainingMs = MAX_WAIT_MS - elapsedMs;
                    if (remainingMs <= 0)
                    {
                        break;
                    }

                    TransContext ctx = toCommitTransContextQueue.poll(remainingMs, TimeUnit.MILLISECONDS);
                    if (ctx == null)
                    {
                        break;
                    }
                    batchContexts.add(ctx);
                    batchTransIds.add(ctx.getTransId());
                }

                transService.commitTransBatch(batchTransIds, false);
                metricsFacade.recordTransaction(batchTransIds.size());

                if (LOGGER.isTraceEnabled())
                {
                    LOGGER.trace("[{}] Batch committed {} transactions ({} waited ms)",
                            Thread.currentThread().getName(),
                            batchTransIds.size(),
                            (System.nanoTime() - startTime) / 1_000_000);
                }
            }
            catch (InterruptedException ie)
            {
                LOGGER.warn("Batch commit worker interrupted, exiting...");
                Thread.currentThread().interrupt();
                break;
            }
            catch (TransException e)
            {
                LOGGER.error("Batch commit failed: {}", e.getMessage(), e);
            }
            catch (Exception e)
            {
                LOGGER.error("Unexpected error in batch commit worker", e);
            }
        }
    }

    public void close()
    {
        synchronized (batchLock)
        {
            while(true)
            {
                TransContext ctx = transContextQueue.poll();
                if (ctx == null)
                {
                    break;
                }
                try
                {
                    transService.rollbackTrans(ctx.getTransId(),false);
                } catch (TransException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
