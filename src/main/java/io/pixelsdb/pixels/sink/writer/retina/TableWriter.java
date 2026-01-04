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
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiter;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @package: io.pixelsdb.pixels.sink.writer.retina
 * @className: TableWriter
 * @author: AntiO2
 * @date: 2025/9/27 09:58
 */
public abstract class TableWriter {

    protected final RetinaServiceProxy delegate; // physical writer
    protected final ReentrantLock bufferLock = new ReentrantLock();
    protected final Condition flushCondition = bufferLock.newCondition();
    protected final Thread flusherThread;
    protected final String tableName;
    protected final long flushInterval;
    protected final SinkContextManager sinkContextManager;
    protected final String freshnessLevel;
    protected final boolean freshness_embed;
    private final ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService logScheduler = Executors.newScheduledThreadPool(1);
    private final AtomicInteger counter = new AtomicInteger();
    protected volatile boolean running = true;
    // Shared state (protected by lock)
    protected List<RowChangeEvent> buffer = new LinkedList<>();
    protected volatile String currentTxId = null;
    protected String txId = null;
    protected String fullTableName;
    protected PixelsSinkConfig config;
    protected MetricsFacade metricsFacade = MetricsFacade.getInstance();
    protected TransactionMode transactionMode;

    protected TableWriter(String tableName, int bucketId) {
        this.config = PixelsSinkConfigFactory.getInstance();
        this.tableName = tableName;
        this.flushInterval = config.getFlushIntervalMs();
        this.sinkContextManager = SinkContextManager.getInstance();
        this.freshnessLevel = config.getSinkMonitorFreshnessLevel();
        this.delegate = new RetinaServiceProxy(bucketId);
        this.transactionMode = config.getTransactionMode();
        String sinkMonitorFreshnessLevel = config.getSinkMonitorFreshnessLevel();
        if (sinkMonitorFreshnessLevel.equals("embed")) {
            freshness_embed = true;
        } else {
            freshness_embed = false;
        }
        if (this.config.isMonitorReportEnabled() && this.config.isRetinaLogQueueEnabled()) {
            long interval = this.config.getMonitorReportInterval();
            Runnable monitorTask = writerInfoTask(tableName);
            logScheduler.scheduleAtFixedRate(
                    monitorTask,
                    0,
                    interval,
                    TimeUnit.MILLISECONDS
            );
        }
        this.flusherThread = new Thread(new FlusherRunnable(), "Pixels-Flusher-" + tableName);
        this.flusherThread.start();
    }

    /**
     * Helper: add insert/delete data into proto builder.
     */
    protected static void addUpdateData(RowChangeEvent rowChangeEvent,
                                        RetinaProto.TableUpdateData.Builder builder) throws SinkException {
        switch (rowChangeEvent.getOp()) {
            case SNAPSHOT, INSERT -> {
                RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
                insertDataBuilder.addIndexKeys(rowChangeEvent.getAfterKey());
                insertDataBuilder.addAllColValues(rowChangeEvent.getAfterData());
                builder.addInsertData(insertDataBuilder);
            }
            case UPDATE -> {
                RetinaProto.UpdateData.Builder updateDataBuilder = RetinaProto.UpdateData.newBuilder();
                updateDataBuilder.addIndexKeys(rowChangeEvent.getAfterKey());
                updateDataBuilder.addAllColValues(rowChangeEvent.getAfterData());
                builder.addUpdateData(updateDataBuilder);
            }
            case DELETE -> {
                RetinaProto.DeleteData.Builder deleteDataBuilder = RetinaProto.DeleteData.newBuilder();
                deleteDataBuilder.addIndexKeys(rowChangeEvent.getBeforeKey());
                builder.addDeleteData(deleteDataBuilder);
            }
            case UNRECOGNIZED -> {
                throw new SinkException("Unrecognized op: " + rowChangeEvent.getOp());
            }
        }
    }

    private void submitFlushTask(List<RowChangeEvent> batch) {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        flushExecutor.submit(() ->
        {
            flush(batch);
        });
    }

    private Runnable writerInfoTask(String tableName) {
        final AtomicInteger reportId = new AtomicInteger();
        final AtomicInteger lastRunCounter = new AtomicInteger();
        Runnable monitorTask = () ->
        {
            String firstTx = "none";
            RowChangeEvent firstEvent = null;
            int len = 0;
            bufferLock.lock();
            len = buffer.size();
            if (!buffer.isEmpty()) {
                firstEvent = buffer.get(0);
            }
            bufferLock.unlock();
            if (firstEvent != null) {
                firstTx = firstEvent.getTransaction().getId();
                int count = counter.get();
                getLOGGER().info("{} Writer {}: Tx Now is {}. Buffer Len is {}. Total Count {}", reportId.incrementAndGet(), tableName, firstTx, len, count);
            }
        };
        return monitorTask;
    }

    protected abstract Logger getLOGGER();

    public boolean write(RowChangeEvent event, SinkContext ctx) {
        try {
            bufferLock.lock();
            try {
                if (!transactionMode.equals(TransactionMode.RECORD)) {
                    txId = ctx.getSourceTxId();
                }
                currentTxId = txId;
                if (fullTableName == null) {
                    fullTableName = event.getFullTableName();
                }
                counter.incrementAndGet();
                buffer.add(event);

                if (needFlush()) {
                    flushCondition.signalAll();
                }
            } finally {
                bufferLock.unlock();
            }
            return true;
        } catch (Exception e) {
            getLOGGER().error("Write failed for table {}", tableName, e);
            return false;
        }
    }

    public abstract void flush(List<RowChangeEvent> batchToFlush);

    protected abstract boolean needFlush();

    public void close() {
        this.running = false;
        if (this.flusherThread != null) {
            this.flusherThread.interrupt();
        }
        logScheduler.shutdown();
        try {
            logScheduler.awaitTermination(5, TimeUnit.SECONDS);
            flushExecutor.awaitTermination(5, TimeUnit.SECONDS);
            if (this.flusherThread != null) {
                this.flusherThread.join(5000);
            }
            delegate.close();
        } catch (InterruptedException ignored) {
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private class FlusherRunnable implements Runnable {
        @Override
        public void run() {
            while (running) {
                bufferLock.lock();
                try {
                    if (!needFlush()) {
                        try {
                            // Conditional wait: will wait until signaled by write() or timeout
                            flushCondition.await(flushInterval, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            // Exit loop if interrupted during shutdown
                            running = false;
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }

                    List<RowChangeEvent> batchToFlush = buffer;
                    buffer = new LinkedList<>();
                    bufferLock.unlock();
                    submitFlushTask(batchToFlush);
                    bufferLock.lock();
                } finally {
                    bufferLock.unlock();
                }
            }
        }
    }
}
