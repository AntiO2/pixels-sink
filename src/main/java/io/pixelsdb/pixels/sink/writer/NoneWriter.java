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
 
package io.pixelsdb.pixels.sink.writer;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.util.TableCounters;
import io.pixelsdb.pixels.sink.writer.retina.SinkContext;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * NoneWriter implementation used for testing and metrics collection.
 * It tracks transaction completeness based on row counts provided in the TXEND metadata,
 * ensuring robust handling of out-of-order and concurrent TX BEGIN, TX END, and ROWChange events.
 */
public class NoneWriter implements PixelsSinkWriter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(NoneWriter.class);

    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();

    /**
     * Data structure to track transaction progress:
     * Map<TransId, TransactionContext>
     */
    private final Map<String, TransactionContext> transTracker = new ConcurrentHashMap<>();

    /**
     * Checks if all tables within a transaction have reached their expected row count.
     * If complete, the transaction is removed from the tracker and final metrics are recorded.
     *
     * @param transId The ID of the transaction to check.
     */
    private void checkAndCleanupTransaction(String transId)
    {
        TransactionContext context = transTracker.get(transId);

        if (context == null)
        {
            return;
        }

        boolean allComplete = context.sinkContext.isCompleted();
        int actualProcessedRows = context.sinkContext.getProcessedRowsNum();

        if (allComplete)
        {
            // All rows expected have been processed. Remove and record metrics.
            transTracker.remove(transId);
            LOGGER.trace("Transaction {} successfully completed and removed from tracker. Total rows: {}.", transId, actualProcessedRows);

            // Record final transaction metrics only upon completion
            metricsFacade.recordTransaction();
            metricsFacade.recordTransactionRowCount(actualProcessedRows);
        } else
        {
            // Not complete, keep tracking
            LOGGER.debug("Transaction {} is partially complete ({} rows processed). Keeping tracker entry.", transId, actualProcessedRows);
        }
    }

    @Override
    public void flush()
    {
        // No-op for NoneWriter
    }

    // --- Interface Methods ---

    @Override
    public boolean writeRow(RowChangeEvent rowChangeEvent)
    {
            metricsFacade.recordRowEvent();
            metricsFacade.recordRowChange(rowChangeEvent.getTable(), rowChangeEvent.getOp());
            try
            {
                rowChangeEvent.initIndexKey();
                metricsFacade.recordPrimaryKeyUpdateDistribution(rowChangeEvent.getTable(), rowChangeEvent.getAfterKey().getKey());

                // Get transaction ID and table name
                String transId = rowChangeEvent.getTransaction().getId();
                String fullTable = rowChangeEvent.getFullTableName();

                // 1. Get or create the transaction context
                TransactionContext context = transTracker.computeIfAbsent(transId, k -> new TransactionContext(transId));

                context.sinkContext.getTableCounterLock().lock();
                context.incrementEndCount(fullTable);
                checkAndCleanupTransaction(transId);
                context.sinkContext.getTableCounterLock().unlock();
            } catch (SinkException e)
            {
                throw new RuntimeException("Error processing row key or metrics.", e);
            }
            return true;
    }

    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata)
    {
            String transId = transactionMetadata.getId();

            if (transactionMetadata.getStatus() == SinkProto.TransactionStatus.BEGIN)
            {
                // 1. BEGIN: Create context if not exists (in case ROWChange arrived first).
                transTracker.computeIfAbsent(transId, k -> new TransactionContext(transId));
                LOGGER.debug("Transaction {} BEGIN received.", transId);

            } else if (transactionMetadata.getStatus() == SinkProto.TransactionStatus.END)
            {
                // 2. END: Finalize tracker state, merge pre-counts, and trigger cleanup.

                // Get existing context or create a new one (in case BEGIN was missed).
                TransactionContext context = transTracker.computeIfAbsent(transId, k -> new TransactionContext(transId));
                context.sinkContext.getTableCounterLock().lock();
                context.sinkContext.setEndTx(transactionMetadata);
                checkAndCleanupTransaction(transId);
                context.sinkContext.getTableCounterLock().unlock();
            }
            return true;
    }

    @Override
    public void close() throws IOException
    {
        // No-op for NoneWriter
        LOGGER.info("Remaining unfinished transactions on close: {}", transTracker.size());

        // Log details of transactions that were never completed
        if (!transTracker.isEmpty())
        {
            transTracker.forEach((transId, context) ->
            {
                LOGGER.warn("Unfinished transaction {}",transId);
            });
        }
    }

    /**
     * Helper class to manage the state of a single transaction, decoupling the row accumulation
     * from the final TableCounters initialization (which requires total counts from TX END).
     */
    public static class TransactionContext
    {
        // Key: Full Table Name, Value: Row Count
        private SinkContext sinkContext = null;


        TransactionContext(String txId)
        {
            this.sinkContext = new SinkContext(txId);
        }


        /**
         * @param table Full table name
         */
        public void incrementEndCount(String table)
        {
            sinkContext.updateCounter(table, 1);
        }
    }
}