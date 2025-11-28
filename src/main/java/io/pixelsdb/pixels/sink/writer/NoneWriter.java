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

package io.pixelsdb.pixels.sink.writer;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.util.TableCounters;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
     * Helper class to manage the state of a single transaction, decoupling the row accumulation
     * from the final TableCounters initialization (which requires total counts from TX END).
     */
    public static class TransactionContext
    {
        @Getter
        private volatile boolean endReceived = false;

        // Key: Full Table Name
        private Map<String, TableCounters> tableCounters = null;

        // Key: Full Table Name, Value: Row Count
        private final Map<String, AtomicInteger> preEndCounts = new ConcurrentHashMap<>();

        public void setEndReceived(Map<String, TableCounters> counters) {
            this.tableCounters = counters;
            this.endReceived = true;
        }

        /**
         * @param table Full table name
         */
        public void incrementPreEndCount(String table) {
            preEndCounts.computeIfAbsent(table, k -> new AtomicInteger(0)).incrementAndGet();
        }
    }


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

        if (context == null || !context.isEndReceived())
        {
            // Transaction has not received TX END or has been cleaned up already.
            return;
        }

        Map<String, TableCounters> tableMap = context.tableCounters;
        if (tableMap == null || tableMap.isEmpty())
        {
            // Empty transaction with no tables. Clean up immediately.
            transTracker.remove(transId);
            metricsFacade.recordTransaction();
            metricsFacade.recordTransactionRowCount(0);
            LOGGER.info("Transaction {} (empty) successfully completed and removed from tracker.", transId);
            return;
        }

        boolean allComplete = true;
        int actualProcessedRows = 0;

        // Iterate through all tables to check completion status
        for (Map.Entry<String, TableCounters> entry : tableMap.entrySet())
        {
            TableCounters counters = entry.getValue();
            actualProcessedRows += counters.getCurrentCount();
            if (!counters.isComplete())
            {
                allComplete = false;
            }
        }

        if (allComplete)
        {
            // All rows expected have been processed. Remove and record metrics.
            transTracker.remove(transId);
            LOGGER.info("Transaction {} successfully completed and removed from tracker. Total rows: {}.", transId, actualProcessedRows);

            // Record final transaction metrics only upon completion
            metricsFacade.recordTransaction();
            metricsFacade.recordTransactionRowCount(actualProcessedRows);
        } else
        {
            // Not complete, keep tracking
            LOGGER.debug("Transaction {} is partially complete ({} rows processed). Keeping tracker entry.", transId, actualProcessedRows);
        }
    }

    // --- Interface Methods ---

    @Override
    public void flush()
    {
        // No-op for NoneWriter
    }

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
            TransactionContext context = transTracker.computeIfAbsent(transId, k -> new TransactionContext());

            // 2. Check if TX END has arrived
            if (context.isEndReceived())
            {
                // TX END arrived: Use official TableCounters
                TableCounters counters = context.tableCounters.get(fullTable);
                if (counters != null)
                {
                    // Increment the processed row count for this table
                    counters.increment();

                    // If this table completed, check if the entire transaction is complete.
                    if (counters.isComplete())
                    {
                        checkAndCleanupTransaction(transId);
                    }
                } else
                {
                    LOGGER.warn("Row received for TransId {} / Table {} but was not included in TX END metadata.", transId, fullTable);
                }
            } else
            {
                // TX END has not arrived: Accumulate count in preEndCounts
                context.incrementPreEndCount(fullTable);
                LOGGER.debug("Row received for TransId {} / Table {} before TX END. Accumulating count.", transId, fullTable);
            }
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
            transTracker.computeIfAbsent(transId, k -> new TransactionContext());
            LOGGER.debug("Transaction {} BEGIN received.", transId);

        } else if (transactionMetadata.getStatus() == SinkProto.TransactionStatus.END)
        {
            // 2. END: Finalize tracker state, merge pre-counts, and trigger cleanup.

            // Get existing context or create a new one (in case BEGIN was missed).
            TransactionContext context = transTracker.computeIfAbsent(transId, k -> new TransactionContext());

            // --- Initialization Step: Set Total Counts ---
            Map<String, TableCounters> newTableCounters = new ConcurrentHashMap<>();
            for(SinkProto.DataCollection dataCollection: transactionMetadata.getDataCollectionsList())
            {
                String fullTable = dataCollection.getDataCollection();
                // Create official counter with total count
                newTableCounters.put(fullTable, new TableCounters((int)dataCollection.getEventCount()));
            }

            // Set the final state (must be volatile write)
            context.setEndReceived(newTableCounters);

            // --- Merge Step: Apply pre-received rows ---
            for (Map.Entry<String, AtomicInteger> preEntry : context.preEndCounts.entrySet())
            {
                String table = preEntry.getKey();
                int accumulatedCount = preEntry.getValue().get();
                TableCounters finalCounter = newTableCounters.get(table);

                if (finalCounter != null)
                {
                    // Apply the accumulated count to the official counter
                    for(int i = 0; i < accumulatedCount; i++)
                    {
                        finalCounter.increment();
                    }
                } else
                {
                    LOGGER.warn("Pre-received rows for table {} (count: {}) but table was not in TX END metadata. Discarding accumulated count.", table, accumulatedCount);
                }
            }

            if (!newTableCounters.isEmpty())
            {
                LOGGER.debug("Transaction {} END received. Tracking initialized and pre-counts merged for {} tables.", transId, newTableCounters.size());
            } else
            {
                LOGGER.info("Transaction {} END received with zero row collections. Marking as complete.", transId);
            }

            // --- Cleanup/Validation Step ---
            // Trigger cleanup. This will validate if all rows (pre and post END) have satisfied the total counts.
            checkAndCleanupTransaction(transId);
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
                if (context.isEndReceived() && context.tableCounters != null)
                {
                    context.tableCounters.forEach((table, counters) ->
                    {
                        if (!counters.isComplete())
                        {
                            LOGGER.warn("Unfinished transaction {}: Table {} - Processed {}/{} rows.",
                                    transId, table, counters.getCurrentCount(), counters.getTotalCount());
                        }
                    });
                } else
                {
                    LOGGER.warn("Unfinished transaction {}: TX END not received. Pre-received rows: {}",
                            transId, context.preEndCounts);
                }
            });
        }
    }
}