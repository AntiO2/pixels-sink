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

package io.pixelsdb.pixels.sink.writer.proto;


import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.util.TableCounters;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @package: io.pixelsdb.pixels.sink.writer
 * @className: ProtoWriter
 * @author: AntiO2
 * @date: 2025/10/5 07:10
 */
public class ProtoWriter implements PixelsSinkWriter {
    private final Logger LOGGER = LoggerFactory.getLogger(ProtoWriter.class);
    private final RotatingWriterManager writerManager;
    private final TableMetadataRegistry instance;
    private final ReentrantLock lock = new ReentrantLock();
    /**
     * Data structure to track transaction progress:
     * Map<TransId, TransactionContext>
     */
    private final Map<String, TransactionContext> transTracker = new ConcurrentHashMap<>();


    public ProtoWriter() throws IOException {
        PixelsSinkConfig sinkConfig = PixelsSinkConfigFactory.getInstance();

        String dataPath = sinkConfig.getSinkProtoData();
        this.writerManager = new RotatingWriterManager(dataPath);
        this.instance = TableMetadataRegistry.Instance();
    }

    /**
     * Checks if all tables within a transaction have reached their expected row count.
     * If complete, the transaction is removed from the tracker and final metrics are recorded.
     *
     * @param transId The ID of the transaction to check.
     */
    private void checkAndCleanupTransaction(String transId) {
        TransactionContext context = transTracker.get(transId);

        if (context == null || !context.isEndReceived()) {
            // Transaction has not received TX END or has been cleaned up already.
            return;
        }

        Map<String, TableCounters> tableMap = context.tableCounters;
        if (tableMap == null || tableMap.isEmpty()) {
            // Empty transaction with no tables. Clean up immediately.
            transTracker.remove(transId);
            LOGGER.info("Transaction {} (empty) successfully completed and removed from tracker.", transId);
            return;
        }

        boolean allComplete = true;
        int actualProcessedRows = 0;

        // Iterate through all tables to check completion status
        for (Map.Entry<String, TableCounters> entry : tableMap.entrySet()) {
            TableCounters counters = entry.getValue();
            if (!counters.isComplete()) {
                allComplete = false;
            }
        }

        if (allComplete) {
            transTracker.remove(transId);
            ByteBuffer transInfo = getTransBuffer(context);
            transInfo.rewind();
            writeBuffer(transInfo);
        }
    }

    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata) {
        try {
            lock.lock();
            String transId = transactionMetadata.getId();
            if (transactionMetadata.getStatus() == SinkProto.TransactionStatus.BEGIN) {
                // 1. BEGIN: Create context if not exists (in case ROWChange arrived first).
                TransactionContext transactionContext = transTracker.computeIfAbsent(transId, k -> new TransactionContext());
                LOGGER.debug("Transaction {} BEGIN received.", transId);
                transactionContext.txBegin = transactionMetadata;
            } else if (transactionMetadata.getStatus() == SinkProto.TransactionStatus.END) {
                // 2. END: Finalize tracker state, merge pre-counts, and trigger cleanup.

                // Get existing context or create a new one (in case BEGIN was missed).
                TransactionContext context = transTracker.computeIfAbsent(transId, k -> new TransactionContext());

                // --- Initialization Step: Set Total Counts ---
                Map<String, TableCounters> newTableCounters = new ConcurrentHashMap<>();
                for (SinkProto.DataCollection dataCollection : transactionMetadata.getDataCollectionsList()) {
                    String fullTable = dataCollection.getDataCollection();
                    // Create official counter with total count
                    newTableCounters.put(fullTable, new TableCounters((int) dataCollection.getEventCount()));
                }

                // Set the final state (must be volatile write)
                context.setEndReceived(newTableCounters);

                // --- Merge Step: Apply pre-received rows ---
                for (Map.Entry<String, AtomicInteger> preEntry : context.preEndCounts.entrySet()) {
                    String table = preEntry.getKey();
                    int accumulatedCount = preEntry.getValue().get();
                    TableCounters finalCounter = newTableCounters.get(table);

                    if (finalCounter != null) {
                        // Apply the accumulated count to the official counter
                        for (int i = 0; i < accumulatedCount; i++) {
                            finalCounter.increment();
                        }
                    } else {
                        LOGGER.warn("Pre-received rows for table {} (count: {}) but table was not in TX END metadata. Discarding accumulated count.", table, accumulatedCount);
                    }
                }
                context.txEnd = transactionMetadata;

                // --- Cleanup/Validation Step ---
                // Trigger cleanup. This will validate if all rows (pre and post END) have satisfied the total counts.
                checkAndCleanupTransaction(transId);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private ByteBuffer getTransBuffer(TransactionContext transactionContext) {
        int total = 0;
        byte[] transDataBegin = transactionContext.txBegin.toByteArray();
        ByteBuffer beginByteBuffer = writeData(-1, transDataBegin);
        total += beginByteBuffer.limit();
        beginByteBuffer.rewind();
        byte[] transDataEnd = transactionContext.txEnd.toByteArray();
        ByteBuffer endByteBuffer = writeData(-1, transDataEnd);
        endByteBuffer.rewind();
        total += endByteBuffer.limit();
        List<ByteBuffer> rowEvents = new ArrayList<>();
        for (RowChangeEvent rowChangeEvent : transactionContext.rowChangeEventList) {
            ByteBuffer byteBuffer = write(rowChangeEvent.getRowRecord());
            if (byteBuffer == null) {
                return null;
            }
            byteBuffer.rewind();
            rowEvents.add(byteBuffer);
            total += byteBuffer.limit();
        }
        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.put(beginByteBuffer.array());
        for (ByteBuffer rowEvent : rowEvents) {
            buffer.put(rowEvent.array());
        }
        buffer.put(endByteBuffer.array());
        return buffer;
    }

    public ByteBuffer write(SinkProto.RowRecord rowRecord) {
        byte[] rowData = rowRecord.toByteArray();
        String tableName = rowRecord.getSource().getTable();
        String schemaName = rowRecord.getSource().getDb();

        long tableId;
        try {
            tableId = instance.getTableId(schemaName, tableName);
        } catch (SinkException e) {
            LOGGER.error("Error while getting schema table id.", e);
            return null;
        }
        {
            return writeData((int) tableId, rowData);
        }
    }

    // key: -1 means transaction, else means table id
    private ByteBuffer writeData(int key, byte[] data) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + data.length).order(ByteOrder.BIG_ENDIAN); // key + value len + data
        buf.putInt(key).putInt(data.length).put(data);
        return buf;
    }

    private synchronized boolean writeBuffer(ByteBuffer buf) {
        PhysicalWriter writer;
        try {
            writer = writerManager.current();
            writer.prepare(buf.remaining());
            writer.append(buf.array());
        } catch (IOException e) {
            LOGGER.error("Error while writing row record.", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean writeRow(RowChangeEvent rowChangeEvent) {
        try {
            lock.lock();
            String transId = rowChangeEvent.getTransaction().getId();
            String fullTable = rowChangeEvent.getFullTableName();

            // 1. Get or create the transaction context
            TransactionContext context = transTracker.computeIfAbsent(transId, k -> new TransactionContext());
            context.rowChangeEventList.add(rowChangeEvent);
            // 2. Check if TX END has arrived
            if (context.isEndReceived()) {
                // TX END arrived: Use official TableCounters
                TableCounters counters = context.tableCounters.get(fullTable);
                if (counters != null) {
                    // Increment the processed row count for this table
                    counters.increment();

                    // If this table completed, check if the entire transaction is complete.
                    if (counters.isComplete()) {
                        checkAndCleanupTransaction(transId);
                    }
                } else {
                    LOGGER.warn("Row received for TransId {} / Table {} but was not included in TX END metadata.", transId, fullTable);
                }
            } else {
                context.incrementPreEndCount(fullTable);
                LOGGER.debug("Row received for TransId {} / Table {} before TX END. Accumulating count.", transId, fullTable);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws IOException {
        this.writerManager.close();
    }

    private static class TransactionContext {
        // Key: Full Table Name, Value: Row Count
        private final Map<String, AtomicInteger> preEndCounts = new ConcurrentHashMap<>();
        public List<RowChangeEvent> rowChangeEventList = new ArrayList<>();
        public SinkProto.TransactionMetadata txBegin;
        public SinkProto.TransactionMetadata txEnd;
        @Getter
        private volatile boolean endReceived = false;
        // Key: Full Table Name
        private Map<String, TableCounters> tableCounters = null;

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
}