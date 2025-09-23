/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pixelsdb.pixels.sink.sink;

import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.concurrent.SinkContext;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinatorFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class TableWriter
{
    private static final Map<String, TableWriter> WRITER_REGISTRY = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(TableWriter.class);
    private static final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    private static final long TX_TIMEOUT_MS = 3000;
    private final PixelsSinkWriter delegate; // physical writer
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ReentrantLock lock = new ReentrantLock();
    private final String tableName;
    // Shared state (protected by lock)
    private List<RowChangeEvent> buffer = new ArrayList<>();
    private volatile String currentTxId = null;
    private ScheduledFuture<?> flushTask = null;
    private String fullTableName;

    public TableWriter(String tableName) throws IOException
    {
        this.delegate = PixelsSinkWriterFactory.getWriter();
        this.tableName = tableName;
    }

    public static TableWriter getTableWriter(String tableName)
    {
        return WRITER_REGISTRY.computeIfAbsent(tableName, t ->
        {
            try
            {
                return new TableWriter(t);
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Helper: add insert/delete data into proto builder.
     */
    public static void addUpdateData(RowChangeEvent rowChangeEvent,
                                     RetinaProto.TableUpdateData.Builder builder) throws SinkException
    {
        if (rowChangeEvent.hasBeforeData())
        {
            RetinaProto.DeleteData.Builder deleteDataBuilder = RetinaProto.DeleteData.newBuilder();
            deleteDataBuilder.addIndexKeys(rowChangeEvent.getBeforeKey());
            builder.addDeleteData(deleteDataBuilder);
        }

        if (rowChangeEvent.hasAfterData())
        {
            RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
            insertDataBuilder.addIndexKeys(rowChangeEvent.getAfterKey());
            insertDataBuilder.addAllColValues(rowChangeEvent.getAfterData());
            builder.addInsertData(insertDataBuilder);
        }
    }

    /**
     * Write a single RowChangeEvent.
     * Ensures that events are grouped by transaction (txId).
     */
    public boolean write(RowChangeEvent event, SinkContext ctx)
    {
        try
        {
            boolean needFlush = false;

            lock.lock();
            try
            {
                String txId = ctx.getSourceTxId();

                // If this is a new transaction, flush the old one
                if (currentTxId == null || !currentTxId.equals(txId))
                {
                    needFlush = !buffer.isEmpty();
                    if (needFlush)
                    {
                        if (flushTask != null)
                        {
                            flushTask.cancel(false);
                        }
                        flush();
                    }
                    currentTxId = txId;
                    if (fullTableName == null)
                    {
                        fullTableName = event.getFullTableName();
                    }
                }

                buffer.add(event);

                // Reset scheduled flush: cancel old one and reschedule
                if (flushTask != null && !flushTask.isDone())
                {
                    flushTask.cancel(false);
                }
                flushTask = scheduler.schedule(() ->
                {
                    try
                    {
                        lock.lock();
                        try
                        {
                            if (txId.equals(currentTxId))
                            {
                                flush();
                            }
                        } finally
                        {
                            lock.unlock();
                        }
                    } catch (Exception e)
                    {
                        LOGGER.error("Scheduled flush failed for table {}", tableName, e);
                    }
                }, TX_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } finally
            {
                lock.unlock();
            }
            return true;
        } catch (Exception e)
        {
            LOGGER.error("Write failed for table {}", tableName, e);
            return false;
        }
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush()
    {
        List<RowChangeEvent> batch;
        String txId;
        RetinaProto.TableUpdateData.Builder toBuild;
        SinkContext sinkContext = null;
        lock.lock();
        try
        {
            if (buffer.isEmpty() || currentTxId == null)
            {
                return;
            }
            txId = currentTxId;
            currentTxId = null;

            sinkContext = TransactionCoordinatorFactory.getCoordinator().getSinkContext(txId);
            sinkContext.getLock().lock();
            try
            {
                while (sinkContext.getPixelsTransCtx() == null)
                {
                    LOGGER.warn("Wait for prev tx to begin trans: {}", txId);
                    sinkContext.getCond().await();
                }
            } finally
            {
                sinkContext.getLock().unlock();
            }

            // Swap buffers quickly under lock
            batch = buffer;
            buffer = new ArrayList<>();
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        } finally
        {
            lock.unlock();
        }

        RowChangeEvent event1 = batch.get(0);

        RetinaProto.TableUpdateData.Builder builder = RetinaProto.TableUpdateData.newBuilder()
                .setPrimaryIndexId(event1.getTableMetadata().getPrimaryIndexKeyId())
                .setTableName(tableName);


        try
        {
            for (RowChangeEvent event : batch)
            {
                addUpdateData(event, builder);
            }
            List<RetinaProto.TableUpdateData> tableUpdateData = List.of(builder.build());
            delegate.writeTrans(event1.getSchemaName(), tableUpdateData, sinkContext.getTimestamp());
            sinkContext.updateCounter(fullTableName, batch.size());
            // ---- Outside lock: build proto and write ----
            LOGGER.info("Flushing {} events for table {} txId={}", batch.size(), fullTableName, txId);
        } catch (SinkException e)
        {
            throw new RuntimeException("Flush failed for table " + tableName, e);
        }
    }

    public void close()
    {
        scheduler.shutdown();
        try
        {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            delegate.close();
        } catch (InterruptedException ignored)
        {
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
