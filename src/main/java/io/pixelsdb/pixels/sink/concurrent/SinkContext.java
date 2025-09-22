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

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SinkContext
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkContext.class);
    final ReentrantLock lock = new ReentrantLock();
    final Condition cond = lock.newCondition();

    final String sourceTxId;
    final Map<String, Long> tableCursors = new ConcurrentHashMap<>();
    final Map<String, Long> tableCounters = new ConcurrentHashMap<>();
    final Map<String, RetinaProto.TableUpdateData.Builder> tableUpdateDataMap = new ConcurrentHashMap<>();
    final Map<String, ReentrantLock> tableUpdateLockMap= new ConcurrentHashMap<>();
    final AtomicInteger pendingEvents = new AtomicInteger(0);
    final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

    final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    TransContext pixelsTransCtx;
    volatile boolean completed = false;


    SinkContext(String sourceTxId)
    {
        this.sourceTxId = sourceTxId;
        this.pixelsTransCtx = null;
    }

    SinkContext(String sourceTxId, TransContext pixelsTransCtx)
    {
        this.sourceTxId = sourceTxId;
        this.pixelsTransCtx = pixelsTransCtx;
    }

    public List<RetinaProto.TableUpdateData> getTableUpdateDataList()
    {
        return tableUpdateDataMap.values()
                .stream()
                .map(RetinaProto.TableUpdateData.Builder::build)
                .toList();
    }

    boolean isReadyForDispatch(String table, long collectionOrder)
    {
        lock.lock();
        boolean ready = tableCursors
                .computeIfAbsent(table, k -> 1L) >= collectionOrder;
        lock.unlock();
        return ready;
    }

    void updateCursor(String table, long currentOrder)
    {
        tableCursors.compute(table, (k, v) ->
                (v == null) ? currentOrder + 1 : Math.max(v, currentOrder + 1));
    }

    void updateCounter(String table)
    {
        tableCounters.compute(table, (k, v) ->
                (v == null) ? 1 : v + 1);
    }

    public void updateCounter(String table, long count) {
        tableCounters.compute(table, (k, v) ->
                (v == null) ? count : v + count);
    }

    Set<String> getTrackedTables()
    {
        return tableCursors.keySet();
    }

    boolean isCompleted(SinkProto.TransactionMetadata tx)
    {
        for (SinkProto.DataCollection dataCollection : tx.getDataCollectionsList())
        {
            // Long targetEventCount = tableCursors.get(dataCollection.getDataCollection());
            Long targetEventCount = tableCounters.get(dataCollection.getDataCollection());
            long target = targetEventCount == null ? 0 : targetEventCount;
            LOGGER.debug("TX {}, Table {}, event count {}, tableCursors {}", tx.getId(), dataCollection.getDataCollection(), dataCollection.getEventCount(), target);
            if (dataCollection.getEventCount() > target)
            {
                return false;
            }
        }
        return true;
    }

    boolean isExpired()
    {
        // TODO: expire timeout transaction
        return false;
        // return System.currentTimeMillis() - pixelsTransCtx.getTimestamp() > TX_TIMEOUT_MS;
    }

    void markCompleted()
    {
        this.completed = true;
    }

    void awaitCompletion() throws InterruptedException, ExecutionException
    {
        completionFuture.get();
    }

    public long getTimestamp()
    {
        return pixelsTransCtx == null ? 0 : pixelsTransCtx.getTimestamp();
    }

    public ReentrantLock getLock()
    {
        return lock;
    }

    public Condition getCond()
    {
        return cond;
    }

    public String getSourceTxId()
    {
        return sourceTxId;
    }

    public Map<String, Long> getTableCursors()
    {
        return tableCursors;
    }

    public Map<String, Long> getTableCounters()
    {
        return tableCounters;
    }

    public Map<String, RetinaProto.TableUpdateData.Builder> getTableUpdateDataMap()
    {
        return tableUpdateDataMap;
    }

    public Map<String, ReentrantLock> getTableUpdateLockMap()
    {
        return tableUpdateLockMap;
    }

    public AtomicInteger getPendingEvents()
    {
        return pendingEvents;
    }

    public CompletableFuture<Void> getCompletionFuture()
    {
        return completionFuture;
    }

    public TableMetadataRegistry getTableMetadataRegistry()
    {
        return tableMetadataRegistry;
    }

    public TransContext getPixelsTransCtx()
    {
        return pixelsTransCtx;
    }

    public void setPixelsTransCtx(TransContext pixelsTransCtx)
    {
        this.pixelsTransCtx = pixelsTransCtx;
    }

    public boolean isCompleted()
    {
        return completed;
    }

    public void setCompleted(boolean completed)
    {
        this.completed = completed;
    }
}
