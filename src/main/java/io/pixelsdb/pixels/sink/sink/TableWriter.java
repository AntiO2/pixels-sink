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


package io.pixelsdb.pixels.sink.sink;

import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.concurrent.SinkContext;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class TableWriter
{
    private static final Map<String, TableWriter> WRITER_REGISTRY = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(TableWriter.class);
    private static final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    private final PixelsSinkWriter delegate; // physical writer
    private final Map<String, SinkContext> txnContextMap = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private final long TX_TIMEOUT_MS = 3000;

    private final ReentrantLock lock = new ReentrantLock();

    private List<RowChangeEvent> buffer = new ArrayList<>();
    private volatile String currentTxId = null;
    private final String tableName;
    private ScheduledFuture<?> flushTask = null;
    private RetinaProto.TableUpdateData.Builder builder;
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

    public boolean write(RowChangeEvent event, SinkContext ctx)
    {
        lock.lock();
        try
        {
            String txId = ctx.getSourceTxId();
            if (currentTxId != null && !currentTxId.equals(txId))
            {
                flushInternal(ctx);
                builder = RetinaProto.TableUpdateData.newBuilder();
                long primaryIndexId = tableMetadataRegistry.getPrimaryIndexKeyId(event.getSchemaName(), tableName);
                builder.setTableName(tableName);
                builder.setPrimaryIndexId(primaryIndexId);
            }

            currentTxId = txId;
            buffer.add(event);

            if (flushTask == null || flushTask.isDone())
            {
                flushTask = scheduler.schedule(() ->
                {
                    try
                    {
                        lock.lock();
                        flushInternal(ctx);
                    } catch (Exception e)
                    {
                        LOGGER.error("Scheduled flush failed for table {}", tableName, e);
                    } finally
                    {
                        lock.unlock();
                    }
                }, 3, TimeUnit.SECONDS); // 3s 定时器
            }
        } catch (SinkException e)
        {
            LOGGER.error(e.getMessage(), e);
            return false;
        } finally
        {
            lock.unlock();
        }
        return true;
    }

    public void flush(SinkContext sinkContext)
    {
        lock.lock();
        try
        {
            flushInternal(sinkContext);
        } catch (SinkException e)
        {
            throw new RuntimeException(e);
        } finally
        {
            lock.unlock();
        }
    }

    private void flushInternal(SinkContext sinkContext) throws SinkException
    {
        if (buffer.isEmpty() || currentTxId == null)
        {
            return;
        }

        List<RowChangeEvent> batch = buffer;
        buffer = new ArrayList<>();

        String txId = currentTxId;
        currentTxId = null;

        LOGGER.info("Flushing {} events for table {} txId={}", batch.size(), tableName, txId);

        for (RowChangeEvent event : batch)
        {
            addUpdateData(event, builder, sinkContext);
        }
        List<RetinaProto.TableUpdateData> tableUpdateData = List.of(builder.build());

        delegate.writeTrans(batch.get(0).getSchemaName(), tableUpdateData, sinkContext.getTimestamp());
        sinkContext.updateCounter(tableName, batch.size());
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

    public static void addUpdateData(RowChangeEvent rowChangeEvent, RetinaProto.TableUpdateData.Builder builder, SinkContext ctx) throws SinkException
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
}
