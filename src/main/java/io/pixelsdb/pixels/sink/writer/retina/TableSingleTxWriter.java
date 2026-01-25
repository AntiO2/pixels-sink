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
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TableSingleTxWriter extends TableWriter
{
    private static final long TX_TIMEOUT_MS = 3000;
    @Getter
    private final Logger LOGGER = LoggerFactory.getLogger(TableSingleTxWriter.class);

    public TableSingleTxWriter(String tableName, int bucketId)
    {
        super(tableName, bucketId);
    }

    /**
     * Flush any buffered events for the current transaction.
     */
    public void flush(List<RowChangeEvent> batchToFlush)
    {
        List<RowChangeEvent> batch;
        String txId;
        RetinaProto.TableUpdateData.Builder toBuild;
        SinkContext sinkContext = null;
        bufferLock.lock();
        try
        {
            if (buffer.isEmpty() || currentTxId == null)
            {
                return;
            }
            txId = currentTxId;
            currentTxId = null;

            sinkContext = SinkContextManager.getInstance().getSinkContext(txId);
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
            bufferLock.unlock();
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
            delegate.writeTrans(event1.getSchemaName(), tableUpdateData);
            sinkContext.updateCounter(fullTableName, batch.size());
            // ---- Outside lock: build proto and write ----
            LOGGER.info("Flushing {} events for table {} txId={}", batch.size(), fullTableName, txId);
        } catch (SinkException e)
        {
            throw new RuntimeException("Flush failed for table " + tableName, e);
        }
    }

    @Override
    protected boolean needFlush()
    {
        if (currentTxId == null || !currentTxId.equals(txId))
        {
            return !buffer.isEmpty();
        }
        return false;
    }
}
