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

public abstract class AbstractBucketedWriter<C>
{
    public void writeRowChangeEvent(RowChangeEvent event, C context) throws SinkException
    {
        if (event == null)
        {
            return;
        }

        event.initIndexKey();

        switch (event.getOp())
        {
            case UPDATE ->
            {
                if (!event.isPkChanged())
                {
                    emitBefore(event, context);
                }
                else
                {
                    emitPkChangedUpdate(event, context);
                }
            }

            case DELETE -> emitBefore(event, context);

            case INSERT, SNAPSHOT -> emitAfter(event, context);

            case UNRECOGNIZED ->
            {
                return;
            }
        }
    }

    /* ================= hook points ================= */

    protected void emitBefore(RowChangeEvent event, C context)
    {
        int bucketId = event.getBeforeBucketFromIndex();
        emit(event, bucketId, context);
    }

    protected void emitAfter(RowChangeEvent event, C context)
    {
        int bucketId = event.getAfterBucketFromIndex();
        emit(event, bucketId, context);
    }

    protected void emitPkChangedUpdate(RowChangeEvent event, C context) throws SinkException
    {
        // DELETE (before)
        RowChangeEvent deleteEvent = buildDeleteEvent(event);
        emitBefore(deleteEvent, context);

        // INSERT (after)
        RowChangeEvent insertEvent = buildInsertEvent(event);
        emitAfter(insertEvent, context);
    }

    protected abstract void emit(RowChangeEvent event, int bucketId, C context);

    /* ================= helpers ================= */

    private RowChangeEvent buildDeleteEvent(RowChangeEvent event) throws SinkException
    {
        SinkProto.RowRecord.Builder builder =
                event.getRowRecord().toBuilder()
                        .clearAfter()
                        .setOp(SinkProto.OperationType.DELETE);

        RowChangeEvent deleteEvent =
                new RowChangeEvent(builder.build(), event.getSchema());
        deleteEvent.initIndexKey();
        return deleteEvent;
    }

    private RowChangeEvent buildInsertEvent(RowChangeEvent event) throws SinkException
    {
        SinkProto.RowRecord.Builder builder =
                event.getRowRecord().toBuilder()
                        .clearBefore()
                        .setOp(SinkProto.OperationType.INSERT);

        RowChangeEvent insertEvent =
                new RowChangeEvent(builder.build(), event.getSchema());
        insertEvent.initIndexKey();
        return insertEvent;
    }
}
