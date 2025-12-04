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
 
package io.pixelsdb.pixels.sink.provider;


import io.pixelsdb.pixels.sink.event.RowChangeEvent;

/**
 * @package: io.pixelsdb.pixels.sink.provider
 * @className: TableEventProvider
 * @author: AntiO2
 * @date: 2025/9/26 07:47
 */
public abstract class TableEventProvider<SOURCE_RECORD_T> extends EventProvider<SOURCE_RECORD_T, RowChangeEvent>
{
    protected void putRowChangeEvent(RowChangeEvent rowChangeEvent)
    {
        putTargetEvent(rowChangeEvent);
    }

    public RowChangeEvent getRowChangeEvent()
    {
        return getTargetEvent();
    }

    protected void putRawRowChangeEvent(SOURCE_RECORD_T record)
    {
        putRawEvent(record);
    }

    final protected void recordSerdEvent()
    {
        metricsFacade.recordSerdRowChange();
    }
}
