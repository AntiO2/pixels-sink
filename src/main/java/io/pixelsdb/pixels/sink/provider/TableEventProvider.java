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
    public RowChangeEvent getRowChangeEvent()
    {
        return getTargetEvent();
    }

    protected void putRowChangeEvent(RowChangeEvent rowChangeEvent)
    {
        putTargetEvent(rowChangeEvent);
    }
}
