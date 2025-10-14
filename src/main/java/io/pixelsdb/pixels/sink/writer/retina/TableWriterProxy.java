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

package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableWriterProxy
{
    private final static TableWriterProxy INSTANCE = new TableWriterProxy();

    private final TransactionMode transactionMode;

    record TableKey(long tableId, int bucket) { }
    private final Map<TableKey, TableWriter> WRITER_REGISTRY = new ConcurrentHashMap<>();

    private TableWriterProxy()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.transactionMode = pixelsSinkConfig.getTransactionMode();
    }

    protected static TableWriterProxy getInstance()
    {
        return INSTANCE;
    }

    protected TableWriter getTableWriter(String tableName, long tableId, int bucket)
    {
        // warn: we assume table id is less than INT.MAX
        TableKey key = new TableKey(tableId, bucket);
        return WRITER_REGISTRY.computeIfAbsent(key, t ->
        {
            switch (transactionMode)
            {
                case SINGLE ->
                {
                    return new TableSingleTxWriter(tableName);
                }
                case BATCH ->
                {
                    return new TableCrossTxWriter(tableName, bucket);
                }
                default ->
                {
                    throw new IllegalArgumentException("Unknown transaction mode: " + transactionMode);
                }
            }
        });
    }
}
