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

import io.pixelsdb.pixels.common.node.BucketCache;
import io.pixelsdb.pixels.daemon.NodeProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableWriterProxy {
    private final static TableWriterProxy INSTANCE = new TableWriterProxy();

    private final TransactionMode transactionMode;
    private final int retinaCliNum;
    private final Map<WriterKey, TableWriter> WRITER_REGISTRY = new ConcurrentHashMap<>();

    private TableWriterProxy() {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.transactionMode = pixelsSinkConfig.getTransactionMode();
        this.retinaCliNum = pixelsSinkConfig.getRetinaClientNum();
    }

    protected static TableWriterProxy getInstance() {
        return INSTANCE;
    }

    protected TableWriter getTableWriter(String tableName, long tableId, int bucket) {
        int cliNo = bucket % retinaCliNum;
        // warn: we assume table id is less than INT.MAX
        WriterKey key = new WriterKey(tableId, BucketCache.getInstance().getRetinaNodeInfoByBucketId(bucket), cliNo);

        return WRITER_REGISTRY.computeIfAbsent(key, t ->
        {
            switch (transactionMode) {
                case SINGLE -> {
                    return new TableSingleTxWriter(tableName, bucket);
                }
                case BATCH -> {
                    return new TableCrossTxWriter(tableName, bucket);
                }
                case RECORD -> {
                    return new TableSingleRecordWriter(tableName, bucket);
                }
                default -> {
                    throw new IllegalArgumentException("Unknown transaction mode: " + transactionMode);
                }
            }
        });
    }

    record WriterKey(long tableId, NodeProto.NodeInfo nodeInfo, int cliNo) {
    }
}
