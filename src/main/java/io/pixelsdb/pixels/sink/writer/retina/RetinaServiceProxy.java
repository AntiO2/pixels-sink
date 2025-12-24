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

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.node.BucketCache;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.PixelsSinkMode;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetinaServiceProxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaServiceProxy.class);
    @Getter
    private static final PixelsSinkMode pixelsSinkMode = PixelsSinkMode.RETINA;
    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
    // private static final IndexService indexService = IndexService.Instance();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final RetinaService retinaService;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final int vNodeId;
    private RetinaService.StreamHandler retinaStream = null;

    public RetinaServiceProxy(int bucketId) {
        if (bucketId == -1) {
            this.retinaService = RetinaService.Instance();
        } else {
            this.retinaService = RetinaUtils.getRetinaServiceFromBucketId(bucketId);
        }


        if (config.getRetinaWriteMode() == RetinaWriteMode.STREAM) {
            retinaStream = retinaService.startUpdateStream();
        } else {
            retinaStream = null;
        }

        this.vNodeId = BucketCache.getInstance().getRetinaNodeInfoByBucketId(bucketId).getVirtualNodeId();
    }

    public boolean writeTrans(String schemaName, List<RetinaProto.TableUpdateData> tableUpdateData) {
        if (config.getRetinaWriteMode() == RetinaWriteMode.STUB) {
            try {
                retinaService.updateRecord(schemaName, vNodeId, tableUpdateData);
            } catch (RetinaException e) {
                e.printStackTrace();
                return false;
            }
        } else {
            try {
                retinaStream.updateRecord(schemaName, vNodeId, tableUpdateData);
            } catch (RetinaException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public CompletableFuture<RetinaProto.UpdateRecordResponse> writeBatchAsync
            (String schemaName, List<RetinaProto.TableUpdateData> tableUpdateData) {
        if (config.getRetinaWriteMode() == RetinaWriteMode.STUB) {
            try {
                retinaService.updateRecord(schemaName, vNodeId, tableUpdateData);
            } catch (RetinaException e) {
                e.printStackTrace();
            }
            return null;
        } else {
            try {
                return retinaStream.updateRecord(schemaName, vNodeId, tableUpdateData);
            } catch (RetinaException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    public void close() throws IOException {
        isClosed.compareAndSet(false, true);
        if (config.getRetinaWriteMode() == RetinaWriteMode.STREAM) {
            retinaStream.close();
        }
    }

    public enum RetinaWriteMode {
        STREAM,
        STUB;

        public static RetinaWriteMode fromValue(String value) {
            for (RetinaWriteMode mode : values()) {
                if (mode.name().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new RuntimeException(String.format("Can't convert %s to Retina writer type", value));
        }
    }
}
