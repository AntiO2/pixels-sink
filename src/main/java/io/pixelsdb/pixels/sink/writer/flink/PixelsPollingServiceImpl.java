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
 
package io.pixelsdb.pixels.sink.writer.flink;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.PixelsPollingServiceGrpc;
import io.pixelsdb.pixels.sink.util.FlushRateLimiter;
import io.pixelsdb.pixels.sink.writer.flink.FlinkPollingWriter;
import io.pixelsdb.pixels.sink.util.DataTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.validation.Schema;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 实现了 .proto 文件中定义的 PixelsPollingService 服务。
 * 它处理来自客户端的 PollRequest，并从 FlinkPollingWriter 中拉取数据进行响应。
 */
// *** 核心修复: 继承自 gRPC 生成的基类 ***
public class PixelsPollingServiceImpl extends PixelsPollingServiceGrpc.PixelsPollingServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsPollingServiceImpl.class);
    private final FlinkPollingWriter writer;
    private final int pollBatchSize;
    private final long pollTimeoutMs;
    private final FlushRateLimiter flushRateLimiter;
    /**
     * 构造函数，注入 FlinkPollingWriter 并初始化服务器端配置。
     * @param writer 数据缓冲区的实例。
     */
    public PixelsPollingServiceImpl(FlinkPollingWriter writer) {
        if (writer == null) {
            throw new IllegalArgumentException("FlinkPollingWriter cannot be null.");
        }
        this.writer = writer;
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        this.pollBatchSize = config.getCommitBatchSize();
        this.pollTimeoutMs = config.getTimeoutMs();
        this.flushRateLimiter = FlushRateLimiter.getInstance();
        LOGGER.info("PixelsPollingServiceImpl initialized. Using 'sink.commit.batch.size' for pollBatchSize ({}) " +
                        "and 'sink.timeout.ms' for pollTimeoutMs ({}).",
                this.pollBatchSize, this.pollTimeoutMs);
    }

    @Override
    public void pollEvents(SinkProto.PollRequest request, StreamObserver<SinkProto.PollResponse> responseObserver) {
        SchemaTableName schemaTableName = new SchemaTableName(request.getSchemaName(), request.getTableName());
        LOGGER.debug("Received poll request for table '{}'", schemaTableName);
        try {
            List<SinkProto.RowRecord> records = writer.pollRecords(
                    schemaTableName,
                    pollBatchSize,
                    pollTimeoutMs,
                    TimeUnit.MILLISECONDS
            );

            SinkProto.PollResponse.Builder responseBuilder = SinkProto.PollResponse.newBuilder();
            if(records != null)
            {
                List<SinkProto.RowRecord> updatedRecords =  updatedRecords = DataTransform.updateRecordTimestamp(
                        records,
                        System.currentTimeMillis()
                );

                if (updatedRecords != null && !updatedRecords.isEmpty()) {
                    responseBuilder.addAllRecords(updatedRecords);
                    this.flushRateLimiter.acquire(updatedRecords.size());
                }
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Polling thread was interrupted for table: " + schemaTableName, e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Server polling was interrupted")
                    .asRuntimeException());
        } catch (Exception e) {
            LOGGER.error("An unexpected error occurred while polling for table: " + schemaTableName, e);
            responseObserver.onError(io.grpc.Status.UNKNOWN
                    .withDescription("An unexpected error occurred: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}