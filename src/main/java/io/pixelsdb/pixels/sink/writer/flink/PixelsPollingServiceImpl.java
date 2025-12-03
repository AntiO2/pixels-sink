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
package io.pixelsdb.pixels.sink.writer.flink;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.PixelsPollingServiceGrpc; 
import io.pixelsdb.pixels.sink.writer.flink.FlinkPollingWriter;
import io.pixelsdb.pixels.sink.util.DataTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        LOGGER.info("PixelsPollingServiceImpl initialized. Using 'sink.commit.batch.size' for pollBatchSize ({}) " +
                        "and 'sink.timeout.ms' for pollTimeoutMs ({}).",
                this.pollBatchSize, this.pollTimeoutMs);
    }

    @Override
    public void pollEvents(SinkProto.PollRequest request, StreamObserver<SinkProto.PollResponse> responseObserver) {
        String fullTableName = request.getSchemaName() + "." + request.getTableName();
        LOGGER.debug("Received poll request for table '{}'", fullTableName);
        try {
            List<SinkProto.RowRecord> records = writer.pollRecords(
                    fullTableName,
                    pollBatchSize,
                    pollTimeoutMs,
                    TimeUnit.MILLISECONDS
            );
            List<SinkProto.RowRecord> updatedRecords = DataTransform.updateRecordTimestamp(
                records,
                System.currentTimeMillis()
            );
            SinkProto.PollResponse.Builder responseBuilder = SinkProto.PollResponse.newBuilder();
            if (updatedRecords != null && !updatedRecords.isEmpty()) {
                responseBuilder.addAllRecords(updatedRecords);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Polling thread was interrupted for table: " + fullTableName, e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription("Server polling was interrupted")
                    .asRuntimeException());
        } catch (Exception e) {
            LOGGER.error("An unexpected error occurred while polling for table: " + fullTableName, e);
            responseObserver.onError(io.grpc.Status.UNKNOWN
                    .withDescription("An unexpected error occurred: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}