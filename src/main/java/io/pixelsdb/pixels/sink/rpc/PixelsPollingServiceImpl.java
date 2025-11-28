package io.pixelsdb.pixels.sink.rpc;

import io.grpc.stub.StreamObserver;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.writer.FlinkPollingWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 实现了 .proto 文件中定义的 PixelsPollingService 服务。
 * 它处理来自客户端的 PollRequest，并从 FlinkPollingWriter 中拉取数据进行响应。
 */
public class PixelsPollingServiceImpl {

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

        // 从全局配置中获取轮询参数
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        // 使用 sink.commit.batch.size 作为轮询的批次大小
        this.pollBatchSize = config.getCommitBatchSize();
        // 使用 sink.timeout.ms 作为轮询的超时时间
        this.pollTimeoutMs = config.getTimeoutMs();
        LOGGER.info("PixelsPollingServiceImpl initialized. Using 'sink.commit.batch.size' for pollBatchSize ({}) " +
                        "and 'sink.timeout.ms' for pollTimeoutMs ({}).",
                this.pollBatchSize, this.pollTimeoutMs);
    }

    @Override
    public void pollEvents(PollRequest request, StreamObserver<PollResponse> responseObserver) {
        // 1. 从请求中组合出完整的表名，这与 FlinkPollingWriter 中的 key 格式保持一致
        String fullTableName = request.getSchemaName() + "." + request.getTableName();

        LOGGER.debug("Received poll request for table '{}'", fullTableName);

        try {
            // 2. 调用 writer 的 pollRecords 方法。
            //    由于 RowRecord 定义已统一，返回的 List<RowRecord> 类型与 RPC 响应所需的类型完全一致。
            List<RowRecord> records = writer.pollRecords(
                    fullTableName,
                    pollBatchSize,
                    pollTimeoutMs,
                    TimeUnit.MILLISECONDS
            );

            // 3. 构建 gRPC 响应。
            PollResponse.Builder responseBuilder = PollResponse.newBuilder();

            // 4. ***核心优化***: 直接将记录列表添加到响应构建器中。
            //    不再需要遍历和进行类型转换，代码更简洁，性能更高。
            if (records != null && !records.isEmpty()) {
                responseBuilder.addAllRecords(records);
            }

            // 5. 发送响应给客户端
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
