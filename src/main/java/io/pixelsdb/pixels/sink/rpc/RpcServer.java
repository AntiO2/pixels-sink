package io.pixelsdb.pixels.sink.rpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.writer.FlinkPollingWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * gRPC Server 启动类.
 * 负责初始化和管理 PixelsPollingService 的生命周期.
 */
public class RpcServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServer.class);

    private Server server;
    private final int port;
    private final FlinkPollingWriter writer;

    /**
     * 构造函数.
     * @param port The port to listen on.
     * @param writer The writer instance to be used by the service.
     */
    public RpcServer(int port, FlinkPollingWriter writer) {
        this.port = port;
        this.writer = writer;
    }

    /**
     * 启动 gRPC 服务.
     * @throws IOException if server fails to start.
     */
    public void start() throws IOException {
        // 使用 ServerBuilder 构建服务器实例
        this.server = ServerBuilder.forPort(port)
                // 将我们的服务实现添加到服务器
                .addService(new PixelsPollingServiceImpl(writer))
                .build()
                .start();

        LOGGER.info("gRPC Server started, listening on port " + port);

        // 添加 JVM 关闭钩子，用于优雅关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("*** Shutting down gRPC server since JVM is shutting down ***");
            try {
                this.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace(System.err);
            }
            LOGGER.info("*** Server shut down ***");
        }));
    }

    /**
     * 优雅地关闭服务器.
     * @throws InterruptedException if shutdown is interrupted.
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            // 等待最多30秒让现有请求处理完成
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * 阻塞主线程直到服务器被终止.
     * @throws InterruptedException if waiting is interrupted.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * 主函数，程序的入口.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        // 1. 初始化配置 (这是关键的第一步)
        // 假设您的配置工厂可以无参数初始化，或者从环境变量/默认路径加载
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

        // 2. 从配置中获取 RPC 服务的端口号
        // 我们需要一个专门为 RPC 服务定义的端口配置，这里我们复用 'sink.remote.port'
        int rpcPort = config.getRemotePort();

        // 3. 初始化 FlinkPollingWriter (这是服务的核心依赖)
        // 注意：这里的初始化方式取决于 FlinkPollingWriter 的设计
        // 这是一个示例，您需要根据实际情况调整
        FlinkPollingWriter flinkWriter = FlinkPollingWriter.getInstance(); // 假设它也是单例模式

        // 4. 创建并启动 RPC 服务器
        final RpcServer rpcServer = new RpcServer(rpcPort, flinkWriter);
        rpcServer.start();

        // 5. 阻塞主线程
        rpcServer.blockUntilShutdown();
    }
}
