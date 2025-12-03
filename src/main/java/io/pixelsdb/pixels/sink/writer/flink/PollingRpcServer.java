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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 负责管理 gRPC Server 的生命周期（启动、关闭）。
 * 这个类的实现与具体的 .proto 服务定义解耦。
 */
public class PollingRpcServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PollingRpcServer.class);
    private final Server server;
    private final int port;

    /**
     * 构造函数。
     * @param serviceImpl gRPC 服务的实现实例
     * @param port        服务器监听的端口
     */
    public PollingRpcServer(PixelsPollingServiceImpl serviceImpl, int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(serviceImpl) // 将具体的服务实现绑定到服务器
                .build();
    }

    /**
     * 启动 gRPC 服务器。
     * @throws IOException 如果端口绑定失败
     */
    public void start() throws IOException {
        server.start();
        LOGGER.info("gRPC Polling Server started, listening on port " + port);
    }

    /**
     * 平滑地关闭服务器。
     */
    public void stop() {
        LOGGER.info("Attempting to shut down gRPC Polling Server...");
        if (server != null) {
            try {
                if (!server.isTerminated()) {
                    server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                LOGGER.error("gRPC server shutdown interrupted.", e);
                Thread.currentThread().interrupt();
            } finally {
                if (!server.isTerminated()) {
                    LOGGER.warn("gRPC server did not terminate gracefully. Forcing shutdown.");
                    server.shutdownNow();
                }
            }
        }
        LOGGER.info("gRPC Polling Server shut down.");
    }

    /**
     * 阻塞当前线程，直到 gRPC 服务器关闭。
     * 通常在主线程中调用，以防止应用退出。
     */
    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
