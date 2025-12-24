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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PollingRpcServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PollingRpcServer.class);
    private final Server server;
    private final int port;

    public PollingRpcServer(PixelsPollingServiceImpl serviceImpl, int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                .addService(serviceImpl) // 将具体的服务实现绑定到服务器
                .build();
    }

    public void start() throws IOException {
        server.start();
        LOGGER.info("gRPC Polling Server started, listening on port " + port);
    }

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

    public void awaitTermination() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
