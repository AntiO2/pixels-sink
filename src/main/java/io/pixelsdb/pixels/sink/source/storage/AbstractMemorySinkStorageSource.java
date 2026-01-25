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

package io.pixelsdb.pixels.sink.source.storage;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class AbstractMemorySinkStorageSource extends AbstractSinkStorageSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMemorySinkStorageSource.class);

    // All preloaded records, order preserved
    // key + value buffer
    private final List<Pair<Integer, ByteBuffer>> preloadedRecords = new ArrayList<>();

    @Override
    public void start() {
        this.running.set(true);
        this.transactionProcessorThread.start();
        this.transactionProviderThread.start();
        try {
            /* =====================================================
             * 1. Initialization phase: preload all ByteBuffers
             * ===================================================== */
            for (String file : files) {
                Storage.Scheme scheme = Storage.Scheme.fromPath(file);
                LOGGER.info("Preloading file {}", file);

                PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(scheme, file);
                readers.add(reader);

                while (true) {
                    int key;
                    int valueLen;

                    try {
                        key = reader.readInt(ByteOrder.BIG_ENDIAN);
                        valueLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                    } catch (IOException eof) {
                        // Reached end of file
                        break;
                    }
                    // Synchronous read and copy to heap buffer
                    ByteBuffer valueBuffer = reader.readFully(valueLen);
                    // Store into a single global array
                    ByteBuffer cleanBuffer = valueBuffer.duplicate();
                    cleanBuffer.rewind();
                    cleanBuffer.limit(cleanBuffer.position() + valueLen);
                    preloadedRecords.add(new Pair<>(key, cleanBuffer));
                }
            }

            LOGGER.info("Preload finished, total records = {}", preloadedRecords.size());

            /* =====================================================
             * Phase 2: Runtime loop
             * Queue initialization, consumer startup, and feeding
             * are done together in this phase
             * ===================================================== */
            do {
                for (Pair<Integer, ByteBuffer> record : preloadedRecords) {
                    int key = record.getLeft();
                    ByteBuffer src = record.getRight();
                    ByteBuffer copy = ByteBuffer.allocate(src.remaining());
                    copy.put(src.duplicate().rewind());
                    copy.flip();
                    // Lazily create queue
                    BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>> queue =
                            queueMap.computeIfAbsent(
                                    key,
                                    k -> new LinkedBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE)
                            );

                    // Lazily start consumer thread
                    consumerThreads.computeIfAbsent(key, k ->
                    {
                        ProtoType protoType = getProtoType(k);
                        Thread t = new Thread(() -> consumeQueue(k, queue, protoType));
                        t.setName("consumer-" + k);
                        t.start();
                        return t;
                    });

                    ProtoType protoType = getProtoType(key);
                    if (protoType == ProtoType.ROW) {
                        sourceRateLimiter.acquire(1);
                    }

                    // Use completed future to keep consumer logic unchanged
                    CompletableFuture<ByteBuffer> future =
                            CompletableFuture.completedFuture(copy);

                    queue.put(new Pair<>(future, loopId));
                }
                ++loopId;
            } while (storageLoopEnabled && isRunning());
        } catch (IOException | IndexOutOfBoundsException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            clean();
        }
    }
}
