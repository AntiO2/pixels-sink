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

public abstract class AbstractMemorySinkStorageSource extends AbstractSinkStorageSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMemorySinkStorageSource.class);

    // All preloaded records, order preserved
    // key + value buffer
    private final List<StorageSourceRecord<ByteBuffer>> preloadedRecords = new ArrayList<>();

    @Override
    public void start()
    {
        recoveryManager.initializeForSourceStart();
        StorageSourceOffset replayStartOffset = recoveryManager.getReplayStartOffset();
        if (replayStartOffset != null)
        {
            this.loopId = replayStartOffset.getEpoch();
        }
        this.running.set(true);
        this.transactionProcessorThread.start();
        this.transactionProviderThread.start();
        try
        {
            /* =====================================================
             * 1. Initialization phase: preload all ByteBuffers
             * ===================================================== */
            for (int fileId = 0; fileId < files.size(); fileId++)
            {
                String file = files.get(fileId);
                Storage.Scheme scheme = Storage.Scheme.fromPath(file);
                LOGGER.info("Preloading file {}", file);

                PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(scheme, file);
                readers.add(reader);
                long offset = 0;

                while (true)
                {
                    int key;
                    int valueLen;
                    long recordOffset;

                    try
                    {
                        recordOffset = offset;
                        key = reader.readInt(ByteOrder.BIG_ENDIAN);
                        valueLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                    } catch (IOException eof)
                    {
                        // Reached end of file
                        break;
                    }
                    // Synchronous read and copy to heap buffer
                    ByteBuffer valueBuffer = reader.readFully(valueLen);
                    ByteBuffer duplicateValueBuffer = valueBuffer.duplicate();
                    duplicateValueBuffer.rewind();
                    ByteBuffer cleanBuffer = ByteBuffer.allocate(valueLen);
                    cleanBuffer.put(duplicateValueBuffer);
                    cleanBuffer.flip();
                    offset += Integer.BYTES * 2L + valueLen;
                    preloadedRecords.add(new StorageSourceRecord<>(
                            key,
                            cleanBuffer,
                            new StorageSourceOffset(fileId, recordOffset, 0, getProtoType(key))
                    ));
                }
            }

            LOGGER.info("Preload finished, total records = {}", preloadedRecords.size());

            /* =====================================================
             * Phase 2: Runtime loop
             * Queue initialization, consumer startup, and feeding
             * are done together in this phase
             * ===================================================== */
            do
            {
                for (StorageSourceRecord<ByteBuffer> record : preloadedRecords)
                {
                    ByteBuffer src = record.getPayload();
                    StorageSourceOffset offset = record.getOffset();
                    if (replayStartOffset != null && offset.getEpoch() == 0)
                    {
                        StorageSourceOffset currentLoopOffset = new StorageSourceOffset(
                                offset.getFileId(),
                                offset.getByteOffset(),
                                loopId,
                                offset.getRecordType()
                        );
                        if (currentLoopOffset.compareTo(replayStartOffset) < 0)
                        {
                            continue;
                        }
                    }
                    int key = record.getSourceKey();
                    ByteBuffer duplicate = src.duplicate();
                    duplicate.rewind();
                    ByteBuffer copy = ByteBuffer.allocate(duplicate.remaining());
                    copy.put(duplicate);
                    copy.flip();
                    // Lazily create queue
                    BlockingQueue<StorageSourceRecord<CompletableFuture<ByteBuffer>>> queue =
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
                    if (protoType == ProtoType.ROW)
                    {
                        sourceRateLimiter.acquire(1);
                    }

                    // Use completed future to keep consumer logic unchanged
                    CompletableFuture<ByteBuffer> future = CompletableFuture.completedFuture(copy);
                    StorageSourceOffset currentOffset = new StorageSourceOffset(
                            offset.getFileId(),
                            offset.getByteOffset(),
                            loopId,
                            offset.getRecordType()
                    );
                    queue.put(new StorageSourceRecord<>(key, future, currentOffset));
                }
                ++loopId;
            } while (storageLoopEnabled && isRunning());
        } catch (IOException | IndexOutOfBoundsException e)
        {
            throw new RuntimeException(e);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        } finally
        {
            clean();
        }
    }

}
