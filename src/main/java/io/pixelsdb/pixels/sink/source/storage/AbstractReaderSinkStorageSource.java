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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class AbstractReaderSinkStorageSource extends AbstractSinkStorageSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractReaderSinkStorageSource.class);

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
        for (int fileId = 0; fileId < files.size(); fileId++)
        {
            String file = files.get(fileId);
            Storage.Scheme scheme = Storage.Scheme.fromPath(file);
            LOGGER.info("Start read from file {}", file);
            PhysicalReader reader;
            try
            {
                reader = PhysicalReaderUtil.newPhysicalReader(scheme, file);
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            readers.add(reader);
        }
        try
        {
            do
            {
                for (int fileId = 0; fileId < readers.size(); fileId++)
                {
                    PhysicalReader reader = readers.get(fileId);
                    LOGGER.info("Start Read {}", reader.getPath());
                    long offset = 0;
                    while (true)
                    {
                        try
                        {
                            int key;
                            int valueLen;
                            long recordOffset = offset;
                            reader.seek(offset);
                            try
                            {
                                key = reader.readInt(ByteOrder.BIG_ENDIAN);
                                valueLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                            } catch (IOException e)
                            {
                                break;
                            }

                            ProtoType protoType = getProtoType(key);
                            StorageSourceOffset currentOffset = new StorageSourceOffset(fileId, recordOffset, loopId, protoType);
                            offset += Integer.BYTES * 2L;
                            CompletableFuture<ByteBuffer> valueFuture = reader.readAsync(offset, valueLen)
                                    .thenApply(this::copyToHeap)
                                    .thenApply(buf -> buf.order(ByteOrder.BIG_ENDIAN));
                            offset += valueLen;
                            if (replayStartOffset != null && currentOffset.compareTo(replayStartOffset) < 0)
                            {
                                continue;
                            }

                            BlockingQueue<StorageSourceRecord<CompletableFuture<ByteBuffer>>> queue =
                                    queueMap.computeIfAbsent(key,
                                            k -> new LinkedBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE));
                            if (protoType.equals(ProtoType.ROW))
                            {
                                sourceRateLimiter.acquire(1);
                            }
                            queue.put(new StorageSourceRecord<>(key, valueFuture, currentOffset));
                            consumerThreads.computeIfAbsent(key, k ->
                            {
                                Thread t = new Thread(() -> consumeQueue(k, queue, protoType));
                                t.setName("consumer-" + key);
                                t.start();
                                return t;
                            });
                        } catch (InterruptedException e)
                        {
                            Thread.currentThread().interrupt();
                            return;
                        } catch (IOException e)
                        {
                            LOGGER.warn("Failed to read source file {}", reader.getPath(), e);
                            break;
                        }
                    }
                }
                ++loopId;
            }
            while (storageLoopEnabled && isRunning());
        } finally
        {
            clean();
        }
    }
}
