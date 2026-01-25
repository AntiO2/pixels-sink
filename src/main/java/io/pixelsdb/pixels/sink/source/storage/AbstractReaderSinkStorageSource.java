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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class AbstractReaderSinkStorageSource extends AbstractSinkStorageSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractReaderSinkStorageSource.class);

    @Override
    public void start()
    {
        this.running.set(true);
        this.transactionProcessorThread.start();
        this.transactionProviderThread.start();
        for (String file : files)
        {
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
        do
        {
            for (PhysicalReader reader : readers)
            {
                LOGGER.info("Start Read {}", reader.getPath());
                long offset = 0;
                while (true)
                {
                    try
                    {
                        int key, valueLen;
                        reader.seek(offset);
                        try
                        {
                            key = reader.readInt(ByteOrder.BIG_ENDIAN);
                            valueLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                        } catch (IOException e)
                        {
                            // EOF
                            break;
                        }

                        ProtoType protoType = getProtoType(key);
                        offset += Integer.BYTES * 2;
                        CompletableFuture<ByteBuffer> valueFuture = reader.readAsync(offset, valueLen)
                                .thenApply(this::copyToHeap)
                                .thenApply(buf -> buf.order(ByteOrder.BIG_ENDIAN));
                        // move offset for next record
                        offset += valueLen;


                        // Get or create queue
                        BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>> queue =
                                queueMap.computeIfAbsent(key,
                                        k -> new LinkedBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE));

                        // Put future in queue
                        if (protoType.equals(ProtoType.ROW))
                        {
                            sourceRateLimiter.acquire(1);
                        }
                        queue.put(new Pair<>(valueFuture, loopId));
                        // Start consumer thread if not exists
                        consumerThreads.computeIfAbsent(key, k ->
                        {
                            Thread t = new Thread(() -> consumeQueue(k, queue, protoType));
                            t.setName("consumer-" + key);
                            t.start();
                            return t;
                        });
                    } catch (IOException | InterruptedException e)
                    {
                        break;
                    }
                }
            }
            ++loopId;
        } while (storageLoopEnabled && isRunning());

        clean();
    }
}
