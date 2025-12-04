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
 
package io.pixelsdb.pixels.sink.source;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.provider.TableProviderAndProcessorPipelineManager;
import io.pixelsdb.pixels.sink.provider.TransactionEventStorageLoopProvider;
import io.pixelsdb.pixels.sink.provider.TransactionEventStorageProvider;
import io.pixelsdb.pixels.sink.util.EtcdFileRegistry;
import io.pixelsdb.pixels.sink.util.FlushRateLimiter;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractSinkStorageSource implements SinkSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSinkStorageSource.class);
    protected final AtomicBoolean running = new AtomicBoolean(false);

    protected final String topic;
    protected final String baseDir;
    protected final EtcdFileRegistry etcdFileRegistry;
    protected final List<String> files;
    protected final CompletableFuture<ByteBuffer> POISON_PILL = new CompletableFuture<>();
    private final Map<Integer, Thread> consumerThreads = new ConcurrentHashMap<>();
    private final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    private final Map<Integer, BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>>> queueMap = new ConcurrentHashMap<>();
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final TableProviderAndProcessorPipelineManager<Pair<ByteBuffer, Integer>> tablePipelineManager = new TableProviderAndProcessorPipelineManager<>();
    private final boolean storageLoopEnabled;
    private final int MAX_QUEUE_SIZE = 10_000;
    private final FlushRateLimiter sourceRateLimiter;
    protected TransactionEventStorageLoopProvider<Pair<ByteBuffer, Integer>> transactionEventProvider;
    protected TransactionProcessor transactionProcessor;
    protected Thread transactionProviderThread;
    protected Thread transactionProcessorThread;
    private int loopId = 0;

    protected AbstractSinkStorageSource()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.topic = pixelsSinkConfig.getSinkProtoData();
        this.baseDir = pixelsSinkConfig.getSinkProtoDir();
        this.etcdFileRegistry = new EtcdFileRegistry(topic, baseDir);
        this.files = this.etcdFileRegistry.listAllFiles();
        this.storageLoopEnabled = pixelsSinkConfig.isSinkStorageLoop();

        this.transactionEventProvider = new TransactionEventStorageLoopProvider<>();
        this.transactionProviderThread = new Thread(transactionEventProvider);

        this.transactionProcessor = new TransactionProcessor(transactionEventProvider);
        this.transactionProcessorThread = new Thread(transactionProcessor, "debezium-processor");
        this.sourceRateLimiter = FlushRateLimiter.getNewInstance();
    }

    abstract ProtoType getProtoType(int i);

    protected void handleTransactionSourceRecord(ByteBuffer record, Integer loopId)
    {
        sourceRateLimiter.acquire(1);
        transactionEventProvider.putTransRawEvent(new Pair<>(record, loopId));
    }

    @Override
    public void start()
    {
        this.running.set(true);
        this.transactionProcessorThread.start();
        this.transactionProviderThread.start();
        List<PhysicalReader> readers = new ArrayList<>();
        for (String file : files)
        {
            Storage.Scheme scheme = Storage.Scheme.fromPath(file);
            LOGGER.info("Start read from file {}", file);
            PhysicalReader reader = null;
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
                                        k -> new LinkedBlockingQueue<>(MAX_QUEUE_SIZE));

                        // Put future in queue
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

        // signal all queues to stop
        queueMap.values().forEach(q ->
        {
            try
            {
                q.put(new Pair<>(POISON_PILL, loopId));
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });

        // wait all consumers to finish
        consumerThreads.values().forEach(t ->
        {
            try
            {
                t.join();
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        });

        // close all readers
        for (PhysicalReader reader : readers)
        {
            try
            {
                reader.close();
            } catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

    }

    private void consumeQueue(int key, BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>> queue, ProtoType protoType)
    {
        try
        {
            while (true)
            {
                Pair<CompletableFuture<ByteBuffer>, Integer> pair = queue.take();
                CompletableFuture<ByteBuffer> value = pair.getLeft();
                int loopId = pair.getRight();
                if (value == POISON_PILL)
                {
                    break;
                }
                ByteBuffer valueBuffer = value.get();
                metricsFacade.recordDebeziumEvent();
                switch (protoType)
                {
                    case ROW -> handleRowChangeSourceRecord(key, valueBuffer, loopId);
                    case TRANS -> handleTransactionSourceRecord(valueBuffer, loopId);
                }
            }
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e)
        {
            LOGGER.error("Error in async processing", e);
        }
    }

    private ByteBuffer copyToHeap(ByteBuffer directBuffer)
    {
        ByteBuffer duplicate = directBuffer.duplicate();
        ByteBuffer heapBuffer = ByteBuffer.allocate(duplicate.remaining());
        heapBuffer.put(duplicate);
        heapBuffer.flip();
        return heapBuffer;
    }

    protected void handleRowChangeSourceRecord(int key, ByteBuffer dataBuffer, int loopId)
    {
        tablePipelineManager.routeRecord(key, new Pair<>(dataBuffer, loopId));
    }

    @Override
    public boolean isRunning()
    {
        return running.get();
    }

    @Override
    public void stopProcessor()
    {
        running.set(false);
        transactionProviderThread.interrupt();
        transactionProcessorThread.interrupt();
        transactionProcessor.stopProcessor();
    }
}
