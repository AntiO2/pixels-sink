package io.pixelsdb.pixels.sink.source;

import com.google.common.util.concurrent.RateLimiter;
import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.provider.TableProviderAndProcessorPipelineManager;
import io.pixelsdb.pixels.sink.provider.TransactionEventStorageProvider;
import io.pixelsdb.pixels.sink.util.EtcdFileRegistry;
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

public abstract class AbstractSinkStorageSource implements SinkSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSinkStorageSource.class);
    protected final AtomicBoolean running = new AtomicBoolean(false);

    protected final String topic;
    protected final String baseDir;
    protected final EtcdFileRegistry etcdFileRegistry;
    protected final List<String> files;
    protected final CompletableFuture<ByteBuffer> POISON_PILL = new CompletableFuture<>();
    private final Map<Integer, Thread> consumerThreads = new ConcurrentHashMap<>();
    private final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    private final Map<Integer, BlockingQueue<CompletableFuture<ByteBuffer>>> queueMap = new ConcurrentHashMap<>();
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final TableProviderAndProcessorPipelineManager<ByteBuffer> tablePipelineManager = new TableProviderAndProcessorPipelineManager<ByteBuffer>();
    private final boolean storageLoopEnabled;
    private final int MAX_QUEUE_SIZE = 10_000;
    protected TransactionEventStorageProvider<ByteBuffer> transactionEventProvider;
    protected TransactionProcessor transactionProcessor;
    protected Thread transactionProviderThread;
    protected Thread transactionProcessorThread;

    protected AbstractSinkStorageSource() {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.topic = pixelsSinkConfig.getSinkProtoData();
        this.baseDir = pixelsSinkConfig.getSinkProtoDir();
        this.etcdFileRegistry = new EtcdFileRegistry(topic, baseDir);
        this.files = this.etcdFileRegistry.listAllFiles();
        this.storageLoopEnabled = pixelsSinkConfig.isSinkStorageLoop();

        this.transactionEventProvider = new TransactionEventStorageProvider<>();
        this.transactionProviderThread = new Thread(transactionEventProvider);

        this.transactionProcessor = new TransactionProcessor(transactionEventProvider);
        this.transactionProcessorThread = new Thread(transactionProcessor, "debezium-processor");
    }

    abstract ProtoType getProtoType(int i);

    protected void handleTransactionSourceRecord(ByteBuffer record) {
        transactionEventProvider.putTransRawEvent(record);
    }

    @Override
    public void start() {
        this.running.set(true);
        this.transactionProcessorThread.start();
        this.transactionProviderThread.start();
        List<PhysicalReader> readers = new ArrayList<>();
        for (String file : files) {
            Storage.Scheme scheme = Storage.Scheme.fromPath(file);
            LOGGER.info("Start read from file {}", file);
            PhysicalReader reader = null;
            try {
                reader = PhysicalReaderUtil.newPhysicalReader(scheme, file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            readers.add(reader);
        }
        do {
            for (PhysicalReader reader : readers) {
                LOGGER.info("Start Read {}", reader.getPath());
                long offset = 0;
                while (true) {
                    try {
                        int key, valueLen;
                        reader.seek(offset);
                        try {
                            key = reader.readInt(ByteOrder.BIG_ENDIAN);
                            valueLen = reader.readInt(ByteOrder.BIG_ENDIAN);
                        } catch (IOException e) {
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
                        BlockingQueue<CompletableFuture<ByteBuffer>> queue =
                                queueMap.computeIfAbsent(key,
                                        k -> new LinkedBlockingQueue<>(MAX_QUEUE_SIZE));

                        // Put future in queue
                        queue.put(valueFuture);

                        // Start consumer thread if not exists
                        consumerThreads.computeIfAbsent(key, k ->
                        {
                            Thread t = new Thread(() -> consumeQueue(k, queue, protoType));
                            t.setName("consumer-" + key);
                            t.start();
                            return t;
                        });
                    } catch (IOException | InterruptedException e) {
                        break;
                    }
                }
            }
        } while (storageLoopEnabled && isRunning());

        // signal all queues to stop
        queueMap.values().forEach(q ->
        {
            try {
                q.put(POISON_PILL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // wait all consumers to finish
        consumerThreads.values().forEach(t ->
        {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        // close all readers
        for(PhysicalReader reader: readers)
        {
            try {
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private void consumeQueue(int key, BlockingQueue<CompletableFuture<ByteBuffer>> queue, ProtoType protoType) {
        try {
            while (true) {
                CompletableFuture<ByteBuffer> value = queue.take();
                if (value == POISON_PILL) {
                    break;
                }
                ByteBuffer valueBuffer = value.get();
                metricsFacade.recordDebeziumEvent();
                switch (protoType) {
                    case ROW -> handleRowChangeSourceRecord(key, valueBuffer);
                    case TRANS -> handleTransactionSourceRecord(valueBuffer);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOGGER.error("Error in async processing", e);
        }
    }

    private ByteBuffer copyToHeap(ByteBuffer directBuffer) {
        ByteBuffer duplicate = directBuffer.duplicate();
        ByteBuffer heapBuffer = ByteBuffer.allocate(duplicate.remaining());
        heapBuffer.put(duplicate);
        heapBuffer.flip();
        return heapBuffer;
    }

    private void handleRowChangeSourceRecord(int key, ByteBuffer dataBuffer) {
        tablePipelineManager.routeRecord(key, dataBuffer);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void stopProcessor() {
        running.set(false);
        transactionProviderThread.interrupt();
        transactionProcessorThread.interrupt();
        transactionProcessor.stopProcessor();
    }
}
