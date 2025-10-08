package io.pixelsdb.pixels.sink.processor;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.ProtoType;
import io.pixelsdb.pixels.sink.util.EtcdFileRegistry;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractSinkStorageProcessor
{
    protected final AtomicBoolean running = new AtomicBoolean(false);

    protected final String topic;
    protected final String baseDir;
    protected final EtcdFileRegistry etcdFileRegistry;
    protected final List<String> files;

    protected final BlockingQueue<ByteBuffer> rawTransactionQueue = new LinkedBlockingQueue<>(10000);

    protected AbstractSinkStorageProcessor()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.topic = pixelsSinkConfig.getSinkProtoData();
        this.baseDir = pixelsSinkConfig.getSinkProtoDir();
        this.etcdFileRegistry = new EtcdFileRegistry(topic, baseDir);
        this.files = this.etcdFileRegistry.listAllFiles();
    }
    protected void handleTransactionSourceRecord(ByteBuffer sourceRecord) throws InterruptedException
    {
        rawTransactionQueue.put(sourceRecord);
    }

    abstract ProtoType getProtoType(int i);
}
