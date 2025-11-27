package io.pixelsdb.pixels.sink.writer;

//import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.util.FlushRateLimiter; // 引入 RateLimiter
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * FlinkPollingWriter 是一个为长轮询(long-polling)模式设计的 PixelsSinkWriter 实现。
 * 它维护了按表名区分的内存阻塞队列，作为上游数据源(生产者)和 gRPC 服务(消费者)之间的缓冲区。
 * 这个类是线程安全的，并集成了 FlushRateLimiter 来控制入口流量。
 */
public class FlinkPollingWriter implements PixelsSinkWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkPollingWriter.class);

    // 核心数据结构：一个线程安全的 Map，其中每个表名对应一个线程安全的阻塞队列。
    private final Map<String, BlockingQueue<SinkProto.RowRecord>> tableQueues;

    // 入口速率限制器，用于在数据写入队列前进行限流。
    private final FlushRateLimiter rateLimiter;

    public FlinkPollingWriter() {
        this.tableQueues = new ConcurrentHashMap<>();
        // 获取全局的 RateLimiter 单例
        this.rateLimiter = FlushRateLimiter.getInstance();
        LOGGER.info("FlinkPollingWriter initialized with FlushRateLimiter.");
    }

    /**
     * [生产者侧] 接收来自数据源的行变更事件，进行限流，然后转换并放入内存队列。
     *
     * @param event 行变更事件
     * @return 总是返回 true，除非发生中断异常
     */
    @Override
    public boolean writeRow(RowChangeEvent event) {
        if (event == null) {
            LOGGER.warn("Received a null RowChangeEvent, skipping.");
            return false;
        }

        try {
            // 1. ***新增***: 在处理任何数据之前，首先获取一个令牌以遵循速率限制。
            //    这是一个阻塞操作，它将有效地控制上游的写入速度。
            rateLimiter.acquire(1);

            // 2. 将 Flink 的 RowChangeEvent 转换为 gRPC 使用的 RowRecord Protobuf 对象
            SinkProto.RowRecord rowRecord = convertToRowRecord(event);

            // 3. 根据表名找到对应的队列，如果不存在则原子性地创建一个新的。
            BlockingQueue<SinkProto.RowRecord> queue = tableQueues.computeIfAbsent(
                    event.getFullTableName(),
                    k -> new LinkedBlockingQueue<>() // 默认使用无界队列
            );

            // 4. 将转换后的记录放入队列。
            queue.put(rowRecord);

            LOGGER.debug("Enqueued a row for table '{}'. Queue size is now {}.", event.getFullTableName(), queue.size());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 恢复中断状态
            LOGGER.error("Thread was interrupted while writing row for table: " + event.getFullTableName(), e);
            return false;
        } catch (Exception e) {
            LOGGER.error("Failed to process and write row for table: " + event.getFullTableName(), e);
            return false;
        }

        return true;
    }

    /**
     * [消费者侧] gRPC 服务将调用此方法来拉取数据。
     * 实现了长轮询逻辑：如果队列为空，它会阻塞等待指定的时间。
     * batchSize 仍然作为拉取数据批次的上限，防止单次 RPC 响应过大。
     *
     * @param tableName  要拉取数据的表名
     * @param batchSize  最大拉取记录数
     * @param timeout    等待数据的最大时长
     * @param unit       等待时长的单位
     * @return 一个 RowRecord 列表，如果超时或没有数据则为空列表
     * @throws InterruptedException 如果在等待时线程被中断
     */
    public List<SinkProto.RowRecord> pollRecords(String tableName, int batchSize, long timeout, TimeUnit unit)
            throws InterruptedException {
        List<SinkProto.RowRecord> records = new ArrayList<>(batchSize);
        BlockingQueue<SinkProto.RowRecord> queue = tableQueues.get(tableName);

        if (queue == null) {
            unit.sleep(timeout);
            return records;
        }

        SinkProto.RowRecord firstRecord = queue.poll(timeout, unit);
        if (firstRecord == null) {
            return records;
        }

        records.add(firstRecord);
        queue.drainTo(records, batchSize - 1);

        LOGGER.info("Polled {} records for table '{}'.", records.size(), tableName);
        return records;
    }

    /**
     * 辅助方法：将内部的 RowChangeEvent 转换为 Protobuf 定义的 RowRecord。
     * 由于 event 已经包含了 RowValue，此方法现在非常简单。
     */
    private SinkProto.RowRecord convertToRowRecord(RowChangeEvent event) {
        SinkProto.RowRecord.Builder recordBuilder = SinkProto.RowRecord.newBuilder();

        // 直接从 event 获取已经转换好的 RowValue
        if (event.getBefore() != null) {
            recordBuilder.setBefore(event.getBefore());
        }

        // 直接从 event 获取已经转换好的 RowValue
        if (event.getAfter() != null) {
            recordBuilder.setAfter(event.getAfter());
        }

        // 3. 转换操作类型 (这部分逻辑不变)
        SinkProto.OperationType opType = convertToOperationType(event.getOp());
        recordBuilder.setOp(opType);

        return recordBuilder.build();
    }

    /**
     * 辅助方法：将 RowChangeEvent 的操作类型映射到 Protobuf 的 OperationType 枚举。
     */
    private SinkProto.OperationType convertToOperationType(RowChangeEvent.Op op) {
        switch (op) {
            case INSERT:
                return SinkProto.OperationType.INSERT;
            case UPDATE:
                return SinkProto.OperationType.UPDATE;
            case DELETE:
                return SinkProto.OperationType.DELETE;
            default:
                LOGGER.warn("Unknown RowChangeEvent.Op type: {}. Defaulting to UNRECOGNIZED.", op);
                // 使用 UNRECOGNIZED 更符合 Protobuf 3 的实践
                return SinkProto.OperationType.UNRECOGNIZED;
        }
    }


    /**
     * 本实现不涉及事务，此方法为空实现。
     */
    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata) {
        return true;
    }

    /**
     * 本实现是内存队列，数据实时可见，flush 为空实现。
     */
    @Override
    public void flush() {
        // No-op
    }

    /**
     * 关闭时清理资源。
     */
    @Override
    public void close() throws IOException {
        LOGGER.info("Closing FlinkPollingWriter. Clearing all table queues.");
        tableQueues.clear();
    }
}
