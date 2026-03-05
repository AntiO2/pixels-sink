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

package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.event.deserializer.RowChangeEventJsonDeserializer;
import io.pixelsdb.pixels.sink.writer.PixelsSinkMode;
import io.pixelsdb.pixels.sink.writer.retina.RetinaServiceProxy;
import io.pixelsdb.pixels.sink.writer.retina.TransactionMode;
import lombok.Getter;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.List;

@Getter
public class PixelsSinkConfig
{
    private final ConfigFactory config;

    @ConfigKey(value = "transaction.timeout", defaultValue = TransactionConfig.DEFAULT_TRANSACTION_TIME_OUT)
    private Long transactionTimeout;

    @ConfigKey(value = "sink.mode", defaultValue = PixelsSinkDefaultConfig.SINK_MODE)
    private PixelsSinkMode pixelsSinkMode;

    @ConfigKey(value = "sink.retina.mode", defaultValue = PixelsSinkDefaultConfig.SINK_RETINA_MODE)
    private RetinaServiceProxy.RetinaWriteMode retinaWriteMode;

    @ConfigKey(value = "sink.retina.client", defaultValue = "1")
    private int retinaClientNum;

    @ConfigKey(value = "sink.retina.log.queue", defaultValue = "true")
    private boolean retinaLogQueueEnabled;

    @ConfigKey(value = "sink.retina.rpc.limit", defaultValue = "1000")
    private int retinaRpcLimit;

    @ConfigKey(value = "sink.retina.trans.limit", defaultValue = "1000")
    private int retinaTransLimit;

    @ConfigKey(value = "sink.trans.mode", defaultValue = TransactionConfig.DEFAULT_TRANSACTION_MODE)
    private TransactionMode transactionMode;

    @ConfigKey(value = "sink.commit.method", defaultValue = "async")
    private String commitMethod;

    @ConfigKey(value = "sink.commit.batch.size", defaultValue = "500")
    private int commitBatchSize;

    @ConfigKey(value = "sink.commit.batch.worker", defaultValue = "16")
    private int commitBatchWorkers;

    @ConfigKey(value = "sink.commit.batch.delay", defaultValue = "200")
    private int commitBatchDelay;

    @ConfigKey(value = "sink.remote.port", defaultValue = "9090")
    private short remotePort;

    @ConfigKey(value = "sink.flink.server.port", defaultValue = "9091")
    private int sinkFlinkServerPort;

    @ConfigKey(value = "sink.timeout.ms", defaultValue = "30000")
    private int timeoutMs;

    @ConfigKey(value = "sink.flush.interval.ms", defaultValue = "1000")
    private int flushIntervalMs;

    @ConfigKey(value = "sink.flush.batch.size", defaultValue = "100")
    private int flushBatchSize;

    @ConfigKey(value = "sink.max.retries", defaultValue = "3")
    private int maxRetries;

    @ConfigKey(value = "sink.csv.enable_header", defaultValue = "false")
    private boolean sinkCsvEnableHeader;

    @ConfigKey(value = "sink.monitor.enable", defaultValue = "false")
    private boolean monitorEnabled;

    @ConfigKey(value = "sink.monitor.port", defaultValue = "9464")
    private short monitorPort;

    @ConfigKey(value = "sink.monitor.report.enable", defaultValue = "true")
    private boolean monitorReportEnabled;

    @ConfigKey(value = "sink.monitor.report.interval", defaultValue = "5000")
    private short monitorReportInterval;

    @ConfigKey(value = "sink.monitor.freshness.interval", defaultValue = "1000")
    private int freshnessReportInterval;

    @ConfigKey(value = "sink.monitor.freshness.file", defaultValue = "/tmp/sinkFreshness.csv")
    private String monitorFreshnessReportFile;

    @ConfigKey(value = "sink.monitor.report.file", defaultValue = "/tmp/sink.csv")
    private String monitorReportFile;

    @ConfigKey(value = "sink.rpc.enable", defaultValue = "false")
    private boolean rpcEnable;

    @ConfigKey(value = "sink.rpc.mock.delay", defaultValue = "0")
    private int mockRpcDelay;

    @ConfigKey(value = "sink.trans.batch.size", defaultValue = "100")
    private int transBatchSize;

    @ConfigKey(value = "sink.retina.trans.request.batch", defaultValue = "false")
    private boolean retinaTransRequestBatch;

    @ConfigKey(value = "sink.retina.trans.request.batch.size", defaultValue = "100")
    private int retinaTransRequestBatchSize;

    private boolean retinaEmbedded = false;

    @ConfigKey("topic.prefix")
    private String topicPrefix;
    @ConfigKey("debezium.topic.prefix")
    private String debeziumTopicPrefix;

    @ConfigKey("consumer.capture_database")
    private String captureDatabase;

    @ConfigKey(value = "consumer.include_tables", defaultValue = "")
    private String includeTablesRaw;

    @ConfigKey("bootstrap.servers")
    private String bootstrapServers;

    @ConfigKey("group.id")
    private String groupId;

    @ConfigKey(value = "key.deserializer", defaultClass = StringDeserializer.class)
    private String keyDeserializer;

    @ConfigKey(value = "value.deserializer", defaultClass = RowChangeEventJsonDeserializer.class)
    private String valueDeserializer;

    @ConfigKey(value = "sink.csv.path", defaultValue = PixelsSinkDefaultConfig.CSV_SINK_PATH)
    private String csvSinkPath;

    @ConfigKey(value = "transaction.topic.suffix", defaultValue = TransactionConfig.DEFAULT_TRANSACTION_TOPIC_SUFFIX)
    private String transactionTopicSuffix;

    @ConfigKey(value = "transaction.topic.value.deserializer",
            defaultClass = RowChangeEventJsonDeserializer.class)
    private String transactionTopicValueDeserializer;

    @ConfigKey(value = "transaction.topic.group_id",
            defaultValue = TransactionConfig.DEFAULT_TRANSACTION_TOPIC_GROUP_ID)
    private String transactionTopicGroupId;

    @ConfigKey(value = "sink.remote.host", defaultValue = PixelsSinkDefaultConfig.SINK_REMOTE_HOST)
    private String sinkRemoteHost;

    @ConfigKey("sink.registry.url")
    private String registryUrl;

    @ConfigKey(value = "sink.datasource", defaultValue = PixelsSinkDefaultConfig.DATA_SOURCE)
    private String dataSource;

    @ConfigKey(value = "sink.datasource.rate.limit", defaultValue = "-1")
    private int sourceRateLimit;

    @ConfigKey(value = "sink.datasource.rate.limit.type", defaultValue = "-1")
    private String rateLimiterType;

    private boolean enableSourceRateLimit;

    @ConfigKey(value = "sink.proto.dir")
    private String sinkProtoDir;
    @ConfigKey(value = "sink.proto.data", defaultValue = "data")
    private String sinkProtoData;

    @ConfigKey(value = "sink.proto.maxRecords", defaultValue = PixelsSinkDefaultConfig.MAX_RECORDS_PER_FILE)
    private int maxRecordsPerFile;

    @ConfigKey(value = "sink.storage.loop", defaultValue = "false")
    private boolean sinkStorageLoop;

    @ConfigKey(value = "sink.monitor.freshness.level", defaultValue = "row") // row or txn or embed
    private String sinkMonitorFreshnessLevel;
    @ConfigKey(value = "sink.monitor.freshness.embed.warmup", defaultValue = "10")
    private Integer sinkMonitorFreshnessEmbedWarmupSeconds;

    @ConfigKey(value = "sink.monitor.freshness.embed.static", defaultValue = "false")
    private boolean sinkMonitorFreshnessEmbedStatic;

    @ConfigKey(value = "sink.monitor.freshness.embed.snapshot", defaultValue = "false")
    private boolean sinkMonitorFreshnessEmbedSnapshot;

    @ConfigKey(value = "sink.monitor.freshness.embed.tablelist", defaultValue = "")
    private List<String> sinkMonitorFreshnessEmbedTableList;

    @ConfigKey(value = "sink.monitor.freshness.embed.delay", defaultValue = "0")
    private Integer sinkMonitorFreshnessEmbedDelay;

    @ConfigKey(value = "sink.monitor.freshness.verbose", defaultValue = "false")
    private boolean sinkMonitorFreshnessVerbose;

    @ConfigKey(value = "sink.monitor.freshness.timestamp", defaultValue = "false")
    private boolean sinkMonitorFreshnessTimestamp;

    @ConfigKey(value = "trino.url")
    private String trinoUrl;

    @ConfigKey(value = "trino.user")
    private String trinoUser;

    @ConfigKey(value = "trino.password")
    private String trinoPassword;

    @ConfigKey(value = "trino.parallel", defaultValue = "1")
    private int trinoParallel;

    public PixelsSinkConfig(String configFilePath) throws IOException
    {
        this.config = ConfigFactory.Instance();
        this.config.loadProperties(configFilePath);
        init();
    }

    public PixelsSinkConfig(ConfigFactory config)
    {
        this.config = config;
        init();
    }

    public String[] getIncludeTables()
    {
        return includeTablesRaw.isEmpty() ? new String[0] : includeTablesRaw.split(",");
    }

    private void init()
    {
        ConfigLoader.load(this.config.extractPropertiesByPrefix("", false), this);

        this.enableSourceRateLimit = this.sourceRateLimit >= 0;
    }
}