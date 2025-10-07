/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.concurrent.TransactionMode;
import io.pixelsdb.pixels.sink.deserializer.RowChangeEventJsonDeserializer;
import io.pixelsdb.pixels.sink.sink.PixelsSinkMode;
import io.pixelsdb.pixels.sink.sink.RetinaWriter;
import lombok.Getter;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.util.Objects;

@Getter
public class PixelsSinkConfig
{
    private final ConfigFactory config;

    @ConfigKey(value = "transaction.timeout", defaultValue = TransactionConfig.DEFAULT_TRANSACTION_TIME_OUT)
    private Long transactionTimeout;

    @ConfigKey(value = "sink.mode", defaultValue = PixelsSinkDefaultConfig.SINK_MODE)
    private PixelsSinkMode pixelsSinkMode;

    @ConfigKey(value = "sink.retina.mode", defaultValue = PixelsSinkDefaultConfig.SINK_RETINA_MODE)
    private RetinaWriter.RetinaWriteMode retinaWriteMode;

    @ConfigKey(value = "sink.trans.mode", defaultValue = TransactionConfig.DEFAULT_TRANSACTION_MODE)
    private TransactionMode transactionMode;

    @ConfigKey(value = "sink.remote.port", defaultValue = "9090")
    private short remotePort;

    @ConfigKey(value = "sink.batch.size", defaultValue = "5000")
    private int batchSize;

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

    @ConfigKey(value = "sink.rpc.enable", defaultValue = "false")
    private boolean rpcEnable;

    @ConfigKey(value = "sink.rpc.mock.delay", defaultValue = "0")
    private int mockRpcDelay;

    @ConfigKey(value = "sink.trans.batch.size", defaultValue = "100")
    private int transBatchSize;

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

    @ConfigKey(value = "sink.proto.dir")
    private String sinkProtoDir;
    @ConfigKey(value = "sink.proto.data", defaultValue = "data")
    private String sinkProtoData;

    @ConfigKey(value = "sink.proto.maxRecords", defaultValue = PixelsSinkDefaultConfig.MAX_RECORDS_PER_FILE)
    private int maxRecordsPerFile;

    public PixelsSinkConfig(String configFilePath) throws IOException
    {
        this.config = ConfigFactory.Instance();
        this.config.loadProperties(configFilePath);
        ConfigLoader.load(this.config.extractPropertiesByPrefix("", false), this);
    }

    public PixelsSinkConfig(ConfigFactory config)
    {
        this.config = config;
        ConfigLoader.load(this.config.extractPropertiesByPrefix("", false), this);
    }

    public String[] getIncludeTables() {
        return includeTablesRaw.isEmpty() ? new String[0] : includeTablesRaw.split(",");
    }

}