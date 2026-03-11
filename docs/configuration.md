# Configuration Reference

Pixels Sink is configured via a Java properties file. Pass the path with `-c`.

- Examples: `conf/pixels-sink.pg.properties`, `conf/pixels-sink.aws.properties`, `conf/pixels-sink.flink.properties`

Values are loaded by `PixelsSinkConfig` and mapped from keys in the properties file.

**Core Keys**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.datasource` | `kafka` | Source type: `engine`, `kafka`, or `storage`. |
| `sink.mode` | `csv` | Sink type: `retina`, `csv`, `proto`, `flink`, or `none`. |
| `sink.datasource.rate.limit` | `-1` | Rate limit for source ingestion. `-1` disables. |
| `sink.datasource.rate.limit.type` | `semaphore` | Rate limiter type used by `FlushRateLimiterFactory`. 'guava' or 'semaphore'|


## Source and Sink

**Transaction**

Only support **Retina** Sink Mode

| `sink.trans.batch.size` | `100` | Batch size for transaction processing. |
| `sink.trans.mode` | `batch` | Transaction mode: `single`, `record`, or `batch`. |
| `transaction.timeout` | `300` | Transaction timeout in seconds. |

Notes on `sink.trans.mode`:
- `single`: each Retina request writes exactly one transaction.
- `batch`: a single Retina request may carry multiple transactions.
- `single` and `batch` both support cross-table transactions.
- `record`: cross-table transactions are disabled; only single-table transactions are processed.



**Debezium Engine Source**

| Key | Default | Notes |
| --- | --- | --- |
| `debezium.name` | none | Engine name. |
| `debezium.connector.class` | none | Connector class, e.g. PostgreSQL connector. |
| `debezium.*` | none | Standard Debezium engine properties. |

**Retina Sink**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.retina.mode` | `stub` | Write mode: `stub` or `stream`. |
| `sink.retina.client` | `1` | Number of Retina clients per table writer. |
| `sink.retina.log.queue` | `true` | Enable queue logging. |
| `sink.retina.rpc.limit` | `1000` | Max inflight RPC requests. |
| `sink.retina.trans.limit` | `1000` | Max inflight transaction requests. |
| `sink.retina.trans.request.batch` | `false` | Enable batched transaction requests. |
| `sink.retina.trans.request.batch.size` | `100` | Batch size for transaction requests. |
| `sink.timeout.ms` | `30000` | RPC timeout. |
| `sink.flush.interval.ms` | `1000` | Flush interval. |
| `sink.flush.batch.size` | `100` | Flush batch size. |
| `sink.max.retries` | `3` | Retry limit. |
| `sink.commit.method` | `async` | Commit method: `sync` or `async`. |
| `sink.commit.batch.size` | `500` | Commit batch size. |
| `sink.commit.batch.worker` | `16` | Commit worker threads. |
| `sink.commit.batch.delay` | `200` | Commit batch delay in ms. |


**CSV Sink**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.csv.path` | `./data` | Output directory. |
| `sink.csv.enable_header` | `false` | Write header row. |

**Proto Sink and Storage Source**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.proto.dir` | required | Proto output or input directory. |
| `sink.proto.data` | `data` | Data set name. |
| `sink.proto.maxRecords` | `100000` | Max records per file. |
| `sink.storage.loop` | `false` | Whether to loop over stored files. |

**Flink Sink**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.flink.server.port` | `9091` | Polling server port. |


**Kafka Source**

*Kafka source is deprecated*

| Key | Default | Notes |
| --- | --- | --- |
| `bootstrap.servers` | required | Kafka bootstrap servers. |
| `group.id` | required | Consumer group id. |
| `auto.offset.reset` | none | Standard Kafka consumer property. |
| `key.deserializer` | `org.apache.kafka.common.serialization.StringDeserializer` | Kafka key deserializer. |
| `value.deserializer` | `io.pixelsdb.pixels.sink.event.deserializer.RowChangeEventJsonDeserializer` | Kafka value deserializer for row events. |
| `topic.prefix` | required | Topic prefix for table events. |
| `consumer.capture_database` | required | Database name used to build topic names. |
| `consumer.include_tables` | empty | Comma-separated table list, empty means all. |
| `transaction.topic.suffix` | `transaction` | Suffix appended to transaction topics. |
| `transaction.topic.value.deserializer` | `io.pixelsdb.pixels.sink.event.deserializer.RowChangeEventJsonDeserializer` | Deserializer for transaction messages. |
| `transaction.topic.group_id` | `transaction_consumer` | Consumer group for transaction topic. |
| `sink.registry.url` | required | Avro Schema registry endpoint. |

**Reserved Configuration**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.remote.host` | `localhost` | Sink server host. |
| `sink.remote.port` | `9090` | Sink server port. |
| `sink.rpc.enable` | `false` | Enable RPC simulation (for development). |
| `sink.rpc.mock.delay` | `0` | Artificial delay in ms. |

**Monitoring and Metrics**

| Key | Default | Notes |
| --- | --- | --- |
| `sink.monitor.enable` | `false` | Enable Prometheus metrics endpoint. |
| `sink.monitor.port` | `9464` | Metrics server port. |
| `sink.monitor.report.enable` | `true` | Enable report file output. |
| `sink.monitor.report.interval` | `5000` | Report interval in ms. |
| `sink.monitor.report.file` | `/tmp/sink.csv` | Report output file. |
| `sink.monitor.freshness.interval` | `1000` | Freshness report interval in ms. |
| `sink.monitor.freshness.file` | `/tmp/sinkFreshness.csv` | Freshness report output file. |
| `sink.monitor.freshness.level` | `row` | `row`, `txn`, or `embed`. |
| `sink.monitor.freshness.embed.warmup` | `10` | Warmup seconds for embedded freshness. |
| `sink.monitor.freshness.embed.static` | `false` | Whether to keep a static snapshot. |
| `sink.monitor.freshness.embed.snapshot` | `false` | Whether to take a snapshot. |
| `sink.monitor.freshness.embed.tablelist` | empty | Tables to include for embedded mode. |
| `sink.monitor.freshness.embed.delay` | `0` | Delay seconds for embedded freshness. |
| `sink.monitor.freshness.verbose` | `false` | Verbose freshness logging. |
| `sink.monitor.freshness.timestamp` | `false` | Include timestamps. |

**Freshness Trino Settings**

| Key | Default | Notes |
| --- | --- | --- |
| `trino.url` | required for Trino-based freshness | JDBC URL. |
| `trino.user` | required for Trino-based freshness | Username. |
| `trino.password` | required for Trino-based freshness | Password. |
| `trino.parallel` | `1` | Parallel query count. |


