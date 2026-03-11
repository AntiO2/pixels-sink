# pixels-sink

Pixels Sink is the data sink service for Pixels. It ingests debezium-format change events from multiple sources (Debezium engine, Kafka, or storage files), converts them into Pixels events, and writes them to a configured sink (Retina, CSV, Proto, Flink, or none).

This project is under active development.

## Docs

- Architecture and pipeline overview: [docs/overview.md](docs/overview.md)
- Transaction handling: [docs/transaction.md](docs/transaction.md)
- Usage guide: [docs/usage.md](docs/usage.md)
- Configuration reference: [docs/configuration.md](docs/configuration.md)
- Local dev environment (Docker): [develop/README.md](develop/README.md)

## Quick Start

### Requirements
- [Pixels](https://github.com/pixelsdb/pixels)
- Java 17
- Maven 3.9+
- Source and sink dependencies based on your configuration
  - Kafka broker if `sink.datasource=kafka`
  - Debezium + database access if `sink.datasource=engine`
  - Retina service if `sink.mode=retina`
  - Trino if freshness checking is enabled and uses Trino

### Build
```bash
mvn -q -DskipTests package
```

### Run (Script)
```bash
./pixels-sink [config.properties]
```

The script reads `conf/jvm.conf` and uses a properties file configured inside `./pixels-sink` by default. If you pass a path, it overrides the default.

### Run (IDE)
- Main class: `io.pixelsdb.pixels.sink.PixelsSinkApp`
- Program arguments: `-c conf/pixels-sink.aws.properties`

## Configuration
- Sample configs are in `conf/`.
- Start with `conf/pixels-sink.aws.properties` and adjust.
- See [docs/configuration.md](docs/configuration.md) for a full key reference and guidance.

## Monitoring
- Enable Prometheus metrics with `sink.monitor.enable=true`.
- Metrics endpoint listens on `sink.monitor.port` (default `9464`).
