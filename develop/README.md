## Overview

This folder contains the local Docker-based development environment for Pixels Sink.

## Quick Start

### Prerequisites
- Docker Engine
- Docker Compose
- Pixels installed and available in your local or remote Maven repository

### Install
```bash
./install
```

### Verify
After installation, open Kafdrop to verify Kafka is up:

[http://localhost:9000](http://localhost:9000)

Grafana is also exposed at:

[http://localhost:3000](http://localhost:3000)

### Cleanup
```bash
. scripts/common_func.sh
shutdown_containers
```

## Install Options

The `./install` script supports the following flags (all are optional):
- `--need_build=on|off` build images and Pixels Sink
- `--need_init=on|off` start containers via `docker compose`
- `--generate_data=on|off` generate TPCH data
- `--data_scale=<number>` TPCH scale factor, default `0.1`
- `--enable_mysql=on|off` enable MySQL connector and source DB
- `--load_mysql=on|off` load TPCH data into MySQL (requires `--enable_tpch=on`)
- `--enable_postgres=on|off` enable Postgres connector and source DB
- `--load_postgres=on|off` load TPCH data into Postgres (requires `--enable_tpch=on`)
- `--enable_tpch=on|off` enable TPCH dataset and loading logic
- `--enable_tpcc=on|off` run TPC-C benchmark after startup
- `--develop_debug=on` enable verbose shell tracing

## Example Commands

- Start containers only, no build, no data generation:
```bash
./install --need_build=off --generate_data=off --enable_mysql=off --load_postgres=off
```

- Build images and start, without TPCH/TPC-C:
```bash
./install --need_build=on --generate_data=off --enable_tpch=off --enable_tpcc=off
```

- MySQL only, no Postgres:
```bash
./install --need_build=on --generate_data=off --enable_mysql=on --enable_postgres=off --enable_tpch=off
```

- MySQL + TPCH data generation (scale 10):
```bash
./install --need_build=on --generate_data=on --data_scale=10 --enable_mysql=on --enable_postgres=off --enable_tpch=on --enable_tpcc=off
```

- MySQL + TPC-C benchmark:
```bash
./install --need_build=off --generate_data=off --enable_mysql=on --enable_postgres=off --enable_tpch=off --enable_tpcc=on
```

## Test Databases

### MySQL
```bash
docker exec -it pixels_mysql_source_db \
  mysql -upixels -ppixels_realtime_crud -D pixels_realtime_crud
```

### Postgres
```bash
docker exec -it pixels_postgres_source_db \
  psql -Upixels -d pixels_realtime_crud
```
