# Scripts

This folder contains performance and monitoring helpers for Pixels Sink. Most scripts are used to organize results, aggregate metrics, or prepare datasets for analysis. Some scripts also generate plots.

## Scripts

- `perf_flush.py`:
  Measures flush behavior and latency under different flush settings.

- `perf_freshness.py`:
  Collects freshness metrics (row/txn/embed) and writes reports.

- `perf_multi_rate.py`:
  Runs multi-rate load tests and aggregates results.

- `perf_query.py`:
  Issues benchmark queries (typically via Trino) to validate freshness or latency.

- `perf_rate.py`:
  Runs single-rate throughput tests.

- `perf_retina.py`:
  Benchmarks Retina write performance.

- `perf_retina_2.py`:
  Alternative Retina benchmark with different batching or concurrency settings.

- `perf_retina_3.py`:
  Another Retina benchmark variant for comparative testing.

- `perf_web_monitor.py`:
  Pulls metrics from local folder and shows results.
