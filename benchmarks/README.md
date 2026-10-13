# Reproducible verification and benchmarks

All workloads are deterministic, offline, and safe to run without provider
credentials. Results in `results/` were recorded on October 13, 2026. Raw
measurements are committed alongside dependency-free PNG charts in `charts/`.

## Published results

| Workload | Rows | Workers | Duration | Throughput | Peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Reconciliation | 100,000/source | 1 | 3.045 s | 32,840 rows/s | 276.04 MB |
| Reconciliation | 1,000,000/source | 1 | 30.173 s | 33,142 rows/s | 1,066.08 MB |
| Partition ingestion | 1,000 | 1 | 12.701 s | 78.73 rows/s | 228.52 MB |
| Partition ingestion | 1,000 | 4 | 10.285 s | 97.23 rows/s | 226.45 MB |
| Partition ingestion | 1,000 | 8 | 10.299 s | 97.10 rows/s | 229.00 MB |

The 100K and 1M correctness oracle found no lost records, duplicate business
keys, or silent mismatches. The 1M result also shows the current in-memory
Pandas implementation reaching roughly 1 GB RSS; 5M–10M is therefore an
explicit known limit on this host and should use chunked/SQL reconciliation.
The result CSVs also record reconciliation CPU utilization (96.94% and 96.90%),
ingestion CPU utilization (44.52%, 54.82%, and 57.87%), and disk growth
(2.81 MB in every ingestion case).

The API fixture contained 10,000 curated reconciliations. Twenty requests at
each of 1, 10, 25, and 50 clients produced zero errors. Exact p50/p95/p99 and
request-rate measurements are in `results/api_load_results.csv`. This is a
bounded local DuckDB service, not a multi-host capacity claim.

Crash processes exited immediately before archival, after immutable archival,
and after database commit. `results/recovery_report.json` proves respectively:
no partial state, five reusable Parquet artifacts with no partial database
commit, and one complete commit. Repeating the logical write returned `False`
and added no rows.

## Commands

```sh
python -m benchmarks.benchmark_reconciliation --sizes 100000 1000000
python -m benchmarks.benchmark_ingestion --symbols 1000 --workers 1 4 8
python -m benchmarks.benchmark_recovery
python -m benchmarks.run_end_to_end_demo
python -m benchmarks.chart_results
```

For API load testing:

```sh
python -m benchmarks.seed_api --db /tmp/reconciliation-load.duckdb --rows 10000
RECONCILIATION_EXPORT_API_KEY=benchmark-token \
  python -m reconciliation_engine.api --db /tmp/reconciliation-load.duckdb \
  --host 127.0.0.1 --port 8765 --max-concurrent-queries 4
RECONCILIATION_EXPORT_API_KEY=benchmark-token \
  RECONCILIATION_API_URL=http://127.0.0.1:8765 \
  LOAD_TEST_REQUESTS=20 node benchmarks/api_load_test.js
```

The recovery harness covers abrupt termination and atomicity. Unit tests cover
timeouts, HTTP 429 retry, corrupt-payload quarantine, disk-write failure,
snapshot consistency, authentication, and API capacity rejection.

## Runtime verification requiring Docker

The host used for these results has no Docker executable, so neither the batch
image nor the Collector/Tempo/Prometheus/Grafana stack can truthfully be marked
runtime-verified here. Compose contract tests, secret-exclusion tests, and the
in-memory telemetry integration pass. On a Docker host run:

```sh
docker compose -f orchestration/docker/compose.yaml build batch
docker compose -f orchestration/docker/compose.yaml run --rm batch
docker compose -f orchestration/docker/compose.yaml run --rm batch
docker volume inspect automated-reconciliation-engine-state
docker compose --env-file project.env -f observability/docker-compose.yaml up -d
```

Confirm the second batch run reuses persisted state, inspect image history for
secret absence, then query Prometheus and Tempo as described in
`docs/OBSERVABILITY.md`.
