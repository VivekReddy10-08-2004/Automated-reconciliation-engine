# Engineering Case Study: Auditable Market-Data Reconciliation

## Problem

Independent market-data providers do not always return the same observations.
They differ in schemas, coverage, adjustment policy, timestamps, account
entitlements, and availability. A naïve inner join silently discards missing
records; a naïve retry can duplicate writes or consume provider quota again.

The project builds a local, auditable control that answers three questions:

1. What did each provider return?
2. Which observations match, warn, break, or exist on only one side?
3. Can the result be reproduced after a process or provider failure?

## Current architecture

Provider adapters normalize equity and option observations into canonical
schemas. Equity work is divided into stable provider/symbol partitions and
executed with bounded concurrency. Raw and normalized inputs are written to
immutable Parquet checkpoints. Pairwise reconciliation preserves one-sided
observations and writes run, result, and exception state atomically to DuckDB.

dbt builds typed equity facts from the staged source. An authenticated FastAPI
service exposes curated runs, reconciliations, exceptions, and bounded exports.
For concurrent serving, a completed DuckDB catalog is atomically published as a
read-only snapshot. A separate simulated ZeroMQ path demonstrates tick latency,
explainable anomaly scoring, and dashboard failure isolation.

The exact implemented/planned boundary is maintained in the
[Feature Support Matrix](FEATURE_SUPPORT.md).

## Engineering challenges

### Missing and inconsistent data

Reconciliation uses full outer comparisons. A provider-only row becomes a
`BREAK` with an explicit missing-provider reason instead of disappearing.
Inputs reject duplicate business keys, invalid dates, null/non-finite values,
negative OHLCV values, and inconsistent high/low ranges.

### Idempotency and crash recovery

Logical inputs produce deterministic run IDs. Completed retries make no
provider request and no additional write. Partition checkpoints allow an
interrupted run to resume without replacing original evidence. The final
DuckDB write is one transaction.

### Concurrent reading and writing

DuckDB is a strong local analytical fit but not a multi-host serving database.
The API uses bounded read-only connections, and the snapshot publisher creates
a complete temporary catalog before atomically replacing the serving file.

### Vendor quotas and scaling

Stable hash partitions enable bounded parallelism and granular retries. Massive
requests also share a process-wide quota controller with timeout and retryable
HTTP handling. The measured workload improved from one to four workers and
then flattened, demonstrating why concurrency is capped rather than maximized.

## Key decisions

- **DuckDB:** embedded SQL, dbt compatibility, and Parquet analytics with an
  explicit single-writer/local-serving constraint.
- **Parquet:** immutable, compact, columnar evidence for replay and lineage.
- **FastAPI:** typed authenticated access to curated data without exposing a
  write surface.
- **Deterministic IDs:** database-enforced idempotency across retries.
- **Stable partitions:** predictable concurrency, checkpointing, and failure
  isolation under provider quotas.

The detailed trade-offs and revisit conditions are recorded in
[Architecture Decision Records](adr/README.md).

## Evidence

The correctness oracle found zero lost records and zero duplicate keys at 100K
and 1M rows per source, while detecting every injected break. The 1M workload
completed in 30.173 seconds at 33,142 rows/second with 1,066 MB peak RSS.

Process-level recovery tests terminated execution before archival, after
archival, and after commit. They demonstrated respectively no partial state,
reusable immutable evidence without a database row, and exactly one committed
run. Fault tests cover timeouts, HTTP 429, corrupt payloads, and disk failure.

The export API was measured with 1, 10, 25, and 50 authenticated clients against
a 10,000-row fixture. All measured runs had zero errors; raw throughput and
p50/p95/p99 latency are committed under `benchmarks/results/`.

See [Release Evidence](RELEASE_EVIDENCE.md) for the live equity run and
[Benchmark Evidence](../benchmarks/README.md) for reproduction commands.

## Limitations

- Reconciliation currently materializes Pandas frames in memory. The 1M test
  used about 1.07 GB RSS; 5M–10M requires chunked or SQL execution.
- DuckDB and atomic snapshots are a local/single-node design, not a distributed
  serving platform.
- Scheduling uses cron or Windows Task Scheduler. No Airflow DAG is included.
- Options persistence is offline-tested but not live-provider verified.
- Massive options and Yahoo/Massive futures are experimental library capabilities.
  Historical option bars require matching price definitions and observation times;
  futures require contract-roll and session alignment. Live comparability and
  persisted integration must be demonstrated before promoting these adapters.
- Telemetry exporters are offline-tested, but the reference backend stack was
  not run on the verification host because Docker was unavailable.
- Provider entitlements and financial-data redistribution licenses remain the
  operator's responsibility.
