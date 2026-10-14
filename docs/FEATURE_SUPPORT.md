# Feature Support and Verification Levels

This page is the source of truth for what release `v1.0.0` implements and how
each capability has been verified.

## Status definitions

| Status | Meaning |
| --- | --- |
| Implemented, live verified | Production path exists and was exercised against real providers or a running service. |
| Implemented, offline verified | Production path exists and deterministic tests exercise it without external provider access. |
| Instrumented, offline verified | Signal-producing code and in-memory exporters are tested; the external backend was not run on this host. |
| Library only | Reusable ingestion/reconciliation code exists, but no persisted operational command exists. |
| Experimental, library only | Adapter or comparison code is offline-tested; comparable live observations and a persisted end-to-end workflow have not been verified. |
| Deployment contract verified | Configuration and security contracts are tested, but the runtime was unavailable on the verification host. |
| Planned | No runnable implementation is included in this release. |

## v1.0.0 support matrix

| Component | Support level | Evidence or limitation |
| --- | --- | --- |
| Yahoo/Alpaca/Massive equity batch | Implemented, live verified | A real AAPL run, replay, deterministic retry, DuckDB audit, and Parquet evidence are recorded in [Release Evidence](RELEASE_EVIDENCE.md). |
| Yahoo/Alpaca options batch | Implemented, offline verified | `reconciliation_engine.options_batch` atomically persists option runs and rejects duplicate logical runs; database integration tests pass. Live provider execution is not claimed. |
| Massive options adapter and three-provider comparison helpers | Experimental, library only | Contract metadata and historical aggregate bars are available through standalone adapter functions. Massive is not called by `options_batch`; offline fixtures do not establish live snapshot/bar comparability. |
| Yahoo/Massive futures ingestion and reconciliation | Experimental, library only | Provider-aware comparison and roll-mapping tests exist. Live contract/session comparability is unverified; persisted orchestration is planned. |
| DuckDB and Parquet equity audit trail | Implemented, live verified | The live equity run and offline crash/replay harness verify persisted evidence. |
| dbt equity models | Implemented, offline verified | All 23 models/tests pass against an initialized source schema. |
| Authenticated export API | Implemented, offline verified | Endpoint, authentication, export, overload, and 1/10/25/50-client load tests pass. |
| Atomic API serving snapshots | Implemented, offline verified | Snapshot independence and atomic replacement are covered by integration tests. |
| ZeroMQ demonstration | Implemented, offline verified | In-process publisher/subscriber and anomaly-republication tests pass using simulated ticks. |
| OpenTelemetry | Instrumented, offline verified | Spans, metrics, correlation, and in-memory exporters are tested. Collector/Tempo/Prometheus/Grafana were not run because Docker was unavailable. |
| Docker batch image | Deployment contract verified | Dockerfile, Compose volume, non-root user, and secret-exclusion tests pass. No image was built on the verification host. |
| Local scheduling | Implemented, operator configured | Cron and Windows Task Scheduler instructions are documented; scheduling is external to the application. |
| Apache Airflow | Planned | No DAG, Airflow runtime, or task-log workflow is included. Airflow is not a core dependency. |
| Managed secrets, RBAC, TLS, backups | Planned deployment controls | Local owner-only secrets and files are implemented; shared-environment controls remain deployment work. |

“Offline verified” is an evidence boundary, not a statement that live providers
will return comparable data for every entitlement, date, or instrument.

## Derivatives scope and promotion criteria

The supported persisted options workflow is Yahoo/Alpaca, with offline evidence
only. Massive derivatives adapters are retained for evaluation and future
historical controls; the normal options and equity commands do not invoke them.
Their existence does not establish an operational three-provider derivatives control.

- **Massive options:** the current adapter retrieves contract metadata and daily
  trade aggregates. It leaves bid, ask, and open interest unavailable and does
  not fetch IV or Greeks. A historical aggregate close cannot validate a chain
  snapshot price without matching contract, observation time, and price basis.
- **Yahoo/Massive futures:** both adapters expose OHLCV, but Yahoo continuous
  symbols and Massive expiring contracts require a dated roll mapping and
  aligned trading sessions. The current helper compares closes and retains
  Massive settlement separately; it does not equate settlement with close.

Prioritize one demonstrably comparable control before expanding derivative
ingestion. Promotion requires documented field/time/contract alignment, live
provider evidence for entitled inputs, and a persisted end-to-end workflow
with retry and recovery verification. Offline fixtures remain useful evidence
of code behavior, but do not satisfy those live comparison criteria.
