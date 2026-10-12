# Scalability and Deployment Model

The engine scales by dividing a logical control into deterministic work units:

```text
control date -> instrument type -> stable symbol bucket -> provider
```

For example, the same symbol always maps to the same bucket for a fixed bucket
count. Each non-empty bucket/provider combination gets a UUID5 `partition_id`
derived from its logical identity. The partition can therefore be retried,
measured, or quarantined without rerunning the entire control.

## Runnable local architecture

```text
provider adapters                 bounded ingestion workers
Yahoo   --- canonical equity --\
Alpaca  --- canonical equity ----+--> immutable partition Parquet
Massive --- canonical equity --/       |
                                          v
                                 single DuckDB writer/coordinator
                                          |
                                          v
                              pairwise equity reconciliation
```

Provider adapters own authentication, request shapes, pagination, and payload
translation. Reconciliation receives only canonical equity bars and does not
import a vendor SDK or inspect a vendor payload. Adding another provider means
adding an adapter and configuration; the pairwise reconciliation algorithm is
unchanged.

`RECONCILIATION_MAX_WORKERS` bounds process-wide partition concurrency. Massive
also has its own shared fixed-spacing limiter (`MASSIVE_CALLS_PER_MINUTE`, five
by default), so general concurrency cannot bypass the provider quota. Provider
latency, returned row count, and archive reuse are stored per partition.

Workers fetch and write distinct immutable Parquet paths. They do not write
DuckDB concurrently. The coordinator records manifest state and metrics after
worker completion, preserving DuckDB's single-writer operating model.

## Retry and quarantine behavior

The `work_partitions` table records status, attempts, timestamps, row counts,
and safe error metadata. A retry:

- reads a completed normalized partition without calling the provider;
- normalizes an existing raw partition when normalized output is absent;
- refetches a partition that failed before a raw archive was written; and
- places invalid or failed work in `quarantine_records` for investigation.

Raw evidence is immutable. If archived raw data is malformed, fixing the
normalizer allows it to be replayed; the engine does not silently replace the
evidence with a different payload.

## Storage growth and retention

Partition paths are object-storage compatible:

```text
data/partitions/<layer>/control_date=YYYY-MM-DD/
  instrument_type=EQUITY/symbol_bucket=00000/provider=<PROVIDER>/
  partition_id=<UUID>/<file>.parquet
```

`plan_partition_retention()` lists complete control-date directories before a
cutoff and reports their size. `apply_partition_retention()` is dry-run by
default and validates every target beneath the partition root before an
explicit apply. Retention should follow the organization's audit and legal
requirements; do not remove evidence solely to save local disk space.

## Operational controls

- Notifications are aggregated once per failed or break-bearing run, not once
  per exception.
- Exceptions carry priority, owner, reason, due time, and resolution time.
- The dashboard reports unassigned, overdue, and oldest-open exceptions.
- `provider_request_metrics` captures latency, volume, and checkpoint reuse.
- CLI partition lifecycle events are emitted as structured JSON logs without
  credentials or raw payloads.
- `quarantine_records` is the dead-letter index for failed or malformed work.

## Production scale-out boundary

DuckDB and local Parquet are intentional for a local analytical control, but a
single DuckDB file should not be shared by writers on multiple machines. At
larger scale, keep the same partition contract and replace infrastructure by
responsibility:

| Workload | Local implementation | Scale-out implementation |
| --- | --- | --- |
| Raw/normalized evidence | Local immutable Parquet | Object storage with lifecycle policies |
| Run and partition workflow state | Coordinator-owned DuckDB | Transactional database such as PostgreSQL |
| Historical reconciliation analytics | DuckDB/dbt | Warehouse or analytical database |
| Ingestion execution | Bounded thread pool | Queue-backed workers with provider-specific limits |
| Dashboard | Read-only DuckDB client | Read-only API or warehouse connection |
| Real-time scoring | Independent ZeroMQ process | Independently deployed stream consumer |

This separation allows ingestion, reconciliation, exception processing,
dashboard queries, and real-time scoring to scale independently. Ten ingestion
workers do not imply ten dashboards or ten databases.

## Capacity guidance

Keep `RECONCILIATION_SYMBOL_BUCKETS` stable after production data exists,
because it participates in the run definition and partition identity. Sixty-four
buckets is suitable for the current MVP and gives a larger universe enough
work units for parallel execution. Increase worker count only after measuring
provider quotas, storage throughput, and coordinator write time. More threads
cannot increase throughput beyond an upstream rate limit.
