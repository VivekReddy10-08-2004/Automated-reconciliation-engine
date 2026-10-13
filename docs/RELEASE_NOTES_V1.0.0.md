# v1.0.0 Release Notes

`v1.0.0` is the first documented release of the local automated reconciliation
engine. The package version, API version, and telemetry resource version are
aligned at `1.0.0`. The intended Git tag is `v1.0.0`; create it only from the
reviewed release commit after CI succeeds.

## Included

- live-verified three-provider equity reconciliation;
- immutable Parquet evidence and atomic DuckDB persistence;
- offline-verified persisted Yahoo/Alpaca options reconciliation;
- dbt equity facts and data tests;
- authenticated exports and atomic read-only snapshots;
- simulated ZeroMQ anomaly demonstration;
- OpenTelemetry instrumentation and operational alerts;
- correctness, performance, load, crash-recovery, and fault evidence; and
- automated release gates.

## Explicitly not included

- an Apache Airflow DAG or runtime;
- persisted futures orchestration;
- shared-environment managed secrets, RBAC, TLS, or backups;
- verified Docker or external telemetry-backend execution on the evidence host.

See [Feature Support](FEATURE_SUPPORT.md) for verification levels and
[Release Evidence](RELEASE_EVIDENCE.md) for measured claims.
