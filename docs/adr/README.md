# Architecture Decision Records

These records capture decisions made for the local `v1.0.0` release. A decision
is revisited when its stated conditions change; an ADR is superseded rather
than silently rewritten.

| ADR | Decision |
| --- | --- |
| [0001](0001-duckdb-for-local-analytics.md) | Use DuckDB for local analytical state |
| [0002](0002-immutable-parquet-archives.md) | Preserve immutable Parquet evidence |
| [0003](0003-deterministic-run-identifiers.md) | Derive deterministic logical run IDs |
| [0004](0004-atomic-serving-snapshots.md) | Separate writers and API readers with atomic snapshots |
| [0005](0005-zeromq-for-local-realtime-demo.md) | Use ZeroMQ for the local real-time demonstration |
| [0006](0006-bounded-partition-concurrency.md) | Bound provider-partition concurrency |

Status values follow the lightweight convention `Accepted`, `Superseded`, or
`Deprecated`.
