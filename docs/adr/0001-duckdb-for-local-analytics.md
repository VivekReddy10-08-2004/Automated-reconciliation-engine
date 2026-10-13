# ADR 0001: Use DuckDB for Local Analytical State

- Status: Accepted
- Date: 2026-10-13

## Context

The release needs SQL analytics, transactional run persistence, dbt support,
and a setup that one developer can run without administering a database server.
Writes are batch-oriented and API reads can be served from completed snapshots.

## Alternatives considered

- PostgreSQL: stronger concurrent service semantics, but additional deployment
  and operational overhead for a local analytical demonstration.
- SQLite: simple embedding, but a weaker fit for Parquet analytics and dbt.
- Cloud warehouse: scalable, but introduces cost, credentials, and network
  dependencies that undermine the offline demonstration.

## Decision

Use DuckDB as the local transactional and analytical database. Keep write
ownership in the batch process and expose curated data through read-only API
connections or a published snapshot.

## Consequences

The project is portable and works naturally with Pandas, Parquet, and dbt.
DuckDB is not treated as a multi-host OLTP service. Concurrent write/read
contention must be bounded, and horizontal API scaling requires a different
serving store or immutable snapshot distribution.

## Revisit when

Multiple writers, multi-host serving, high availability, or continuously
available reads become requirements. PostgreSQL plus an analytical warehouse
would then be the leading replacement.
