# ADR 0004: Separate Writers and API Readers with Atomic Snapshots

- Status: Accepted
- Date: 2026-10-13

## Context

DuckDB is embedded and file-oriented. Serving API traffic directly from the
active batch database can create lock contention and couples read availability
to the writer lifecycle.

## Alternatives considered

- Read the active file: minimal storage, but exposes lock conflicts.
- Copy the file directly: simple, but readers could observe a partial copy.
- Move serving data to PostgreSQL or a warehouse: appropriate at larger scale,
  but excessive for the local release.

## Decision

Copy a completed DuckDB catalog into a temporary destination and atomically
replace the published read-only database. Point API processes at that snapshot.

## Consequences

Readers receive a consistent release boundary and do not contend with the next
batch writer. Publication consumes additional disk and data freshness is tied
to snapshot cadence. Existing processes may need restart or connection renewal
to observe the new file.

## Revisit when

Near-real-time freshness, multiple API hosts, or independent scaling is needed.
Publish curated Parquet to object storage or adopt a transactional serving
database and warehouse.
