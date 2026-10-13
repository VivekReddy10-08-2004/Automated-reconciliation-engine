# ADR 0002: Preserve Immutable Parquet Evidence

- Status: Accepted
- Date: 2026-10-13

## Context

Provider payloads must remain replayable and auditable after an interrupted run
or a vendor response changes. Re-fetching is costly, rate-limited, and may not
reproduce the original observation.

## Alternatives considered

- Database-only storage: simpler catalog, but couples recovery to one database
  file and makes raw evidence harder to inspect independently.
- Mutable “latest” files: convenient for consumers, but destroy lineage and can
  expose partial replacement state.
- JSON archives: preserve payloads but are less compact and slower for columnar
  analytical replay.

## Decision

Write raw and normalized Parquet files once under run- and partition-specific
paths. Refuse overwrites and derive reconciliation evidence into a separate
immutable layer.

## Consequences

Retries can reuse checkpoints without provider calls, and auditors can trace a
result to source evidence. Storage grows monotonically until an explicit
retention policy runs. Schema evolution must preserve readable historical
partitions.

## Revisit when

The archive moves to object storage at materially larger scale. The immutability
rule remains, but table formats such as Iceberg or Delta may provide better
catalog, compaction, and schema-evolution behavior.
