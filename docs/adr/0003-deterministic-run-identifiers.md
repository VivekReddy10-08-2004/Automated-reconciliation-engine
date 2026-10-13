# ADR 0003: Derive Deterministic Logical Run IDs

- Status: Accepted
- Date: 2026-10-13

## Context

Schedulers, operators, and crash recovery can invoke the same logical control
more than once. Random IDs make duplicate execution difficult to distinguish
from legitimate reruns.

## Alternatives considered

- Random UUID per attempt: simple, but permits duplicate provider calls and
  duplicate persisted controls.
- Database sequence: unique but not stable across retries.
- Caller-supplied ID: flexible but shifts correctness to every caller.

## Decision

Derive a UUID from the control date, normalized sorted universe, partition
configuration, and reconciliation rule version. Derive options IDs from the
underlying, expiration, control date, and options rule version.

## Consequences

The database primary key becomes an idempotency boundary. A completed retry is
a no-op, while an interrupted attempt can reuse immutable archives. A material
rule change must increment the definition version or it would incorrectly map
to an older logical run.

## Revisit when

Operators need multiple intentional executions of the same rule and control
date. Add an explicit, audited run variant rather than returning to random IDs.
