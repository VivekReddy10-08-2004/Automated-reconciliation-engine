# ADR 0006: Bound Provider-Partition Concurrency

- Status: Accepted
- Date: 2026-10-13

## Context

Large symbol universes benefit from parallel ingestion, while providers impose
rate limits and local DuckDB/file operations have finite capacity. Unbounded
threads can increase throttling, memory pressure, and failure amplification.

## Alternatives considered

- Sequential ingestion: simplest and provider-friendly, but underuses available
  latency overlap.
- One task per symbol with unbounded workers: high apparent parallelism but poor
  quota behavior and excessive checkpoint overhead.
- Distributed task queue: scalable, but unnecessary for the local release.

## Decision

Hash symbols into a stable number of provider partitions and execute them with
a configurable worker ceiling. Apply an additional process-wide limiter to
Massive requests and checkpoint each partition independently.

## Consequences

Retries are granular and parallelism is predictable. The benchmark shows
diminishing returns after four workers for the tested workload, so higher
worker counts are not assumed to improve throughput. A fixed bucket count is
part of the logical run identity.

## Revisit when

One machine cannot meet the batch window. Move partition manifests to durable
workflow state, distribute workers, and retain provider-specific global quota
coordination.
