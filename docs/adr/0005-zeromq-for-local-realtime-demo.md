# ADR 0005: Use ZeroMQ for the Local Real-Time Demonstration

- Status: Accepted
- Date: 2026-10-13

## Context

The project needs to demonstrate tick publication, anomaly scoring, and an
independently degradable dashboard without requiring a managed broker.

## Alternatives considered

- Kafka or Redpanda: durable and scalable, but operationally heavy for a local
  ten-tick demonstration.
- Redis Streams: simpler than Kafka but still requires a server.
- In-process queues: easy, but do not demonstrate process boundaries.

## Decision

Use loopback-only ZeroMQ PUB/SUB sockets for raw and scored tick topics. Keep
the published feed explicitly simulated and deterministic.

## Consequences

The demo is lightweight and preserves process isolation. PUB/SUB does not offer
durable replay, consumer offsets, delivery guarantees, or backpressure suitable
for a production market-data platform.

## Revisit when

Durability, replay, consumer groups, ordering guarantees, or remote deployment
becomes a requirement. Select a managed event log and define schemas and
delivery semantics before migration.
