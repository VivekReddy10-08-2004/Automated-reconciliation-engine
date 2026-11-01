# 0007. TCP and Multicast Transports

## Status

Accepted

## Implementation Status

Implemented and offline verified. The generic JSON TCP/TLS and IPv4 multicast
adapters, common `MarketEvent`, source-and-stream-aware vendor sequence
analysis, event journaling, telemetry, monitor CLI, and packet-capture command
generation are covered by deterministic tests and real loopback TCP checks.
We also keep reproducible local throughput, latency, memory, classification,
and reconnect benchmarks. Live vendor authentication, entitlements, and feed
behavior have not been verified.

## Context

We currently use ZeroMQ for the local real-time demonstration (ADR-0005), but
our simulated tick feed does not represent a vendor feed.

Several external equity data sources (e.g., Alpaca, Massive) offer live streaming feeds over:
1. TCP/IP with custom framing (length-prefixed JSON or newline-delimited)
2. UDP multicast groups

We currently ingest the batch path from REST APIs. We added network transport support to:
- Production-grade real-time feed ingestion
- Comparison of vendor feeds vs batched data (reconciliation)
- Event stream tracing with sequence numbers and gap detection
- Packet capture for debugging/diagnostic purposes

## Decision

Add new production-grade transport adapters:
1. TCP/SSL transport adapter supporting length-prefixed or newline-delimited JSON framing
2. UDP multicast transport adapter supporting join/leave operations
3. Transport-agnostic `MarketEvent` contract to allow downstream components to be independent of transport
4. Logging support for network events with trace correlation

Keep ZeroMQ as the local simulated demonstration in `reconciliation_engine.realtime` (the existing demo functionality remains unchanged).

## Consequences

### Positive
- Enables production real-time data ingestion from vendors with feed over TCP/multicast
- Provides sequence number tracking and gap/reorder detection for event stream tracing
- Adds ability to capture real network traffic via packet capture (scapy or tcpdump)
- Aligns with operational requirements for market data feeds
- Maintains zero-cost upgrade path for downstream components using `MarketEvent` contract

### Negative
- Adds complexity to the architecture with multiple transport types
- May require root privileges for some operations (particularly multicast and pcap)
- Packet-capture execution depends on an operator-managed tool such as
  `tcpdump`; we only validate and build a narrow command
- Need to handle transport-specific connection management and reconnection logic

## Implementation Strategy

1. New module: `reconciliation_engine.realtime.network_transport.py`
2. Transport adapter interface:
   - `TCPTransport`
   - `MulticastTransport`
3. Common `MarketEvent` dataclass with sequence number
4. Enhanced `observability.log_event()` to capture transport events
5. Test coverage for framing, reconnection, validation
6. Bounded result retention for long-running monitors
7. Sequence state keyed by `(source, stream_id)`
8. Reproducible local adapter benchmarks with machine-readable results

See [TCP/TLS and UDP Multicast Market-Data Transports](../NETWORK_TRANSPORTS.md)
for the wire contract, commands, diagnostics, and verification boundary.
