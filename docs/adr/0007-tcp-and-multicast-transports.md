# 0007. TCP and Multicast Transports

## Status

Accepted

## Context

The reconciliation engine currently uses ZeroMQ for the local real-time demonstration (ADR-0005), but this is a simulated tick feed that doesn't represent vendor feeds.

Several external equity data sources (e.g., Alpaca, Massive) offer live streaming feeds over:
1. TCP/IP with custom framing (length-prefixed JSON or newline-delimited)
2. UDP multicast groups

The current engine is batch-based and ingests from REST APIs. Adding network transport support would allow:
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
- Additional dependency on `scapy` for packet capture support (optional)
- Need to handle transport-specific connection management and reconnection logic

## Implementation Strategy

1. New module: `reconciliation_engine.realtime.network_transport.py`
2. Transport adapter interface:
   - `TCPTransport`
   - `MulticastTransport`
3. Common `MarketEvent` dataclass with sequence number
4. Enhanced `observability.log_event()` to capture transport events
5. Test coverage for framing, reconnection, validation