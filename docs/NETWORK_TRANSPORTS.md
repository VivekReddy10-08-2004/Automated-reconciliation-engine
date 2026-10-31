# TCP/TLS and UDP Multicast Market-Data Transports

ADR 0007 is implemented as a transport-agnostic market-event layer in
`reconciliation_engine.realtime.network_transport`. The adapters are offline
verified with local sockets and deterministic fixtures. No live vendor feed,
entitlement, authentication handshake, or exchange certification is claimed.

For short interactive commands, source `scripts/reconciliation_aliases.sh` from
the repository root and use `recon-tcp`, `recon-multicast`, or
`recon-dashboard`. The aliases use environment-variable defaults and forward
additional CLI arguments.

## Common event contract

Every inbound JSON object must contain:

```json
{
  "source": "VENDOR_A",
  "stream_id": "EQUITIES_CHANNEL_1",
  "sequence_number": 10452,
  "symbol": "AAPL",
  "event_type": "TRADE",
  "price": 270.25,
  "quantity": 100,
  "exchange_timestamp": "2026-10-31T14:30:00.123456+00:00",
  "event_id": "optional-vendor-event-id",
  "correlation_id": "optional-vendor-correlation-id"
}
```

`stream_id` is optional and defaults to `source`. `event_id` is optional; when
absent, the engine derives a deterministic UUID from source, stream, transport,
vendor sequence, instrument, event type, and exchange timestamp.
`correlation_id` defaults to the resulting event ID.

The adapter adds the UTC receive timestamp and calculated latency. It rejects
missing fields, naive timestamps, future exchange timestamps, non-positive
prices, negative quantities, invalid sequence numbers, malformed JSON, and
oversized frames.

## TCP and TLS

Newline-delimited JSON:

```sh
python -m reconciliation_engine.realtime.network_transport tcp \
  127.0.0.1 9000 --framing newline --count 100 \
  --db reconciliation.duckdb
```

The monitor defaults to a live, bounded Rich table showing receive time,
transport, source/stream, vendor sequence, ordering status, symbol, event type,
price, quantity, latency, and missing count. Change the retained window with
`--table-rows 50`. For machine-readable output, use `--output-format json`.

Four-byte, unsigned, network-byte-order length prefix followed by JSON:

```sh
python -m reconciliation_engine.realtime.network_transport tcp \
  feed.example.test 9001 --framing length-prefixed --tls \
  --reconnect-attempts 5 --db reconciliation.duckdb
```

TLS uses Python's default authenticated client context and hostname validation.
The CLI intentionally has no insecure certificate-bypass option. A library
caller can inject a managed `ssl.SSLContext` containing the required private CA
or client certificate.

TCP connection closure, timeout, and socket failures trigger bounded exponential
reconnection. Malformed application frames fail explicitly instead of being
silently discarded or treated as a connection problem.

## UDP multicast

Join an IPv4 group on the default interface:

```sh
python -m reconciliation_engine.realtime.network_transport multicast \
  239.10.10.10 5000 --count 100 --db reconciliation.duckdb
```

Select a specific local interface address:

```sh
python -m reconciliation_engine.realtime.network_transport multicast \
  239.10.10.10 5000 --interface 192.0.2.25 \
  --db reconciliation.duckdb
```

The adapter validates that the group is multicast, binds the UDP port, joins
with `IP_ADD_MEMBERSHIP`, receives at most one maximum-size UDP payload, and
leaves with `IP_DROP_MEMBERSHIP` on shutdown. Network routing, firewall rules,
and interface multicast support remain host responsibilities.

## Vendor sequence analysis

Sequence state is independent per `stream_id`:

| Status | Meaning |
| --- | --- |
| `FIRST` | First observation for a stream in this process |
| `IN_ORDER` | Exactly one greater than the highest observed sequence |
| `GAP` | One or more vendor sequences were skipped |
| `DUPLICATE` | Sequence was seen inside the bounded duplicate window |
| `REORDERED` | Unseen sequence arrived below the highest sequence |

The event journal stores expected sequence and missing count. Sequence state is
currently process-local; after a monitor restart, the first received event is
classified `FIRST`. Durable sequence checkpoints are a future enhancement if a
vendor requires continuity across process restarts.

## Evidence and dashboard flow

```text
TCP/TLS stream or multicast datagram
              |
              v
       framing + JSON validation
              |
              v
          MarketEvent
              |
              v
 stream-aware sequence classification
              |
       +------+------+
       |             |
       v             v
OpenTelemetry     DuckDB event journal
logs/traces              |
                         v
               investigation dashboard
```

The monitor writes event identity, source, stream, event type, quantity, vendor
sequence, expected sequence, missing count, transport, timestamps, latency,
correlation ID, and trace/span IDs. The dashboard remains read-only.

## Packet capture

Generate a narrow command without executing it:

```sh
python -m reconciliation_engine.realtime.network_transport capture-command \
  multicast 239.10.10.10 5000 \
  --interface eth0 --output captures/feed.pcap
```

TCP example:

```sh
python -m reconciliation_engine.realtime.network_transport capture-command \
  tcp 192.0.2.20 9000 \
  --interface eth0 --output captures/feed.pcap
```

The command builder requires a literal IP address and `.pcap` destination and
does not invoke `tcpdump`. The operator must review and execute the printed
command with the minimum host capability required for packet capture. Captures
may contain licensed or sensitive market data and must follow access, retention,
and encryption controls. Packet payloads are never stored in telemetry labels.

## Current boundary

- Implemented: TCP framing, TLS wrapping, bounded reconnects, multicast
  join/receive/leave, common contract, sequence analysis, journaling, telemetry,
  dashboard visibility, capture-filter generation, and offline tests.
- Not claimed: live vendor connectivity, vendor login/subscription messages,
  binary exchange protocols, entitlement validation, redundant A/B feed
  arbitration, durable sequence checkpoint recovery, or packet-capture execution.
