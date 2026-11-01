# TCP/TLS and UDP Multicast Market-Data Transports

We implemented ADR 0007 as a transport-agnostic market-event layer in
`reconciliation_engine.realtime.network_transport`. We verified the adapters
offline with local sockets and deterministic fixtures. We have not yet connected
them to a live vendor feed or exercised vendor entitlements, authentication
handshakes, or exchange certification.

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
absent, we derive a deterministic UUID from source, stream, transport,
vendor sequence, instrument, event type, and exchange timestamp.
`correlation_id` defaults to the resulting event ID.

We add the UTC receive timestamp and calculated latency. We reject
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

We default the monitor to a live, bounded Rich table showing receive time,
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
We intentionally provide no insecure certificate-bypass option. A library
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

We validate that the group is multicast, bind the UDP port, join
with `IP_ADD_MEMBERSHIP`, receives at most one maximum-size UDP payload, and
leaves with `IP_DROP_MEMBERSHIP` on shutdown. Network routing, firewall rules,
and interface multicast support remain host responsibilities.

## Vendor sequence analysis

Sequence state is independent per `(source, stream_id)`. Two vendors can use
the same stream name without changing each other's ordering state:

| Status | Meaning |
| --- | --- |
| `FIRST` | First observation for a stream in this process |
| `IN_ORDER` | Exactly one greater than the highest observed sequence |
| `GAP` | One or more vendor sequences were skipped |
| `DUPLICATE` | Sequence was seen inside the bounded duplicate window |
| `REORDERED` | Unseen sequence arrived below the highest sequence |

We store expected sequence and missing count in the event journal. Sequence
state is currently process-local; after a monitor restart, the first received
event is classified `FIRST`. Durable sequence checkpoints are a future
enhancement if a vendor requires continuity across process restarts.

For continuous runs (`--count 0`), we process, journal, display, and then release
each event. The live table retains only `--table-rows` recent display records.
Finite library calls retain returned events by default; callers can set
`collect_events=False` when they only need side effects.

## Reproducible performance checks

Run the ADR 0007 workloads from the repository root:

```sh
python -m benchmarks.network.suite --profile quick
python -m benchmarks.network.suite --profile full
```

We keep the implementation under `benchmarks/network/`. Quick, full, and manual
results are separated under `benchmarks/results/network/` so a development run
cannot overwrite published evidence. The quick profile is intended for
development; the full profile reproduces the published workload sizes. The
transport and reconnect workloads use real IPv4 loopback listeners. We treat
these as local adapter-capacity measurements, not vendor-network or production
latency claims. The memory workload disables DuckDB and terminal rendering so
it isolates the monitor's retention behavior.

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
 source + stream sequence classification
              |
       +------+------+
       |             |
       v             v
OpenTelemetry     DuckDB event journal
logs/traces              |
                         v
               investigation dashboard
```

We write event identity, source, stream, event type, quantity, vendor
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

We require a literal IP address and `.pcap` destination, and our command builder
does not invoke `tcpdump`. We require the operator to review and execute the printed
command with the minimum host capability required for packet capture. Captures
may contain licensed or sensitive market data and must follow access, retention,
and encryption controls. Packet payloads are never stored in telemetry labels.

## Where We Are Today

- We implemented TCP framing, TLS wrapping, bounded reconnects, multicast
  join/receive/leave, common contract, sequence analysis, journaling, telemetry,
  dashboard visibility, capture-filter generation, and offline tests.
- We have not yet integrated live vendor connectivity, vendor login/subscription
  messages, binary exchange protocols, entitlement validation, redundant A/B
  feed arbitration, durable sequence checkpoint recovery, or packet-capture
  execution.
