# ADR 0007 network benchmarks

This package separates network-adapter measurements from batch reconciliation,
API load, and recovery benchmarks.

| Module | Measurement | Output |
| --- | --- | --- |
| `tcp` | Real loopback TCP throughput and p50/p95/p99 receive time by framing | `tcp.csv` |
| `sequence` | Source/stream classifier throughput and correctness | `sequence.json` |
| `memory` | Continuous-monitor retention, heap, RSS, and throughput | `memory.csv` |
| `reconnect` | Real disconnect recovery, loss, and ordering | `reconnect.json` |
| `suite` | Runs all four workloads and records host metadata | `environment.json` plus all files above |

For a development smoke test:

```sh
python -m benchmarks.network.suite --profile quick
```

For the published workload sizes:

```sh
python -m benchmarks.network.suite --profile full
```

The profiles write to separate `benchmarks/results/network/quick/` and
`benchmarks/results/network/full/` directories. Individual modules write to
`benchmarks/results/network/manual/` by default and expose their own `--help`
and output options. Use `--output-dir` on the suite to retain named host or date
runs separately.

The suite measures local adapter behavior. It does not model vendor distance,
production interface contention, multicast loss, or TLS handshake overhead.
