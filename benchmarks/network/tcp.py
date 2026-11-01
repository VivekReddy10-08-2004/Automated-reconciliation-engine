"""Measure real loopback TCP framing throughput and receive latency."""

from __future__ import annotations

import argparse
import socket
import time

from benchmarks.network.common import (
    event_frame,
    loopback_server,
    percentile_ms,
    write_csv,
)
from reconciliation_engine.realtime.network_transport import TCPTransport


def benchmark_framing(framing: str, events: int) -> dict[str, object]:
    payload_bytes = 0

    def send_events(listener: socket.socket) -> None:
        nonlocal payload_bytes
        connection, _address = listener.accept()
        with connection:
            for sequence_number in range(events):
                frame = event_frame(sequence_number, framing=framing)
                payload_bytes += len(frame)
                connection.sendall(frame)

    host, port, sender, errors = loopback_server(send_events)
    transport = TCPTransport(
        host,
        port,
        framing=framing,
        reconnect_attempts=0,
        connect_timeout=2,
        read_timeout=5,
    )
    receive_ns: list[int] = []
    started = time.perf_counter()
    try:
        for expected_sequence in range(events):
            receive_started = time.perf_counter_ns()
            event = transport.receive()
            receive_ns.append(time.perf_counter_ns() - receive_started)
            if event.sequence_number != expected_sequence:
                raise AssertionError(
                    f"Expected sequence {expected_sequence}, got {event.sequence_number}"
                )
    finally:
        transport.close()
    elapsed = time.perf_counter() - started
    sender.join(timeout=10)
    if sender.is_alive():
        raise RuntimeError("Loopback sender did not finish")
    if errors:
        raise RuntimeError("Loopback sender failed") from errors[0]
    return {
        "framing": framing,
        "events": events,
        "duration_seconds": round(elapsed, 6),
        "events_per_second": round(events / elapsed, 2),
        "wire_mib_per_second": round(payload_bytes / elapsed / 1_048_576, 3),
        "receive_p50_ms": percentile_ms(receive_ns, 0.50),
        "receive_p95_ms": percentile_ms(receive_ns, 0.95),
        "receive_p99_ms": percentile_ms(receive_ns, 0.99),
        "sequence_errors": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument(
        "--framing",
        nargs="+",
        choices=("newline", "length-prefixed"),
        default=["newline", "length-prefixed"],
    )
    parser.add_argument(
        "--output",
        default="benchmarks/results/network/manual/tcp.csv",
    )
    args = parser.parse_args()
    if args.events <= 0:
        parser.error("--events must be positive")
    rows = [benchmark_framing(framing, args.events) for framing in args.framing]
    write_csv(args.output, rows)
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
