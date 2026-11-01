"""Measure recovery through a real TCP disconnect and reconnect."""

from __future__ import annotations

import argparse
import socket
import time

from benchmarks.network.common import event_frame, loopback_server, write_json
from reconciliation_engine.realtime.network_transport import TCPTransport


def benchmark(
    events: int,
    disconnect_after: int,
    backoff_seconds: float,
) -> dict[str, object]:
    def send_across_connections(listener: socket.socket) -> None:
        first, _address = listener.accept()
        with first:
            for sequence_number in range(disconnect_after):
                first.sendall(event_frame(sequence_number, framing="newline"))
        second, _address = listener.accept()
        with second:
            for sequence_number in range(disconnect_after, events):
                second.sendall(event_frame(sequence_number, framing="newline"))

    host, port, sender, errors = loopback_server(send_across_connections)
    transport = TCPTransport(
        host,
        port,
        framing="newline",
        reconnect_attempts=3,
        reconnect_backoff_seconds=backoff_seconds,
        connect_timeout=2,
        read_timeout=5,
    )
    sequence_errors = 0
    received_events = 0
    recovery_seconds = 0.0
    started = time.perf_counter()
    try:
        for expected_sequence in range(events):
            receive_started = time.perf_counter()
            event = transport.receive()
            received_events += 1
            receive_elapsed = time.perf_counter() - receive_started
            if expected_sequence == disconnect_after:
                recovery_seconds = receive_elapsed
            if event.sequence_number != expected_sequence:
                sequence_errors += 1
    finally:
        transport.close()
    elapsed = time.perf_counter() - started
    sender.join(timeout=10)
    if sender.is_alive():
        raise RuntimeError("Reconnect sender did not finish")
    if errors:
        raise RuntimeError("Reconnect sender failed") from errors[0]
    return {
        "events": events,
        "disconnect_after": disconnect_after,
        "configured_backoff_ms": round(backoff_seconds * 1000, 3),
        "recovery_ms": round(recovery_seconds * 1000, 3),
        "duration_seconds": round(elapsed, 6),
        "events_per_second": round(events / elapsed, 2),
        "sequence_errors": sequence_errors,
        "events_lost": events - received_events,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--disconnect-after", type=int, default=5_000)
    parser.add_argument("--backoff-seconds", type=float, default=0.01)
    parser.add_argument(
        "--output",
        default="benchmarks/results/network/manual/reconnect.json",
    )
    args = parser.parse_args()
    if args.events <= 1:
        parser.error("--events must be greater than one")
    if not 0 < args.disconnect_after < args.events:
        parser.error("--disconnect-after must be between zero and --events")
    if args.backoff_seconds < 0:
        parser.error("--backoff-seconds cannot be negative")
    result = benchmark(args.events, args.disconnect_after, args.backoff_seconds)
    write_json(args.output, result)
    print(result)


if __name__ == "__main__":
    main()
