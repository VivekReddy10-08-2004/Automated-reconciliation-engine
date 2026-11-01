"""Shared helpers for the ADR 0007 network benchmark package."""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
import struct
import threading
from typing import Callable, Iterable


def event_frame(sequence_number: int, *, framing: str) -> bytes:
    payload = {
        "source": "BENCHMARK-VENDOR",
        "stream_id": "TRADES",
        "sequence_number": sequence_number,
        "symbol": "AAPL",
        "event_type": "TRADE",
        "price": 100.25,
        "quantity": 50,
        "exchange_timestamp": (
            datetime.now(timezone.utc) - timedelta(milliseconds=1)
        ).isoformat(),
    }
    encoded = json.dumps(payload, separators=(",", ":")).encode()
    if framing == "newline":
        return encoded + b"\n"
    if framing == "length-prefixed":
        return struct.pack("!I", len(encoded)) + encoded
    raise ValueError(f"Unsupported framing: {framing}")


def loopback_server(
    handler: Callable[[socket.socket], None],
) -> tuple[str, int, threading.Thread, list[BaseException]]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    listener.settimeout(10)
    host, port = listener.getsockname()
    errors: list[BaseException] = []

    def serve() -> None:
        try:
            handler(listener)
        except BaseException as error:  # propagated in the benchmark process
            errors.append(error)
        finally:
            listener.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    return host, port, thread, errors


def percentile_ms(samples_ns: list[int], percentile: float) -> float:
    if not samples_ns:
        return 0.0
    ordered = sorted(samples_ns)
    index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * percentile)))
    return round(ordered[index] / 1_000_000, 6)


def write_csv(path: str | Path, rows: Iterable[dict[str, object]]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    materialized = list(rows)
    if not materialized:
        raise ValueError("At least one benchmark row is required")
    with destination.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(materialized[0]))
        writer.writeheader()
        writer.writerows(materialized)


def write_json(path: str | Path, value: object) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
