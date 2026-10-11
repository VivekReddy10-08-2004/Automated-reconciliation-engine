"""Subscribe to local ZeroMQ equity ticks and display their latency."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from typing import Callable

import zmq

from realtime import get_endpoint, validate_local_endpoint
from realtime.publisher import TICK_FIELDS


def _parse_utc_timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field_name} must be a valid ISO-8601 timestamp") from error
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    return parsed


def validate_tick(message: object) -> dict[str, str | float]:
    """Validate the exact tick schema and its derived latency value."""
    if not isinstance(message, dict) or set(message) != set(TICK_FIELDS):
        raise ValueError(f"tick fields must be exactly: {', '.join(TICK_FIELDS)}")
    if not isinstance(message["symbol"], str) or not message["symbol"].strip():
        raise ValueError("symbol must be a non-empty string")

    for field_name in ("price", "latency_ms"):
        value = message[field_name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field_name} must be numeric")
        if not math.isfinite(float(value)):
            raise ValueError(f"{field_name} must be finite")
    if float(message["price"]) <= 0:
        raise ValueError("price must be positive")
    if float(message["latency_ms"]) < 0:
        raise ValueError("latency_ms must be non-negative")

    exchange_time = _parse_utc_timestamp(
        message["exchange_timestamp"], "exchange_timestamp"
    )
    receive_time = _parse_utc_timestamp(
        message["received_timestamp"], "received_timestamp"
    )
    calculated_latency = (receive_time - exchange_time).total_seconds() * 1000
    if calculated_latency < 0 or not math.isclose(
        calculated_latency,
        float(message["latency_ms"]),
        abs_tol=0.001,
    ):
        raise ValueError("latency_ms does not match the message timestamps")
    return message


def receive_ticks(
    endpoint: str,
    *,
    count: int | None = None,
    timeout_ms: int | None = None,
    context: zmq.Context | None = None,
    output: Callable[[str], None] | None = print,
    source_label: str = "SIMULATED",
) -> list[dict[str, str | float]]:
    """Receive valid ticks until ``count`` is reached or Ctrl+C is pressed."""
    validate_local_endpoint(endpoint)
    if count is not None and count < 0:
        raise ValueError("count must be non-negative or None")

    owns_context = context is None
    active_context = context or zmq.Context()
    subscriber = active_context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    if timeout_ms is not None:
        if timeout_ms < 0:
            raise ValueError("timeout_ms must be non-negative or None")
        subscriber.setsockopt(zmq.RCVTIMEO, timeout_ms)

    received: list[dict[str, str | float]] = []
    received_count = 0
    try:
        subscriber.connect(endpoint)
        while count is None or received_count < count:
            try:
                tick = validate_tick(subscriber.recv_json())
            except zmq.Again as error:
                raise TimeoutError("Timed out waiting for a real-time tick") from error
            received_count += 1
            if count is not None:
                received.append(tick)
            if output:
                output(f"[{source_label}] {json.dumps(tick, separators=(',', ':'))}")
    except KeyboardInterrupt:
        if output:
            output("Subscriber stopped")
    finally:
        subscriber.close(linger=0)
        if owns_context:
            active_context.term()

    return received


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of ticks to receive; 0 subscribes until Ctrl+C",
    )
    parser.add_argument("--timeout-ms", type=int)
    arguments = parser.parse_args()
    receive_ticks(
        get_endpoint(),
        count=None if arguments.count == 0 else arguments.count,
        timeout_ms=arguments.timeout_ms,
    )


if __name__ == "__main__":
    main()
