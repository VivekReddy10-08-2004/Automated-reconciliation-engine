"""Publish deterministic simulated equity ticks over a local ZeroMQ socket."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import itertools
import math
import time
from typing import Callable, Iterator

import zmq

from realtime import get_endpoint, validate_local_endpoint


TICK_FIELDS = (
    "symbol",
    "price",
    "exchange_timestamp",
    "received_timestamp",
    "latency_ms",
)
SIMULATED_PRICES = (100.00, 100.05, 99.98, 100.12, 100.08)
SIMULATED_LATENCY_MS = 5


def _utc_datetime(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def create_tick(
    symbol: str,
    price: float,
    exchange_timestamp: datetime,
    received_timestamp: datetime | None = None,
) -> dict[str, str | float]:
    """Create the exact five-field tick contract and calculate its latency."""
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string")
    if isinstance(price, bool) or not math.isfinite(float(price)) or float(price) <= 0:
        raise ValueError("price must be a positive finite number")

    exchange_time = _utc_datetime(exchange_timestamp, "exchange_timestamp")
    receive_time = _utc_datetime(
        received_timestamp or datetime.now(timezone.utc),
        "received_timestamp",
    )
    latency_ms = (receive_time - exchange_time).total_seconds() * 1000
    if latency_ms < 0:
        raise ValueError("received_timestamp cannot be before exchange_timestamp")

    return {
        "symbol": symbol.strip().upper(),
        "price": float(price),
        "exchange_timestamp": exchange_time.isoformat(),
        "received_timestamp": receive_time.isoformat(),
        "latency_ms": round(latency_ms, 3),
    }


def simulated_prices() -> Iterator[float]:
    """Yield a repeatable price sequence for offline demonstrations."""
    yield from itertools.cycle(SIMULATED_PRICES)


def publish_simulated_ticks(
    endpoint: str,
    *,
    symbol: str = "AAPL",
    interval_seconds: float = 0.25,
    count: int | None = None,
    startup_delay_seconds: float = 0.5,
    context: zmq.Context | None = None,
    status_output: Callable[[str], None] | None = print,
) -> int:
    """Publish simulated ticks until ``count`` is reached or Ctrl+C is pressed."""
    validate_local_endpoint(endpoint)
    if count is not None and count < 0:
        raise ValueError("count must be non-negative or None")
    if interval_seconds < 0 or startup_delay_seconds < 0:
        raise ValueError("timing values must be non-negative")

    owns_context = context is None
    active_context = context or zmq.Context()
    publisher = active_context.socket(zmq.PUB)
    publisher.setsockopt(zmq.LINGER, 0)
    published = 0

    try:
        publisher.bind(endpoint)
        if status_output:
            status_output(
                f"SIMULATED equity tick publisher ready for {symbol.strip().upper()}"
            )
        time.sleep(startup_delay_seconds)

        for price in simulated_prices():
            if count is not None and published >= count:
                break
            now = datetime.now(timezone.utc)
            tick = create_tick(
                symbol,
                price,
                now - timedelta(milliseconds=SIMULATED_LATENCY_MS),
                now,
            )
            publisher.send_json(tick)
            published += 1
            if count is None or published < count:
                time.sleep(interval_seconds)
    except KeyboardInterrupt:
        if status_output:
            status_output("SIMULATED publisher stopped")
    finally:
        publisher.close(linger=0)
        if owns_context:
            active_context.term()

    return published


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", default="AAPL")
    parser.add_argument("--interval", type=float, default=0.25)
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of ticks to publish; 0 publishes until Ctrl+C",
    )
    arguments = parser.parse_args()
    publish_simulated_ticks(
        get_endpoint(),
        symbol=arguments.symbol,
        interval_seconds=arguments.interval,
        count=None if arguments.count == 0 else arguments.count,
    )


if __name__ == "__main__":
    main()
