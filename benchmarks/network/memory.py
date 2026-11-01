"""Verify that continuous monitoring does not retain the event stream."""

from __future__ import annotations

import argparse
import gc
import resource
import sys
import time
import tracemalloc
from datetime import datetime, timezone

from benchmarks.network.common import write_csv
from reconciliation_engine.realtime.network_transport import (
    MarketEvent,
    MarketEventTransport,
    monitor_transport,
)


class SyntheticContinuousTransport(MarketEventTransport):
    def __init__(self, events: int) -> None:
        self.events_to_send = events
        self.timestamp = datetime.now(timezone.utc)

    def receive(self) -> MarketEvent:
        raise NotImplementedError

    def events(self, limit=None):
        for sequence_number in range(self.events_to_send):
            identity = f"benchmark-{sequence_number}"
            yield MarketEvent(
                event_id=identity,
                source="BENCHMARK-VENDOR",
                stream_id="TRADES",
                transport="TCP",
                sequence_number=sequence_number,
                symbol="AAPL",
                event_type="TRADE",
                price=100.25,
                quantity=50.0,
                exchange_timestamp=self.timestamp,
                received_timestamp=self.timestamp,
                correlation_id=identity,
            )

    def close(self) -> None:
        return None


def benchmark(events: int) -> dict[str, object]:
    gc.collect()
    tracemalloc.start()
    heap_before, _peak_before = tracemalloc.get_traced_memory()
    started = time.perf_counter()
    observed = monitor_transport(
        SyntheticContinuousTransport(events),
        count=None,
        journal_db_path=None,
        output=None,
    )
    elapsed = time.perf_counter() - started
    gc.collect()
    heap_after, heap_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    divisor = 1024 if sys.platform != "darwin" else 1024 * 1024
    return {
        "events": events,
        "duration_seconds": round(elapsed, 6),
        "events_per_second": round(events / elapsed, 2),
        "retained_events": len(observed),
        "heap_growth_kib": round((heap_after - heap_before) / 1024, 3),
        "traced_peak_mib": round(heap_peak / 1_048_576, 3),
        "process_peak_rss_mib": round(
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / divisor,
            3,
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", nargs="+", type=int, default=[10_000, 100_000])
    parser.add_argument(
        "--output",
        default="benchmarks/results/network/manual/memory.csv",
    )
    args = parser.parse_args()
    if any(value <= 0 for value in args.events):
        parser.error("--events values must be positive")
    rows = [benchmark(value) for value in args.events]
    write_csv(args.output, rows)
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
