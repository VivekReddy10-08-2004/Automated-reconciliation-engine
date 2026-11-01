"""Measure sequence-classification speed and cross-source correctness."""

from __future__ import annotations

import argparse
import time

from benchmarks.network.common import write_json
from reconciliation_engine.realtime.network_transport import SequenceTracker


def benchmark(events: int, sources: int) -> dict[str, object]:
    tracker = SequenceTracker()
    errors = 0
    started = time.perf_counter()
    for index in range(events):
        source_index = index % sources
        sequence_number = index // sources
        observation = tracker.observe(
            f"VENDOR-{source_index}",
            "TRADES",
            sequence_number,
        )
        expected = "FIRST" if sequence_number == 0 else "IN_ORDER"
        if observation.status != expected:
            errors += 1
    elapsed = time.perf_counter() - started
    return {
        "events": events,
        "sources": sources,
        "shared_stream_id": "TRADES",
        "duration_seconds": round(elapsed, 6),
        "events_per_second": round(events / elapsed, 2),
        "classification_errors": errors,
        "accuracy_percent": round((events - errors) / events * 100, 6),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--sources", type=int, default=8)
    parser.add_argument(
        "--output",
        default="benchmarks/results/network/manual/sequence.json",
    )
    args = parser.parse_args()
    if args.events <= 0 or args.sources <= 0:
        parser.error("--events and --sources must be positive")
    result = benchmark(args.events, args.sources)
    write_json(args.output, result)
    print(result)


if __name__ == "__main__":
    main()
