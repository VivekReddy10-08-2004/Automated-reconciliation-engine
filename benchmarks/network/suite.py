"""Run the complete ADR 0007 benchmark set with one command."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path

from benchmarks.network.common import write_csv, write_json
from benchmarks.network.memory import benchmark as benchmark_memory
from benchmarks.network.reconnect import benchmark as benchmark_reconnect
from benchmarks.network.sequence import benchmark as benchmark_sequence
from benchmarks.network.tcp import benchmark_framing

PROFILES = {
    "quick": {
        "tcp_events": 1_000,
        "sequence_events": 100_000,
        "memory_events": (1_000, 10_000),
        "reconnect_events": 1_000,
        "disconnect_after": 500,
    },
    "full": {
        "tcp_events": 10_000,
        "sequence_events": 1_000_000,
        "memory_events": (10_000, 100_000),
        "reconnect_events": 10_000,
        "disconnect_after": 5_000,
    },
}


def run_suite(profile: str, output_dir: str | Path | None = None) -> dict[str, object]:
    settings = PROFILES[profile]
    destination = Path(output_dir or f"benchmarks/results/network/{profile}")
    tcp_results = [
        benchmark_framing(framing, settings["tcp_events"])
        for framing in ("newline", "length-prefixed")
    ]
    sequence_result = benchmark_sequence(settings["sequence_events"], 8)
    reconnect_result = benchmark_reconnect(
        settings["reconnect_events"],
        settings["disconnect_after"],
        0.01,
    )
    memory_results = [benchmark_memory(events) for events in settings["memory_events"]]
    environment = {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "python": sys.version,
        "platform": platform.platform(),
        "processor": platform.processor() or "unreported",
        "logical_cpu_count": os.cpu_count(),
        "settings": settings,
        "scope": "Local loopback and in-process adapter measurements",
    }
    write_csv(destination / "tcp.csv", tcp_results)
    write_json(destination / "sequence.json", sequence_result)
    write_json(destination / "reconnect.json", reconnect_result)
    write_csv(destination / "memory.csv", memory_results)
    write_json(destination / "environment.json", environment)
    return {
        "profile": profile,
        "output_dir": str(destination),
        "tcp": tcp_results,
        "sequence": sequence_result,
        "reconnect": reconnect_result,
        "memory": memory_results,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=tuple(PROFILES), default="quick")
    parser.add_argument(
        "--output-dir",
        help="Override benchmarks/results/network/<profile>",
    )
    args = parser.parse_args()
    print(json.dumps(run_suite(args.profile, args.output_dir), indent=2))


if __name__ == "__main__":
    main()
