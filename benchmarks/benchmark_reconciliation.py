"""Measure reconciliation throughput, memory, and reference correctness."""

from __future__ import annotations

import argparse
import csv
import json
import platform
import resource
import sys
import time
from pathlib import Path

import pandas as pd

from benchmarks.generate_data import synthetic_equity_bars
from reconciliation_engine.reconciliation.equities import reconcile_equity_bars

ROOT = Path(__file__).resolve().parent


def run_case(rows: int) -> tuple[dict[str, object], dict[str, object]]:
    left = synthetic_equity_bars(rows)
    right = synthetic_equity_bars(rows, break_every=997)
    cpu_started = time.process_time()
    started = time.perf_counter()
    result = reconcile_equity_bars(left, right, close_tolerance=0.05)
    elapsed = time.perf_counter() - started
    cpu_seconds = time.process_time() - cpu_started
    expected_breaks = (rows - 1) // 997 + 1
    duplicate_keys = int(result.duplicated(["symbol", "trade_date"]).sum())
    actual_breaks = int((result["status"] == "BREAK").sum())
    correctness = {
        "input_rows_per_source": rows,
        "output_rows": len(result),
        "expected_breaks": expected_breaks,
        "actual_breaks": actual_breaks,
        "duplicate_keys": duplicate_keys,
        "lost_records": rows - len(result),
        "passed": len(result) == rows
        and actual_breaks == expected_breaks
        and duplicate_keys == 0,
    }
    usage = resource.getrusage(resource.RUSAGE_SELF)
    divisor = 1024 if sys.platform != "darwin" else 1024 * 1024
    measurement = {
        "benchmark": "reconciliation",
        "rows": rows,
        "workers": 1,
        "duration_seconds": round(elapsed, 6),
        "rows_per_second": round(rows / elapsed, 2),
        "peak_rss_mb": round(usage.ru_maxrss / divisor, 2),
        "cpu_utilization_percent": round(cpu_seconds / elapsed * 100, 2),
        "disk_growth_mb": 0.0,
    }
    return measurement, correctness


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", nargs="+", type=int, default=[100_000, 1_000_000])
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results")
    args = parser.parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)
    measurements, reports = zip(*(run_case(size) for size in args.sizes))
    with (args.results_dir / "benchmark_results.csv").open(
        "w", newline="", encoding="utf-8"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=list(measurements[0]))
        writer.writeheader()
        writer.writerows(measurements)
    (args.results_dir / "correctness_report.json").write_text(
        json.dumps(
            {"all_passed": all(item["passed"] for item in reports), "cases": reports},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (args.results_dir / "environment.json").write_text(
        json.dumps(
            {
                "python": sys.version,
                "platform": platform.platform(),
                "pandas": pd.__version__,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    if not all(item["passed"] for item in reports):
        raise SystemExit("correctness comparison failed")
    for item in measurements:
        print(item)


if __name__ == "__main__":
    main()
