"""Measure partition scheduling across workload sizes and worker counts."""

from __future__ import annotations

import argparse
import resource
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from reconciliation_engine.scaling.equity_ingestion import ingest_equity_partitions
from reconciliation_engine.storage.duckdb import initialize_database


class SyntheticAdapter:
    name = "SYNTHETIC"

    def fetch(self, symbols, start, end):
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "trade_date": date(2026, 10, 9),
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1000.0,
                }
                for symbol in symbols
            ]
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", type=int, default=5_000)
    parser.add_argument("--workers", nargs="+", type=int, default=[1, 4, 8])
    args = parser.parse_args()
    for workers in args.workers:
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "benchmark.duckdb")
            initialize_database(database)
            symbols = tuple(f"S{index}" for index in range(args.symbols))
            cpu_started = time.process_time()
            started = time.perf_counter()
            result = ingest_equity_partitions(
                run_id=f"workers-{workers}",
                control_date=date(2026, 10, 9),
                symbols=symbols,
                start=datetime(2026, 10, 1, tzinfo=timezone.utc),
                end=datetime(2026, 10, 10, tzinfo=timezone.utc),
                adapters={"SYNTHETIC": SyntheticAdapter()},
                db_path=database,
                data_root=Path(directory) / "data",
                bucket_count=8,
                max_workers=workers,
            )
            elapsed = time.perf_counter() - started
            cpu_seconds = time.process_time() - cpu_started
            divisor = 1024 if sys.platform != "darwin" else 1024 * 1024
            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / divisor
            disk = (
                sum(
                    path.stat().st_size
                    for path in Path(directory).rglob("*")
                    if path.is_file()
                )
                / 1_048_576
            )
            print(
                {
                    "workers": workers,
                    "rows": len(result["SYNTHETIC"]),
                    "duration_seconds": round(elapsed, 4),
                    "rows_per_second": round(args.symbols / elapsed, 2),
                    "peak_rss_mb": round(rss, 2),
                    "cpu_utilization_percent": round(cpu_seconds / elapsed * 100, 2),
                    "disk_growth_mb": round(disk, 2),
                }
            )


if __name__ == "__main__":
    main()
