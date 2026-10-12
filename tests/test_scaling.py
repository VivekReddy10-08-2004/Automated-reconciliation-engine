from datetime import date, datetime, timezone
from pathlib import Path
import tempfile
import threading
import time
import unittest

import duckdb
import pandas as pd

from reconciliation_engine.scaling.equity_ingestion import (
    PartitionExecutionError,
    ingest_equity_partitions,
)
from reconciliation_engine.scaling.partitions import (
    build_work_partitions,
    symbol_bucket,
)
from reconciliation_engine.storage.duckdb import initialize_database


def _bars(symbols: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": "2026-10-09",
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 1000.0,
            }
            for symbol in symbols
        ]
    )


class RecordingAdapter:
    def __init__(self, name: str, *, delay: float = 0.0, fail: bool = False):
        self.name = name
        self.delay = delay
        self.fail = fail
        self.calls = 0
        self.active = 0
        self.maximum_active = 0
        self._lock = threading.Lock()

    def fetch(self, symbols, start, end):
        with self._lock:
            self.calls += 1
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            if self.fail:
                raise RuntimeError("temporary provider failure")
            return _bars(symbols)
        finally:
            with self._lock:
                self.active -= 1


class WorkPartitionTests(unittest.TestCase):
    def test_partition_ids_are_stable_for_equivalent_inputs(self):
        arguments = {
            "run_id": "run-1",
            "control_date": date(2026, 10, 9),
            "instrument_type": "equity",
            "providers": ("YAHOO", "ALPACA"),
            "bucket_count": 16,
        }
        first = build_work_partitions(symbols=("MSFT", "AAPL"), **arguments)
        second = build_work_partitions(symbols=("aapl", "MSFT", "AAPL"), **arguments)

        self.assertEqual(first, second)
        self.assertTrue(all(partition.symbols for partition in first))
        self.assertEqual(symbol_bucket("aapl", 16), symbol_bucket("AAPL", 16))


class PartitionExecutionTests(unittest.TestCase):
    def _execute(self, directory, adapter, *, run_id="run-1", max_workers=2):
        database = str(Path(directory) / "reconciliation.duckdb")
        initialize_database(database)
        return ingest_equity_partitions(
            run_id=run_id,
            control_date=date(2026, 10, 9),
            symbols=("AAPL", "MSFT", "NVDA", "TSLA", "AMD", "META"),
            start=datetime(2026, 10, 1, tzinfo=timezone.utc),
            end=datetime(2026, 10, 10, tzinfo=timezone.utc),
            adapters={adapter.name: adapter},
            db_path=database,
            data_root=Path(directory) / "data",
            bucket_count=64,
            max_workers=max_workers,
        )

    def test_parallelism_is_bounded_and_manifest_is_completed(self):
        with tempfile.TemporaryDirectory() as directory:
            adapter = RecordingAdapter("TEST", delay=0.03)
            result = self._execute(directory, adapter, max_workers=2)

            self.assertEqual(set(result["TEST"]["symbol"]), {
                "AAPL", "MSFT", "NVDA", "TSLA", "AMD", "META"
            })
            self.assertLessEqual(adapter.maximum_active, 2)
            self.assertGreater(adapter.maximum_active, 1)
            with duckdb.connect(
                str(Path(directory) / "reconciliation.duckdb"), read_only=True
            ) as connection:
                statuses = connection.execute(
                    "SELECT DISTINCT status FROM work_partitions"
                ).fetchall()
                metric_count = connection.execute(
                    "SELECT count(*) FROM provider_request_metrics"
                ).fetchone()[0]
            self.assertEqual(statuses, [("SUCCESS",)])
            self.assertEqual(metric_count, adapter.calls)

    def test_retry_reuses_normalized_checkpoints_without_provider_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            first_adapter = RecordingAdapter("TEST")
            first = self._execute(directory, first_adapter)
            retry_adapter = RecordingAdapter("TEST", fail=True)
            retry = self._execute(directory, retry_adapter)

            self.assertEqual(len(first["TEST"]), len(retry["TEST"]))
            self.assertEqual(retry_adapter.calls, 0)
            with duckdb.connect(
                str(Path(directory) / "reconciliation.duckdb"), read_only=True
            ) as connection:
                attempts = connection.execute(
                    "SELECT DISTINCT attempts FROM work_partitions"
                ).fetchall()
                reused = connection.execute(
                    "SELECT bool_and(reused_archive) FROM provider_request_metrics "
                    "WHERE reused_archive"
                ).fetchone()[0]
            self.assertEqual(attempts, [(1,)])
            self.assertTrue(reused)

    def test_failed_partition_is_quarantined_and_can_be_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            failing = RecordingAdapter("TEST", fail=True)
            with self.assertRaises(PartitionExecutionError):
                self._execute(directory, failing, run_id="retryable-run")

            recovered = RecordingAdapter("TEST")
            result = self._execute(directory, recovered, run_id="retryable-run")
            self.assertEqual(len(result["TEST"]), 6)
            with duckdb.connect(
                str(Path(directory) / "reconciliation.duckdb"), read_only=True
            ) as connection:
                failed_attempts = connection.execute(
                    "SELECT count(*) FROM work_partitions WHERE attempts = 2 "
                    "AND status = 'SUCCESS'"
                ).fetchone()[0]
                quarantine_count = connection.execute(
                    "SELECT count(*) FROM quarantine_records"
                ).fetchone()[0]
            self.assertGreater(failed_attempts, 0)
            self.assertGreater(quarantine_count, 0)


if __name__ == "__main__":
    unittest.main()
