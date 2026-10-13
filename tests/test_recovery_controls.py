import tempfile
import unittest
from contextlib import nullcontext
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb
import pandas as pd
import requests

from reconciliation_engine.ingestion.massive import MassiveClient
from reconciliation_engine.scaling.equity_ingestion import (
    PartitionExecutionError,
    ingest_equity_partitions,
)
from reconciliation_engine.storage.duckdb import initialize_database
from reconciliation_engine.storage.snapshot import publish_snapshot


class NoWaitLimiter:
    def wait(self):
        return None


class RecoveryControlTests(unittest.TestCase):
    @patch("reconciliation_engine.ingestion.massive.time.sleep")
    def test_timeout_and_429_are_retried(self, sleep):
        success = Mock(status_code=200)
        success.json.return_value = {"results": []}
        rate_limited = Mock(status_code=429, headers={"Retry-After": "0"})
        rate_limited.raise_for_status.side_effect = requests.HTTPError("429")
        session = Mock()
        session.get.side_effect = [requests.Timeout("slow"), rate_limited, success]
        client = MassiveClient(
            "key", session=session, limiter=NoWaitLimiter(), max_retries=3
        )

        self.assertEqual(client.get_json("/v2/aggs"), {"results": []})
        self.assertEqual(session.get.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_atomic_snapshot_is_independent_of_later_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "active.duckdb"
            snapshot = Path(directory) / "published.duckdb"
            with duckdb.connect(str(source)) as connection:
                connection.execute("CREATE TABLE values_table AS SELECT 1 AS value")
            publish_snapshot(source, snapshot)
            with duckdb.connect(str(source)) as connection:
                connection.execute("INSERT INTO values_table VALUES (2)")
            with duckdb.connect(str(snapshot), read_only=True) as connection:
                self.assertEqual(
                    connection.execute("SELECT * FROM values_table").fetchall(), [(1,)]
                )

    def test_corrupt_payload_and_disk_failure_are_quarantined(self):
        good = pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "trade_date": "2026-10-09",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "volume": 1000.0,
                }
            ]
        )
        for run_id, frame, write_error in (
            ("corrupt", pd.DataFrame({"symbol": ["AAPL"]}), None),
            ("disk-failure", good, OSError("disk full")),
        ):
            with (
                self.subTest(run_id=run_id),
                tempfile.TemporaryDirectory() as directory,
            ):
                database = str(Path(directory) / "state.duckdb")
                initialize_database(database)
                adapter = Mock()
                adapter.fetch.return_value = frame
                context = (
                    patch(
                        "reconciliation_engine.scaling.equity_ingestion.write_partition_once",
                        side_effect=write_error,
                    )
                    if write_error
                    else nullcontext()
                )
                with context, self.assertRaises(PartitionExecutionError):
                    ingest_equity_partitions(
                        run_id=run_id,
                        control_date=date(2026, 10, 9),
                        symbols=("AAPL",),
                        start=datetime(2026, 10, 1, tzinfo=timezone.utc),
                        end=datetime(2026, 10, 10, tzinfo=timezone.utc),
                        adapters={"TEST": adapter},
                        db_path=database,
                        data_root=Path(directory) / "data",
                        bucket_count=1,
                        max_workers=1,
                    )
                with duckdb.connect(database, read_only=True) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT count(*) FROM quarantine_records"
                        ).fetchone()[0],
                        1,
                    )


if __name__ == "__main__":
    unittest.main()
