from pathlib import Path
import tempfile
import unittest

import duckdb
import pandas as pd

from reconciliation_engine.reconciliation.exceptions import create_equity_exceptions
from reconciliation_engine.storage.duckdb import (
    initialize_database,
    persist_equity_run,
)


def _bars(close: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "trade_date": ["2026-10-09"],
            "open": [100.0],
            "high": [102.0],
            "low": [99.0],
            "close": [close],
            "volume": [1000],
        }
    )


def _reconciliation() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["AAPL"],
            "trade_date": ["2026-10-09"],
            "yahoo_close": [100.0],
            "alpaca_close": [101.0],
            "close_difference": [-1.0],
            "yahoo_volume": [1000],
            "alpaca_volume": [1000],
            "volume_difference": [0],
            "status": ["BREAK"],
        }
    )


class AtomicPersistenceTests(unittest.TestCase):
    def test_complete_run_commits_once_and_retry_is_a_noop(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "reconciliation.duckdb")
            initialize_database(db_path)
            reconciliation = _reconciliation()
            exceptions = create_equity_exceptions(reconciliation, "stable-run")
            arguments = {
                "run_id": "stable-run",
                "started_at": "2026-10-09T20:00:00+00:00",
                "finished_at": "2026-10-09T20:01:00+00:00",
                "status": "SUCCESS_WITH_BREAKS",
                "yahoo_bars": _bars(100.0),
                "alpaca_bars": _bars(101.0),
                "reconciliation": reconciliation,
                "exceptions": exceptions,
                "db_path": db_path,
            }

            self.assertTrue(persist_equity_run(**arguments))
            self.assertFalse(persist_equity_run(**arguments))

            with duckdb.connect(db_path, read_only=True) as connection:
                counts = [
                    connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                    for table in (
                        "pipeline_runs",
                        "equity_bars_staging",
                        "equity_reconciliation_results",
                        "equity_exceptions",
                    )
                ]
            self.assertEqual(counts, [1, 2, 2, 1])

    def test_failure_rolls_back_run_and_every_child_table(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = str(Path(directory) / "reconciliation.duckdb")
            initialize_database(db_path)
            reconciliation = pd.concat(
                [_reconciliation(), _reconciliation()],
                ignore_index=True,
            )
            exceptions = create_equity_exceptions(reconciliation, "failed-run")

            with self.assertRaises(duckdb.ConstraintException):
                persist_equity_run(
                    run_id="failed-run",
                    started_at="2026-10-09T20:00:00+00:00",
                    finished_at="2026-10-09T20:01:00+00:00",
                    status="SUCCESS_WITH_BREAKS",
                    yahoo_bars=_bars(100.0),
                    alpaca_bars=_bars(101.0),
                    reconciliation=reconciliation,
                    exceptions=exceptions,
                    db_path=db_path,
                )

            with duckdb.connect(db_path, read_only=True) as connection:
                counts = [
                    connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                    for table in (
                        "pipeline_runs",
                        "equity_bars_staging",
                        "equity_reconciliation_results",
                        "equity_exceptions",
                    )
                ]
            self.assertEqual(counts, [0, 0, 0, 0])


if __name__ == "__main__":
    unittest.main()
