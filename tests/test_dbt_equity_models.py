import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import duckdb
import pandas as pd

from reconciliation_engine.reconciliation.equities import reconcile_equity_sources


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT = PROJECT_ROOT / "dbt"


class DbtEquityModelTests(unittest.TestCase):
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_directory.name) / "reconciliation.duckdb"
        self.dbt = shutil.which("dbt")
        if self.dbt is None:
            candidate = Path(os.sys.executable).with_name("dbt")
            self.dbt = str(candidate) if candidate.exists() else None
        if self.dbt is None:
            self.skipTest("dbt is not installed in the active environment")

    def tearDown(self):
        self.temp_directory.cleanup()

    def _create_source_table(self, rows):
        with duckdb.connect(str(self.db_path)) as connection:
            connection.execute(
                """
                create table equity_bars_staging (
                    symbol varchar,
                    run_id varchar,
                    source varchar,
                    timestamp timestamp,
                    open double,
                    high double,
                    low double,
                    close double,
                    volume bigint
                )
                """
            )
            connection.executemany(
                "insert into equity_bars_staging values (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

    def _run_dbt(self, *arguments):
        environment = os.environ.copy()
        environment["RECONCILIATION_DB_PATH"] = str(self.db_path)
        return subprocess.run(
            [
                self.dbt,
                *arguments,
                "--project-dir",
                str(DBT_PROJECT),
                "--profiles-dir",
                str(DBT_PROJECT),
            ],
            cwd=PROJECT_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_dbt_build_matches_python_reference(self):
        rows = []
        yahoo_close = [100.00, 100.00, 100.00]
        alpaca_close = [100.00, 99.80, 99.00]
        massive_close = [100.00, 100.00, 100.00]
        dates = ["2026-10-07", "2026-10-08", "2026-10-09"]
        for source, closes in (
            ("YAHOO", yahoo_close),
            ("ALPACA", alpaca_close),
            ("MASSIVE", massive_close),
        ):
            for trade_date, close in zip(dates, closes):
                rows.append(
                    ("AAPL", "saved-run", source, trade_date, 99, 101, 98, close, 1000)
                )
        self._create_source_table(rows)

        completed = self._run_dbt("build")
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

        with duckdb.connect(str(self.db_path), read_only=True) as connection:
            actual = connection.execute(
                """
                select symbol, trade_date, source_a, source_b,
                       source_a_close, source_b_close, close_difference,
                       source_a_volume, source_b_volume, volume_difference,
                       status, break_reason
                from fct_equity_reconciliation
                where run_id = 'saved-run'
                order by source_a, source_b, trade_date
                """
            ).fetchdf()
        actual["trade_date"] = pd.to_datetime(actual["trade_date"]).dt.date

        def provider_frame(closes):
            return pd.DataFrame(
                {
                    "symbol": ["AAPL"] * 3,
                    "trade_date": dates,
                    "open": [99] * 3,
                    "high": [101] * 3,
                    "low": [98] * 3,
                    "close": closes,
                    "volume": [1000] * 3,
                }
            )

        expected = reconcile_equity_sources(
            {
                "YAHOO": provider_frame(yahoo_close),
                "ALPACA": provider_frame(alpaca_close),
                "MASSIVE": provider_frame(massive_close),
            },
            close_tolerance=0.05,
            warn_tolerance=0.01,
            break_pct_tolerance=0.005,
            warn_pct_tolerance=0.001,
        )
        expected = expected.sort_values(
            ["source_a", "source_b", "trade_date"]
        ).reset_index(drop=True)
        self.assertEqual(
            actual["break_reason"].fillna("MISSING").tolist(),
            expected["break_reason"].fillna("MISSING").tolist(),
        )
        pd.testing.assert_frame_equal(
            actual.drop(columns="break_reason"),
            expected.drop(columns="break_reason"),
            check_dtype=False,
        )
        self.assertEqual(set(actual["status"]), {"MATCH", "WARN", "BREAK"})

    def test_duplicate_source_keys_fail_the_dbt_test(self):
        duplicated = (
            "AAPL", "duplicate-run", "YAHOO", "2026-10-09", 99, 101, 98, 100, 1000
        )
        self._create_source_table([duplicated, duplicated])

        completed = self._run_dbt(
            "test", "--select", "source_equity_bars_unique_by_business_key"
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("source_equity_bars_unique_by_business_key", completed.stdout)

    def test_dbt_preserves_one_sided_provider_observations_as_breaks(self):
        self._create_source_table(
            [
                (
                    "AAPL", "missing-run", "YAHOO", "2026-10-08",
                    99, 101, 98, 100, 1000,
                ),
                (
                    "AAPL", "missing-run", "ALPACA", "2026-10-09",
                    99, 101, 98, 101, 1100,
                ),
            ]
        )

        completed = self._run_dbt("build")
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)

        with duckdb.connect(str(self.db_path), read_only=True) as connection:
            actual = connection.execute(
                """
                select trade_date, source_a, source_b, source_a_close,
                       source_b_close, status
                from fct_equity_reconciliation
                where run_id = 'missing-run'
                order by trade_date, source_a, source_b
                """
            ).fetchdf()

        self.assertEqual(len(actual), 6)
        self.assertEqual(set(actual["status"]), {"BREAK"})
        self.assertGreater(actual["source_a_close"].isna().sum(), 0)
        self.assertGreater(actual["source_b_close"].isna().sum(), 0)


if __name__ == "__main__":
    unittest.main()
