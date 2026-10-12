import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import pandas as pd

from reconciliation_engine.smoke import (
    REQUIRED_MODULES,
    REQUIRED_TABLES,
    check_configuration,
    check_database,
    check_imports,
    run_smoke_checks,
)
from reconciliation_engine.storage.duckdb import (
    initialize_database,
    save_equity_bars,
    save_pipeline_run,
)


class SmokeCheckTests(unittest.TestCase):
    def _write_environment(self, directory: str, **overrides: str) -> Path:
        values = {
            "ALPACA_API_KEY": "test-api-key",
            "ALPACA_SECRET_KEY": "test-secret-key",
            "MASSIVE_API_KEY": "test-massive-key",
            "RECONCILIATION_SYMBOLS": "MSFT,NVDA",
            "REALTIME_ZMQ_ENDPOINT": "tcp://127.0.0.1:5555",
            "REALTIME_SCORED_ZMQ_ENDPOINT": "tcp://127.0.0.1:5556",
        }
        values.update(overrides)
        env_path = Path(directory) / "project.env"
        env_path.write_text(
            "".join(f"{key}={value}\n" for key, value in values.items()),
            encoding="utf-8",
        )
        env_path.chmod(0o600)
        return env_path

    def test_import_check_covers_release_modules(self):
        self.assertEqual(check_imports(), REQUIRED_MODULES)

    def test_configuration_check_validates_without_returning_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = self._write_environment(directory)
            with patch.dict(os.environ, {}, clear=True):
                result = check_configuration(env_path)

        self.assertEqual(result["symbols"], ("MSFT", "NVDA"))
        self.assertTrue(result["credentials_configured"])
        self.assertTrue(result["massive_credentials_configured"])
        self.assertNotIn("api_key", result)
        self.assertNotIn("secret_key", result)
        self.assertNotIn("massive_api_key", result)

    def test_configuration_check_rejects_massive_placeholder(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = self._write_environment(
                directory,
                MASSIVE_API_KEY="your_massive_api_key_here",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, "Massive.*placeholders"):
                    check_configuration(env_path)

    def test_configuration_check_rejects_placeholder_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = self._write_environment(
                directory,
                ALPACA_API_KEY="your_alpaca_api_key_here",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(RuntimeError, "placeholders"):
                    check_configuration(env_path)

    def test_configuration_check_rejects_shared_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = self._write_environment(
                directory,
                REALTIME_SCORED_ZMQ_ENDPOINT="tcp://127.0.0.1:5555",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaisesRegex(ValueError, "must be different"):
                    check_configuration(env_path)

    def test_database_check_creates_and_reads_required_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "release.duckdb"

            tables = check_database(db_path)

            self.assertEqual(set(tables), REQUIRED_TABLES)
            with duckdb.connect(str(db_path), read_only=True) as connection:
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM pipeline_runs").fetchone()[0],
                    0,
                )

    def test_database_accepts_multiple_symbols_for_one_source_and_date(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "release.duckdb"
            initialize_database(str(db_path))
            save_pipeline_run(
                "run-1",
                "2026-10-11T20:00:00+00:00",
                "2026-10-11T20:01:00+00:00",
                "SUCCESS",
                2,
                2,
                2,
                0,
                str(db_path),
            )
            bars = pd.DataFrame(
                {
                    "symbol": ["AAPL", "MSFT"],
                    "trade_date": ["2026-10-10", "2026-10-10"],
                    "open": [100.0, 200.0],
                    "high": [101.0, 201.0],
                    "low": [99.0, 199.0],
                    "close": [100.5, 200.5],
                    "volume": [1000, 2000],
                }
            )

            save_equity_bars(bars, "YAHOO", "run-1", str(db_path))

            with duckdb.connect(str(db_path), read_only=True) as connection:
                rows = connection.execute(
                    "SELECT symbol FROM equity_bars_staging ORDER BY symbol"
                ).fetchall()
            self.assertEqual(rows, [("AAPL",), ("MSFT",)])

    def test_database_migrates_the_legacy_staging_primary_key(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "legacy.duckdb"
            with duckdb.connect(str(db_path)) as connection:
                connection.execute(
                    """
                    CREATE TABLE pipeline_runs (run_id VARCHAR PRIMARY KEY);
                    CREATE TABLE equity_bars_staging (
                        symbol VARCHAR, run_id VARCHAR, source VARCHAR,
                        timestamp TIMESTAMP, open FLOAT, high FLOAT, low FLOAT,
                        close FLOAT, volume BIGINT,
                        PRIMARY KEY (run_id, source, timestamp),
                        FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
                    );
                    """
                )

            initialize_database(str(db_path))

            with duckdb.connect(str(db_path), read_only=True) as connection:
                primary_key = connection.execute(
                    """
                    SELECT constraint_text FROM duckdb_constraints()
                    WHERE table_name = 'equity_bars_staging'
                      AND constraint_type = 'PRIMARY KEY'
                    """
                ).fetchone()[0]
            self.assertIn("symbol", primary_key)

    def test_full_smoke_check_uses_no_provider_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = self._write_environment(directory)
            with patch.dict(os.environ, {}, clear=True), patch(
                "reconciliation_engine.ingestion.yahoo_equities.yf.Ticker"
            ) as yahoo, patch(
                "reconciliation_engine.ingestion.alpaca_equities.StockHistoricalDataClient"
            ) as alpaca:
                result = run_smoke_checks(env_path=env_path)

        self.assertEqual(result["module_count"], len(REQUIRED_MODULES))
        yahoo.assert_not_called()
        alpaca.assert_not_called()


if __name__ == "__main__":
    unittest.main()
