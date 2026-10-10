import tempfile
import unittest
from pathlib import Path

import duckdb

from dashboard import load_dashboard_data


class RootDashboardTests(unittest.TestCase):
    def test_loads_latest_run_breaks_and_open_exceptions(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "reconciliation.duckdb"

            with duckdb.connect(str(db_path)) as connection:
                connection.execute(
                    """
                    CREATE TABLE pipeline_runs (
                        run_id VARCHAR,
                        started_at TIMESTAMP,
                        status VARCHAR,
                        yahoo_row_count BIGINT,
                        alpaca_row_count BIGINT,
                        match_count BIGINT,
                        break_count BIGINT
                    );
                    CREATE TABLE equity_reconciliation_results (
                        run_id VARCHAR,
                        symbol VARCHAR,
                        timestamp TIMESTAMP,
                        metric_name VARCHAR,
                        source_a_value DOUBLE,
                        source_b_value DOUBLE,
                        difference DOUBLE,
                        is_discrepancy BOOLEAN
                    );
                    CREATE TABLE equity_exceptions (
                        run_id VARCHAR,
                        symbol VARCHAR,
                        trade_date DATE,
                        yahoo_close DOUBLE,
                        alpaca_close DOUBLE,
                        close_difference DOUBLE,
                        severity VARCHAR,
                        status VARCHAR
                    );

                    INSERT INTO pipeline_runs VALUES
                        ('older', '2026-10-08', 'SUCCESS', 5, 5, 5, 0),
                        ('latest', '2026-10-09', 'SUCCESS_WITH_BREAKS', 5, 4, 3, 1);
                    INSERT INTO equity_reconciliation_results VALUES
                        ('latest', 'AAPL', '2026-10-09', 'close', 100, 101, -1, true),
                        ('latest', 'AAPL', '2026-10-09', 'volume', 10, 20, -10, true);
                    INSERT INTO equity_exceptions VALUES
                        ('latest', 'AAPL', '2026-10-09', 100, 101, -1, 'BREAK', 'OPEN'),
                        ('older', 'MSFT', '2026-10-08', 200, 201, -1, 'BREAK', 'OPEN');
                    """
                )

            data = load_dashboard_data(db_path)

            self.assertIsNotNone(data)
            assert data is not None
            self.assertEqual(data.latest_run["run_id"], "latest")
            self.assertEqual(len(data.largest_differences), 1)
            self.assertEqual(data.largest_differences[0]["symbol"], "AAPL")
            self.assertEqual(len(data.open_exceptions), 1)
            self.assertEqual(data.open_exceptions[0]["symbol"], "AAPL")


if __name__ == "__main__":
    unittest.main()
