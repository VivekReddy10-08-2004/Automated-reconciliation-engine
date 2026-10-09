import tempfile
import unittest
from pathlib import Path

import duckdb

from Dashboard.dashboard import load_dashboard_data


class DashboardAnalyticsTests(unittest.TestCase):
    def test_loads_break_analytics_from_read_only_database(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "reconciliation.duckdb"
            with duckdb.connect(str(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE pipeline_runs (
                        run_id VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
                        status VARCHAR, yahoo_row_count BIGINT, alpaca_row_count BIGINT,
                        match_count BIGINT, break_count BIGINT
                    );
                    CREATE TABLE equity_reconciliation_results (
                        run_id VARCHAR, symbol VARCHAR, timestamp TIMESTAMP,
                        metric_name VARCHAR, source_a_value DOUBLE,
                        source_b_value DOUBLE, difference DOUBLE,
                        is_discrepancy BOOLEAN
                    );
                    CREATE TABLE equity_exceptions (status VARCHAR);
                    INSERT INTO pipeline_runs VALUES
                        ('run-1', '2026-10-08', '2026-10-08', 'SUCCESS_WITH_BREAKS', 2, 2, 1, 1);
                    INSERT INTO equity_reconciliation_results VALUES
                        ('run-1', 'AAPL', '2026-10-08', 'close', 100, 101, -1, true);
                    INSERT INTO equity_exceptions VALUES ('OPEN');
                    """
                )

            data = load_dashboard_data(db_path)

            self.assertEqual(data.latest_run.iloc[0]["run_id"], "run-1")
            self.assertEqual(data.run_trend.iloc[0]["break_rate_pct"], 50.0)
            self.assertEqual(data.top_breaks.iloc[0]["symbol"], "AAPL")
            self.assertEqual(data.repeat_offenders.iloc[0]["affected_runs"], 1)
            self.assertEqual(data.exception_status.iloc[0]["status"], "OPEN")


if __name__ == "__main__":
    unittest.main()
