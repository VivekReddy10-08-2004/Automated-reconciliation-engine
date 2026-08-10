import pandas as pd
import unittest

from Ingestion.equity_reconciliation import RECONCILIATION_COLUMNS, reconcile_equity_bars


class ReconciliationTests(unittest.TestCase):
    def test_reconcile_equity_bars_marks_matches_and_breaks(self):
        yahoo = pd.DataFrame({
            "symbol": ["AAPL", "AAPL"],
            "trade_date": ["2026-08-05", "2026-08-06"],
            "open": [0, 0], "high": [0, 0], "low": [0, 0],
            "close": [100.00, 100.00], "volume": [10, 20],
        })
        alpaca = yahoo.copy()
        alpaca["close"] = [100.00, 100.02]

        result = reconcile_equity_bars(yahoo, alpaca)

        self.assertEqual(result.columns.tolist(), RECONCILIATION_COLUMNS)
        self.assertEqual(result["status"].tolist(), ["MATCH", "BREAK"])


if __name__ == "__main__":
    unittest.main()
