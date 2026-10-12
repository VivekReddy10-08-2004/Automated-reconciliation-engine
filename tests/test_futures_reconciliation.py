import unittest

import pandas as pd

from reconciliation_engine.reconciliation.futures import reconcile_futures_bars


class FuturesReconciliationTests(unittest.TestCase):
    def test_explicit_contract_mapping_aligns_exchange_session(self):
        yahoo = pd.DataFrame(
            {"symbol": ["GC=F"], "trade_date": ["2026-10-13"], "close": [2610.0]}
        )
        massive = pd.DataFrame(
            {
                "symbol": ["GCZ6"], "session_date": ["2026-10-13"],
                "close": [2610.5], "settlement_price": [2611.0],
            }
        )

        result = reconcile_futures_bars(
            yahoo, massive, {"GC=F": "GCZ6"}, close_tolerance=1.0
        )

        self.assertEqual(result.loc[0, "status"], "MATCH")
        self.assertEqual(result.loc[0, "massive_contract"], "GCZ6")
        self.assertEqual(result.loc[0, "massive_settlement_price"], 2611.0)

    def test_one_sided_session_is_a_break(self):
        yahoo = pd.DataFrame(
            {"symbol": ["GC=F"], "trade_date": ["2026-10-13"], "close": [2610.0]}
        )
        massive = pd.DataFrame(
            columns=["symbol", "session_date", "close", "settlement_price"]
        )

        result = reconcile_futures_bars(yahoo, massive, {"GC=F": "GCZ6"})

        self.assertEqual(result.loc[0, "status"], "BREAK")
        self.assertEqual(result.loc[0, "break_reason"], "MISSING_MASSIVE_OBSERVATION")

    def test_mapping_is_mandatory(self):
        empty_yahoo = pd.DataFrame(columns=["symbol", "trade_date", "close"])
        empty_massive = pd.DataFrame(
            columns=["symbol", "session_date", "close", "settlement_price"]
        )
        with self.assertRaisesRegex(ValueError, "mapping is required"):
            reconcile_futures_bars(empty_yahoo, empty_massive, {})


if __name__ == "__main__":
    unittest.main()
