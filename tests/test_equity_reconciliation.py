import pandas as pd
import unittest

from reconciliation_engine.reconciliation.equities import (
    RECONCILIATION_COLUMNS,
    reconcile_equity_bars,
)


class ReconciliationTests(unittest.TestCase):
    def test_reconcile_equity_bars_marks_matches_and_breaks(self):
        yahoo = pd.DataFrame({
            "symbol": ["AAPL", "AAPL"],
            "trade_date": ["2026-08-05", "2026-08-06"],
            "open": [100, 100], "high": [101, 101], "low": [99, 99],
            "close": [100.00, 100.00], "volume": [10, 20],
        })
        alpaca = yahoo.copy()
        alpaca["close"] = [100.00, 100.02]

        result = reconcile_equity_bars(yahoo, alpaca)

        self.assertEqual(result.columns.tolist(), RECONCILIATION_COLUMNS)
        self.assertEqual(result["status"].tolist(), ["MATCH", "BREAK"])

    def test_no_overlap_marks_each_missing_provider_observation_as_a_break(self):
        yahoo = pd.DataFrame({
            "symbol": ["AAPL"],
            "trade_date": ["2026-08-05"],
            "open": [100], "high": [101], "low": [99],
            "close": [100.00], "volume": [10],
        })
        alpaca = pd.DataFrame({
            "symbol": ["AAPL"],
            "trade_date": ["2026-08-06"],
            "open": [100], "high": [101], "low": [99],
            "close": [100.00], "volume": [10],
        })

        result = reconcile_equity_bars(yahoo, alpaca)

        self.assertEqual(len(result), 2)
        self.assertEqual(result["status"].tolist(), ["BREAK", "BREAK"])
        self.assertEqual(result["yahoo_close"].notna().sum(), 1)
        self.assertEqual(result["alpaca_close"].notna().sum(), 1)

    def test_malformed_input_raises_value_error(self):
        yahoo = pd.DataFrame({
            "symbol": ["AAPL"],
            "trade_date": ["2026-08-05"],
            "open": [0], "high": [0], "low": [0],
            "close": [100.00], "volume": [10],
        })
        alpaca = pd.DataFrame({
            "symbol": ["AAPL"],
            "trade_date": ["2026-08-05"],
            "open": [0], "high": [0], "low": [0],
            # Missing 'close' column
            "volume": [10],
        })

        with self.assertRaises(ValueError):
            reconcile_equity_bars(yahoo, alpaca)

    def test_duplicate_business_key_is_rejected(self):
        bars = pd.DataFrame(
            {
                "symbol": ["AAPL", "AAPL"],
                "trade_date": ["2026-08-05", "2026-08-05"],
                "open": [100.0, 100.0],
                "high": [101.0, 101.0],
                "low": [99.0, 99.0],
                "close": [100.0, 100.0],
                "volume": [10, 10],
            }
        )

        with self.assertRaisesRegex(ValueError, "duplicate"):
            reconcile_equity_bars(bars, bars.iloc[[0]])

    def test_null_non_numeric_and_inconsistent_ohlc_are_rejected(self):
        valid = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "trade_date": ["2026-08-05"],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.0],
                "volume": [10],
            }
        )
        cases = []
        null_close = valid.copy()
        null_close.loc[0, "close"] = None
        cases.append(null_close)
        string_volume = valid.copy()
        string_volume["volume"] = "10"
        cases.append(string_volume)
        invalid_high = valid.copy()
        invalid_high.loc[0, "high"] = 98.0
        cases.append(invalid_high)

        for frame in cases:
            with self.subTest(frame=frame):
                with self.assertRaises(ValueError):
                    reconcile_equity_bars(frame, valid)

    def test_column_order_and_extra_provider_fields_do_not_invalidate_data(self):
        bars = pd.DataFrame(
            {
                "volume": [10],
                "close": [100.0],
                "provider_note": ["unadjusted"],
                "low": [99.0],
                "high": [101.0],
                "open": [100.0],
                "trade_date": ["2026-08-05"],
                "symbol": ["aapl"],
            }
        )

        result = reconcile_equity_bars(bars, bars)

        self.assertEqual(result.iloc[0]["symbol"], "AAPL")
        self.assertEqual(result.iloc[0]["status"], "MATCH")

    def test_percentage_bands_absorb_small_cross_feed_differences(self):
        yahoo = pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT", "NVDA"],
                "trade_date": ["2026-08-05"] * 3,
                "open": [300.0] * 3,
                "high": [302.0] * 3,
                "low": [295.0] * 3,
                "close": [300.0] * 3,
                "volume": [1000] * 3,
            }
        )
        alpaca = yahoo.copy()
        alpaca["close"] = [299.80, 299.50, 298.00]

        result = reconcile_equity_bars(
            yahoo,
            alpaca,
            close_tolerance=0.05,
            warn_tolerance=0.01,
            break_pct_tolerance=0.005,
            warn_pct_tolerance=0.001,
        )

        self.assertEqual(result["status"].tolist(), ["MATCH", "WARN", "BREAK"])

if __name__ == "__main__":
    unittest.main()
