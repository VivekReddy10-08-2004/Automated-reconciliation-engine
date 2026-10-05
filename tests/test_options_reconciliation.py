import unittest

import pandas as pd

from Ingestion.options_reconciliation import OUTPUT_COLUMNS, reconcile_option_chain


OPTION_COLUMNS = [
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "contract_symbol",
    "trade_date",
    "close",
    "bid",
    "ask",
    "volume",
    "open_interest",
]


class OptionReconciliationTests(unittest.TestCase):
    def setUp(self):
        self.yahoo = pd.DataFrame(
            [
                [
                    "SPY",
                    "2026-12-18",
                    500.0,
                    "C",
                    "SPY261218C00500000",
                    "2026-10-05",
                    2.50,
                    2.45,
                    2.55,
                    10,
                    100,
                ],
                [
                    "SPY",
                    "2026-12-18",
                    505.0,
                    "C",
                    "SPY261218C00505000",
                    "2026-10-05",
                    1.50,
                    1.45,
                    1.55,
                    20,
                    200,
                ],
            ],
            columns=OPTION_COLUMNS,
        )

        self.alpaca = self.yahoo.copy()
        self.alpaca["close"] = [2.53, 1.60]
        self.alpaca["bid"] = [2.48, 1.54]
        self.alpaca["ask"] = [2.58, 1.66]

    def test_reconciliation_marks_match_and_break(self):
        result = reconcile_option_chain(self.yahoo, self.alpaca)

        self.assertEqual(result.columns.tolist(), OUTPUT_COLUMNS)
        self.assertEqual(result["status"].tolist(), ["MATCH", "BREAK"])
        self.assertAlmostEqual(result.iloc[0]["close_difference"], -0.03)
        self.assertAlmostEqual(result.iloc[1]["close_difference"], -0.10)

    def test_matching_uses_full_option_identity(self):
        alpaca_put = self.alpaca.iloc[[0]].copy()
        alpaca_put["option_type"] = "P"

        result = reconcile_option_chain(self.yahoo.iloc[[0]], alpaca_put)

        self.assertTrue(result.empty)
        self.assertEqual(result.columns.tolist(), OUTPUT_COLUMNS)

    def test_unavailable_provider_returns_empty_result(self):
        unavailable_alpaca = pd.DataFrame(columns=OPTION_COLUMNS)

        result = reconcile_option_chain(self.yahoo, unavailable_alpaca)

        self.assertTrue(result.empty)
        self.assertEqual(result.columns.tolist(), OUTPUT_COLUMNS)


if __name__ == "__main__":
    unittest.main()
