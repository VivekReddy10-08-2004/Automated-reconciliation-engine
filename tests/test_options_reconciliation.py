import unittest

import pandas as pd

from reconciliation_engine.reconciliation.options import (
    OUTPUT_COLUMNS,
    reconcile_option_chain,
    reconcile_option_sources,
)


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

    def test_three_provider_pairs_keep_massive_lineage(self):
        massive = self.yahoo.copy()
        massive["close"] = [3.0, 2.0]
        massive[["bid", "ask"]] = pd.NA

        result = reconcile_option_sources(
            {"YAHOO": self.yahoo, "ALPACA": self.yahoo.copy(), "MASSIVE": massive}
        )

        self.assertEqual(len(result), 6)
        self.assertEqual(
            set(zip(result["source_a"], result["source_b"])),
            {("YAHOO", "ALPACA"), ("YAHOO", "MASSIVE"), ("ALPACA", "MASSIVE")},
        )
        self.assertEqual((result["status"] == "BREAK").sum(), 4)


    def test_matching_uses_full_option_identity(self):
        alpaca_put = self.alpaca.iloc[[0]].copy()
        alpaca_put["option_type"] = "P"

        result = reconcile_option_chain(self.yahoo.iloc[[0]], alpaca_put)

        self.assertEqual(len(result), 2)
        self.assertEqual(set(result["status"]), {"BREAK"})
        self.assertEqual(
            set(result["break_reason"]),
            {"MISSING_ALPACA_CONTRACT", "MISSING_YAHOO_CONTRACT"},
        )
        self.assertEqual(result.columns.tolist(), OUTPUT_COLUMNS)

    def test_unavailable_provider_flags_every_one_sided_contract(self):
        unavailable_alpaca = pd.DataFrame(columns=OPTION_COLUMNS)

        result = reconcile_option_chain(self.yahoo, unavailable_alpaca)

        self.assertEqual(len(result), len(self.yahoo))
        self.assertEqual(set(result["status"]), {"BREAK"})
        self.assertEqual(set(result["break_reason"]), {"MISSING_ALPACA_CONTRACT"})
        self.assertEqual(result.columns.tolist(), OUTPUT_COLUMNS)

    def test_alpaca_only_contract_keeps_its_contract_symbol(self):
        alpaca_only = self.alpaca.iloc[[0]].copy()
        result = reconcile_option_chain(
            pd.DataFrame(columns=OPTION_COLUMNS),
            alpaca_only,
        )

        self.assertEqual(result.iloc[0]["contract_symbol"], "SPY261218C00500000")
        self.assertEqual(result.iloc[0]["break_reason"], "MISSING_YAHOO_CONTRACT")

    def test_duplicate_contract_key_is_rejected(self):
        duplicated = pd.concat([self.yahoo.iloc[[0]], self.yahoo.iloc[[0]]])

        with self.assertRaisesRegex(ValueError, "duplicate contract keys"):
            reconcile_option_chain(duplicated, self.alpaca)

    def test_different_snapshot_dates_are_flagged_as_one_sided_contracts(self):
        alpaca = self.alpaca.iloc[[0]].copy()
        alpaca["trade_date"] = "2026-10-06"

        result = reconcile_option_chain(self.yahoo.iloc[[0]], alpaca)

        self.assertEqual(len(result), 2)
        self.assertEqual(set(result["status"]), {"BREAK"})


if __name__ == "__main__":
    unittest.main()
