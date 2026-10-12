import unittest
from unittest.mock import Mock, patch

import pandas as pd

from reconciliation_engine.ingestion.yahoo_equities import fetch_equity_bars


class ProviderAlignmentTests(unittest.TestCase):
    @patch("reconciliation_engine.ingestion.yahoo_equities.yf.Ticker")
    def test_yahoo_equities_request_unadjusted_bars(self, ticker):
        history = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [101.0],
                "Low": [99.0],
                "Close": [100.5],
                "Volume": [1000],
            },
            index=pd.DatetimeIndex(["2026-10-09"], name="Date"),
        )
        instrument = Mock()
        instrument.history.return_value = history
        ticker.return_value = instrument

        result = fetch_equity_bars("AAPL", period="5d", interval="1d")

        instrument.history.assert_called_once_with(
            period="5d",
            interval="1d",
            auto_adjust=False,
            actions=False,
        )
        self.assertEqual(result.iloc[0]["close"], 100.5)


if __name__ == "__main__":
    unittest.main()
