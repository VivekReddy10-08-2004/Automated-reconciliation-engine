import unittest
from unittest.mock import Mock

import pandas as pd

from reconciliation_engine.ingestion.massive import MassiveClient
from reconciliation_engine.ingestion.massive_equities import (
    fetch_massive_equity_bars,
    fetch_massive_grouped_daily,
)
from reconciliation_engine.ingestion.massive_futures import fetch_massive_futures_bars
from reconciliation_engine.ingestion.massive_options import (
    fetch_massive_option_bars,
    fetch_massive_option_contracts,
)


class NoWaitLimiter:
    def wait(self):
        return None


class MassiveClientTests(unittest.TestCase):
    def test_key_is_sent_in_header_and_not_url(self):
        response = Mock(status_code=200)
        response.json.return_value = {"status": "OK", "results": []}
        session = Mock()
        session.get.return_value = response
        client = MassiveClient(
            "secret-value", session=session, limiter=NoWaitLimiter()
        )

        client.get_json("/v3/reference/tickers", {"limit": 1})

        url = session.get.call_args.args[0]
        kwargs = session.get.call_args.kwargs
        self.assertNotIn("secret-value", url)
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer secret-value")

    def test_rejects_non_massive_absolute_url(self):
        with self.assertRaises(ValueError):
            MassiveClient._url("https://example.com/steal")


class MassiveEquityTests(unittest.TestCase):
    def test_normalizes_single_symbol_daily_bars(self):
        client = Mock(spec=MassiveClient)
        client.get_json.return_value = {
            "results": [
                {"t": 1760313600000, "o": 10, "h": 12, "l": 9, "c": 11, "v": 100}
            ]
        }
        result = fetch_massive_equity_bars(
            "aapl", "2025-10-13", "2025-10-14", client=client
        )
        self.assertEqual(result.loc[0, "symbol"], "AAPL")
        self.assertEqual(result.loc[0, "close"], 11)
        self.assertEqual(client.get_json.call_args.args[1]["adjusted"], "false")

    def test_grouped_daily_keeps_each_market_symbol(self):
        client = Mock(spec=MassiveClient)
        client.get_json.return_value = {
            "results": [
                {"T": "AAPL", "t": 1760313600000, "o": 1, "h": 2, "l": 1, "c": 2, "v": 3},
                {"T": "MSFT", "t": 1760313600000, "o": 4, "h": 5, "l": 4, "c": 5, "v": 6},
            ]
        }
        result = fetch_massive_grouped_daily("2025-10-13", client=client)
        self.assertEqual(result["symbol"].tolist(), ["AAPL", "MSFT"])


class MassiveOptionsTests(unittest.TestCase):
    def test_contract_reference_is_normalized(self):
        client = Mock(spec=MassiveClient)
        client.iter_results.return_value = iter(
            [{
                "ticker": "O:AAPL260116C00200000",
                "underlying_ticker": "AAPL",
                "expiration_date": "2026-01-16",
                "strike_price": 200,
                "contract_type": "call",
                "exercise_style": "american",
                "shares_per_contract": 100,
                "primary_exchange": "BATO",
            }]
        )
        result = fetch_massive_option_contracts(
            "aapl", expiration="2026-01-16", client=client
        )
        self.assertEqual(result.loc[0, "option_type"], "C")
        self.assertEqual(result.loc[0, "strike"], 200)

    def test_free_plan_bars_make_unavailable_quote_fields_explicit(self):
        client = Mock(spec=MassiveClient)
        client.get_json.return_value = {
            "results": [{"t": 1760313600000, "c": 4.5, "v": 25}]
        }
        contract = {
            "contract_symbol": "O:AAPL260116C00200000",
            "underlying": "AAPL",
            "expiry": "2026-01-16",
            "strike": 200,
            "option_type": "C",
        }
        result = fetch_massive_option_bars(
            contract, "2025-10-13", "2025-10-14", client=client
        )
        self.assertEqual(result.loc[0, "close"], 4.5)
        self.assertTrue(pd.isna(result.loc[0, "bid"]))
        self.assertTrue(pd.isna(result.loc[0, "open_interest"]))


class MassiveFuturesTests(unittest.TestCase):
    def test_preserves_exchange_session_date_and_settlement(self):
        client = Mock(spec=MassiveClient)
        client.iter_results.return_value = iter(
            [{
                "ticker": "GCZ6",
                "session_end_date": "2026-10-13",
                "window_start": 1791849600000000000,
                "open": 2600,
                "high": 2620,
                "low": 2590,
                "close": 2610,
                "settlement_price": 2612,
                "volume": 1000,
                "transactions": 500,
            }]
        )
        result = fetch_massive_futures_bars(
            "GCZ6", "2026-10-12", "2026-10-13", client=client
        )
        self.assertEqual(result.loc[0, "session_date"], "2026-10-13")
        self.assertEqual(result.loc[0, "settlement_price"], 2612)
        self.assertEqual(str(result.loc[0, "window_start"].tz), "UTC")


if __name__ == "__main__":
    unittest.main()
