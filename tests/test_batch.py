import unittest
from unittest.mock import DEFAULT, call, patch

import pandas as pd

from reconciliation_engine import batch


class BatchFailureHandlingTests(unittest.TestCase):
    def test_completed_runs_send_their_final_summary_once(self):
        source = pd.DataFrame({"trade_date": ["2026-10-09"]})

        for status, expected_status, expected_breaks in (
            ("MATCH", "SUCCESS", 0),
            ("BREAK", "SUCCESS_WITH_BREAKS", 1),
        ):
            with self.subTest(status=status):
                result = pd.DataFrame(
                    {
                        "status": [status],
                        "close_difference": [0.0 if status == "MATCH" else 1.0],
                    }
                )
                with patch.multiple(
                    batch,
                    initialize_database=DEFAULT,
                    get_pipeline_run=DEFAULT,
                    fetch_equity_sources=DEFAULT,
                    archive_equity_source=DEFAULT,
                    reconcile_equity_bars=DEFAULT,
                    archive_reconciliation_results=DEFAULT,
                    persist_equity_run=DEFAULT,
                    create_equity_exceptions=DEFAULT,
                    send_run_notification=DEFAULT,
                ) as mocks, patch("builtins.print"):
                    mocks["fetch_equity_sources"].return_value = (source, source)
                    mocks["archive_equity_source"].side_effect = [source, source]
                    mocks["reconcile_equity_bars"].return_value = result
                    mocks["create_equity_exceptions"].return_value = pd.DataFrame()
                    mocks["get_pipeline_run"].return_value = None
                    mocks["persist_equity_run"].return_value = True

                    summary = batch.main(
                        "test.duckdb",
                        control_date="2026-10-09",
                    )

                self.assertEqual(summary["status"], expected_status)
                self.assertEqual(summary["break_count"], expected_breaks)
                self.assertEqual(summary["symbols"], ["AAPL"])
                self.assertFalse(summary["already_completed"])
                mocks["fetch_equity_sources"].assert_called_once_with(
                    ("AAPL",),
                    batch.get_control_date("2026-10-09"),
                )
                mocks["reconcile_equity_bars"].assert_called_once_with(
                    source,
                    source,
                    close_tolerance=batch.DEFAULT_BREAK_TOLERANCE,
                    warn_tolerance=batch.DEFAULT_WARN_TOLERANCE,
                    break_pct_tolerance=batch.DEFAULT_BREAK_PCT_TOLERANCE,
                    warn_pct_tolerance=batch.DEFAULT_WARN_PCT_TOLERANCE,
                )
                mocks["send_run_notification"].assert_called_once_with(summary)

    @patch("reconciliation_engine.batch.send_run_notification")
    @patch("reconciliation_engine.batch.fetch_equity_sources")
    @patch("reconciliation_engine.batch.get_pipeline_run")
    @patch("reconciliation_engine.batch.initialize_database")
    def test_completed_logical_run_is_not_fetched_or_written_again(
        self,
        initialize_database,
        get_pipeline_run,
        fetch_equity_sources,
        send_run_notification,
    ):
        get_pipeline_run.return_value = {
            "run_id": batch.build_run_id(("AAPL",), "2026-10-09"),
            "status": "SUCCESS_WITH_BREAKS",
            "yahoo_row_count": 5,
            "alpaca_row_count": 3,
            "match_count": 1,
            "break_count": 4,
        }

        with patch("builtins.print"):
            summary = batch.main(
                "test.duckdb",
                symbols=["AAPL"],
                control_date="2026-10-09",
            )

        self.assertTrue(summary["already_completed"])
        fetch_equity_sources.assert_not_called()
        send_run_notification.assert_not_called()

    def test_run_id_is_stable_for_equivalent_universes(self):
        first = batch.build_run_id(("MSFT", "AAPL"), "2026-10-09")
        second = batch.build_run_id(("aapl", "msft"), "2026-10-09")

        self.assertEqual(first, second)

    @patch("reconciliation_engine.batch.send_run_notification")
    @patch("reconciliation_engine.batch.initialize_database")
    def test_unexpected_failure_sends_failed_summary(
        self,
        initialize_database,
        send_run_notification,
    ):
        initialize_database.side_effect = RuntimeError("database unavailable")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            batch.main("test.duckdb")

        summary = send_run_notification.call_args.args[0]
        self.assertEqual(summary["status"], "FAILED")
        self.assertEqual(summary["break_count"], 0)
        self.assertTrue(summary["run_id"])
        send_run_notification.assert_called_once()


class BatchSymbolConfigurationTests(unittest.TestCase):
    def test_cli_symbols_are_normalized_deduplicated_and_can_be_comma_separated(self):
        symbols = batch.get_reconciliation_symbols(
            [" msft ", "AAPL,nvda", "MSFT"]
        )

        self.assertEqual(symbols, ("MSFT", "AAPL", "NVDA"))

    def test_library_caller_can_pass_one_symbol_as_a_string(self):
        self.assertEqual(batch.normalize_symbols("msft"), ("MSFT",))

    @patch("reconciliation_engine.batch.load_dotenv")
    def test_environment_symbols_are_used_when_cli_is_omitted(self, load_dotenv):
        with patch.dict(
            "os.environ",
            {"RECONCILIATION_SYMBOLS": "spy, brk.b"},
            clear=False,
        ):
            symbols = batch.get_reconciliation_symbols()

        self.assertEqual(symbols, ("SPY", "BRK.B"))
        load_dotenv.assert_called_once_with("project.env", override=False)

    def test_invalid_symbol_is_rejected_before_provider_calls(self):
        with self.assertRaisesRegex(ValueError, "Invalid equity symbol"):
            batch.normalize_symbols(["AAPL;DROP TABLE"])

    @patch("reconciliation_engine.batch.fetch_stock_bars")
    @patch("reconciliation_engine.batch.fetch_yahoo_bars")
    def test_fetches_every_configured_symbol_from_both_providers(
        self,
        fetch_yahoo_bars,
        fetch_stock_bars,
    ):
        def provider_frame(symbol, *args, **kwargs):
            return pd.DataFrame(
                {
                    "symbol": [symbol],
                    "trade_date": ["2026-10-09"],
                    "open": [100.0],
                    "high": [101.0],
                    "low": [99.0],
                    "close": [100.5],
                    "volume": [1000],
                }
            )

        fetch_yahoo_bars.side_effect = provider_frame
        fetch_stock_bars.side_effect = provider_frame

        yahoo, alpaca = batch.fetch_equity_sources(
            ["MSFT", "NVDA"],
            "2026-10-09",
        )

        self.assertEqual(yahoo["symbol"].tolist(), ["MSFT", "NVDA"])
        self.assertEqual(alpaca["symbol"].tolist(), ["MSFT", "NVDA"])
        self.assertEqual(
            fetch_yahoo_bars.call_args_list,
            [
                call(
                    "MSFT",
                    start=batch.datetime(2026, 9, 30, tzinfo=batch.timezone.utc),
                    end=batch.datetime(2026, 10, 10, tzinfo=batch.timezone.utc),
                    interval="1d",
                ),
                call(
                    "NVDA",
                    start=batch.datetime(2026, 9, 30, tzinfo=batch.timezone.utc),
                    end=batch.datetime(2026, 10, 10, tzinfo=batch.timezone.utc),
                    interval="1d",
                ),
            ],
        )
        self.assertEqual(
            [item.args[0] for item in fetch_stock_bars.call_args_list],
            ["MSFT", "NVDA"],
        )


if __name__ == "__main__":
    unittest.main()
