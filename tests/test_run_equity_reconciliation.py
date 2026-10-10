import unittest
from unittest.mock import DEFAULT, patch

import pandas as pd

import run_equity_reconciliation


class RunFailureHandlingTests(unittest.TestCase):
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
                    run_equity_reconciliation,
                    initialize_database=DEFAULT,
                    reconciliation=DEFAULT,
                    save_pipeline_run=DEFAULT,
                    save_equity_bars=DEFAULT,
                    save_reconciliation_results=DEFAULT,
                    create_equity_exceptions=DEFAULT,
                    save_exceptions=DEFAULT,
                    send_run_notification=DEFAULT,
                ) as mocks, patch("builtins.print"):
                    mocks["reconciliation"].return_value = (source, source, result)
                    mocks["create_equity_exceptions"].return_value = pd.DataFrame()

                    summary = run_equity_reconciliation.main("test.duckdb")

                self.assertEqual(summary["status"], expected_status)
                self.assertEqual(summary["break_count"], expected_breaks)
                mocks["send_run_notification"].assert_called_once_with(summary)

    @patch("run_equity_reconciliation.send_run_notification")
    @patch("run_equity_reconciliation.initialize_database")
    def test_unexpected_failure_sends_failed_summary(
        self,
        initialize_database,
        send_run_notification,
    ):
        initialize_database.side_effect = RuntimeError("database unavailable")

        with self.assertRaisesRegex(RuntimeError, "database unavailable"):
            run_equity_reconciliation.main("test.duckdb")

        summary = send_run_notification.call_args.args[0]
        self.assertEqual(summary["status"], "FAILED")
        self.assertEqual(summary["break_count"], 0)
        self.assertTrue(summary["run_id"])
        send_run_notification.assert_called_once()


if __name__ == "__main__":
    unittest.main()
