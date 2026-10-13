from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

import duckdb

from reconciliation_engine.alerts import (
    Alert,
    AlertDispatcher,
    AlertThresholds,
    evaluate_operational_alerts,
)
from reconciliation_engine.storage.duckdb import initialize_database


class AlertDispatcherTests(unittest.TestCase):
    def test_delivers_structured_alert_and_suppresses_duplicate_during_cooldown(self):
        now = [100.0]
        session = Mock()
        session.post.return_value.raise_for_status.return_value = None
        dispatcher = AlertDispatcher(
            "https://alerts.example.test/hook",
            cooldown_seconds=60,
            clock=lambda: now[0],
            session=session,
        )
        alert = Alert(
            "provider_latency",
            "warning",
            "Yahoo latency is high",
            {"provider": "YAHOO", "max_latency_ms": 12000},
        )

        self.assertTrue(dispatcher.dispatch(alert))
        self.assertFalse(dispatcher.dispatch(alert))
        now[0] = 161.0
        self.assertTrue(dispatcher.dispatch(alert))

        self.assertEqual(session.post.call_count, 2)
        payload = session.post.call_args.kwargs["json"]
        self.assertEqual(payload["alert"]["rule"], "provider_latency")
        self.assertNotIn("webhook", str(payload).lower())


class OperationalAlertRuleTests(unittest.TestCase):
    def test_aggregates_sla_partition_failure_rate_and_latency_alerts(self):
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "reconciliation.duckdb")
            initialize_database(database)
            with duckdb.connect(database) as connection:
                connection.execute(
                    """
                    INSERT INTO work_partitions (
                        partition_id, run_id, control_date, instrument_type,
                        symbol_bucket, provider, symbols_json, status, attempts
                    ) VALUES (
                        'partition-1', 'run-1', current_date, 'EQUITY', 1,
                        'YAHOO', '["AAPL"]', 'FAILED', 1
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO provider_request_metrics (
                        partition_id, run_id, provider, instrument_type,
                        symbol_bucket, requested_symbols, returned_rows,
                        latency_ms, reused_archive, outcome_status, error_type,
                        recorded_at
                    ) VALUES
                        ('partition-1', 'run-1', 'YAHOO', 'EQUITY', 1, 1, 0,
                         12000, false, 'FAILED', 'TimeoutError', current_timestamp),
                        ('partition-2', 'run-1', 'YAHOO', 'EQUITY', 2, 1, 1,
                         100, false, 'SUCCESS', NULL, current_timestamp)
                    """
                )
                connection.execute(
                    """
                    INSERT INTO pipeline_runs (run_id) VALUES ('run-1');
                    INSERT INTO equity_exceptions (
                        exception_id, run_id, instrument_type, symbol,
                        trade_date, severity, priority, status, owner,
                        reason_code, due_at, created_at
                    ) VALUES (
                        'exception-1', 'run-1', 'EQUITY', 'AAPL', current_date,
                        'BREAK', 'HIGH', 'OPEN', 'UNASSIGNED',
                        'PRICE_DIFFERENCE', current_timestamp - INTERVAL '1 hour',
                        current_timestamp - INTERVAL '25 hours'
                    )
                    """
                )

            dispatcher = Mock()
            alerts = evaluate_operational_alerts(
                database,
                dispatcher=dispatcher,
                thresholds=AlertThresholds(
                    lookback_minutes=15,
                    provider_failure_rate=0.20,
                    provider_latency_ms=10_000,
                ),
            )

        self.assertEqual(
            {alert.rule for alert in alerts},
            {
                "overdue_exceptions",
                "failed_partitions",
                "provider_failure_rate",
                "provider_latency",
            },
        )
        self.assertEqual(dispatcher.dispatch.call_count, 4)


if __name__ == "__main__":
    unittest.main()
