from datetime import datetime, timedelta, timezone
import io
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock
from uuid import uuid4

import duckdb
import pandas as pd
from rich.console import Console
import zmq

from reconciliation_engine.dashboard import (
    BatchDashboardState,
    DashboardData,
    RealtimeDashboardData,
    RealtimeFeedSubscriber,
    RealtimeStateTracker,
    bounded_refresh_interval,
    load_batch_dashboard_state,
    load_dashboard_data,
    render_control_room,
)
from reconciliation_engine.realtime.anomaly_scorer import score_tick
from reconciliation_engine.realtime.publisher import create_tick


def _raw_tick(price: float) -> dict[str, str | float]:
    received = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)
    return create_tick(
        "AAPL",
        price,
        received - timedelta(milliseconds=7),
        received,
    )


def _dashboard_data() -> DashboardData:
    return DashboardData(
        latest_run=pd.DataFrame(
            [
                {
                    "run_id": "run-demo",
                    "started_at": "2026-10-11",
                    "finished_at": "2026-10-11",
                    "status": "SUCCESS_WITH_BREAKS",
                    "yahoo_row_count": 3,
                    "alpaca_row_count": 3,
                    "match_count": 1,
                    "warn_count": 1,
                    "break_count": 1,
                }
            ]
        ),
        run_trend=pd.DataFrame(),
        top_breaks=pd.DataFrame(
            [
                {
                    "symbol": "AAPL",
                    "timestamp": "2026-10-11",
                    "metric_name": "close",
                    "source_a_value": 100.0,
                    "source_b_value": 102.0,
                    "difference": -2.0,
                }
            ]
        ),
        repeat_offenders=pd.DataFrame(),
        exception_status=pd.DataFrame(),
    )


def _render_text(
    batch: BatchDashboardState,
    realtime: RealtimeDashboardData,
) -> str:
    stream = io.StringIO()
    console = Console(file=stream, width=180, color_system=None)
    console.print(render_control_room(batch, realtime))
    return stream.getvalue()


class DashboardAnalyticsTests(unittest.TestCase):
    def test_loads_break_analytics_from_read_only_database(self):
        with tempfile.TemporaryDirectory() as directory:
            db_path = Path(directory) / "reconciliation.duckdb"
            with duckdb.connect(str(db_path)) as conn:
                conn.execute(
                    """
                    CREATE TABLE pipeline_runs (
                        run_id VARCHAR, started_at TIMESTAMP, finished_at TIMESTAMP,
                        status VARCHAR, yahoo_row_count BIGINT, alpaca_row_count BIGINT,
                        match_count BIGINT, break_count BIGINT
                    );
                    CREATE TABLE equity_reconciliation_results (
                        run_id VARCHAR, symbol VARCHAR, timestamp TIMESTAMP,
                        metric_name VARCHAR, source_a_value DOUBLE,
                        source_b_value DOUBLE, difference DOUBLE,
                        is_discrepancy BOOLEAN
                    );
                    CREATE TABLE equity_exceptions (status VARCHAR);
                    CREATE TABLE fct_equity_reconciliation (
                        run_id VARCHAR, status VARCHAR
                    );
                    INSERT INTO pipeline_runs VALUES
                        ('run-1', '2026-10-08', '2026-10-08', 'SUCCESS_WITH_BREAKS', 2, 2, 1, 1);
                    INSERT INTO equity_reconciliation_results VALUES
                        ('run-1', 'AAPL', '2026-10-08', 'close', 100, 101, -1, true);
                    INSERT INTO equity_exceptions VALUES ('OPEN');
                    INSERT INTO fct_equity_reconciliation VALUES
                        ('run-1', 'MATCH'), ('run-1', 'WARN'), ('run-1', 'BREAK');
                    """
                )

            data = load_dashboard_data(db_path)

            self.assertEqual(data.latest_run.iloc[0]["run_id"], "run-1")
            self.assertEqual(data.latest_run.iloc[0]["match_count"], 1)
            self.assertEqual(data.latest_run.iloc[0]["warn_count"], 1)
            self.assertEqual(data.latest_run.iloc[0]["break_count"], 1)
            self.assertEqual(data.run_trend.iloc[0]["break_rate_pct"], 50.0)
            self.assertEqual(data.top_breaks.iloc[0]["symbol"], "AAPL")
            self.assertEqual(data.repeat_offenders.iloc[0]["affected_runs"], 1)
            self.assertEqual(data.exception_status.iloc[0]["status"], "OPEN")

    def test_missing_database_becomes_batch_warning(self):
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / "missing.duckdb"

            state = load_batch_dashboard_state(missing)

            self.assertIsNone(state.data)
            self.assertIn("Batch data unavailable", state.warning)
            self.assertFalse(missing.exists())


class RealtimeDashboardTests(unittest.TestCase):
    def test_tracks_latest_tick_recent_anomalies_and_staleness(self):
        now = [0.0]
        tracker = RealtimeStateTracker(
            stale_after_seconds=5,
            anomaly_window_seconds=10,
            clock=lambda: now[0],
        )
        tracker.observe(score_tick(_raw_tick(100.0), None))
        now[0] = 1.0
        anomaly = score_tick(_raw_tick(102.0), 100.0, threshold_pct=1.0)
        tracker.observe(anomaly)

        live = tracker.snapshot()

        self.assertEqual(live.status, "LIVE")
        self.assertEqual(live.latest_tick["price"], 102.0)
        self.assertEqual(live.recent_anomaly_count, 1)
        self.assertIn("exceeded", live.latest_anomaly_reason)

        now[0] = 12.0
        stale = tracker.snapshot()
        self.assertEqual(stale.status, "STALE")
        self.assertEqual(stale.recent_anomaly_count, 0)
        self.assertIn("unavailable", stale.warning)

    def test_silent_feed_becomes_unavailable(self):
        now = [0.0]
        tracker = RealtimeStateTracker(stale_after_seconds=5, clock=lambda: now[0])

        self.assertEqual(tracker.snapshot().status, "WAITING")
        now[0] = 5.0
        self.assertEqual(tracker.snapshot().status, "UNAVAILABLE")

    def test_subscriber_uses_only_a_sub_socket(self):
        context = Mock()
        socket = Mock()
        context.socket.return_value = socket

        subscriber = RealtimeFeedSubscriber("inproc://dashboard-read-only", context=context)
        subscriber.close()

        context.socket.assert_called_once_with(zmq.SUB)
        socket.connect.assert_called_once_with("inproc://dashboard-read-only")
        self.assertFalse(socket.send.called)
        self.assertFalse(socket.send_json.called)
        context.term.assert_not_called()

    def test_subscriber_receives_annotated_tick_without_publishing(self):
        endpoint = f"inproc://task-20-{uuid4()}"
        context = zmq.Context()
        publisher = context.socket(zmq.PUB)
        publisher.setsockopt(zmq.LINGER, 0)
        publisher.bind(endpoint)
        subscriber = RealtimeFeedSubscriber(endpoint, context=context)
        annotated = score_tick(_raw_tick(102.0), 100.0)
        time.sleep(0.1)

        state = subscriber.poll()
        for _ in range(10):
            publisher.send_json(annotated)
            time.sleep(0.02)
            state = subscriber.poll()
            if state.ticks_received:
                break

        subscriber.close()
        publisher.close(linger=0)
        context.term()

        self.assertEqual(state.status, "LIVE")
        self.assertEqual(state.latest_tick["symbol"], "AAPL")
        self.assertEqual(state.recent_anomaly_count, 1)

    def test_refresh_rate_is_bounded_to_twice_per_second(self):
        self.assertEqual(bounded_refresh_interval(0.1), 0.5)
        self.assertEqual(bounded_refresh_interval(2.0), 2.0)

    def test_both_panels_available(self):
        realtime = RealtimeDashboardData(
            status="LIVE",
            latest_tick=score_tick(_raw_tick(102.0), 100.0),
            ticks_received=3,
            recent_anomaly_count=1,
            latest_anomaly_reason="Price threshold exceeded",
        )

        output = _render_text(BatchDashboardState(_dashboard_data()), realtime)

        self.assertIn("Batch reconciliation", output)
        self.assertIn("Real-time anomaly feed", output)
        self.assertIn("run-demo", output)
        self.assertIn("AAPL", output)
        self.assertIn("Price threshold exceeded", output)

    def test_batch_unavailable_does_not_hide_realtime_data(self):
        realtime = RealtimeDashboardData(
            status="LIVE",
            latest_tick=score_tick(_raw_tick(102.0), 100.0),
            ticks_received=3,
            recent_anomaly_count=1,
            latest_anomaly_reason="Price threshold exceeded",
        )

        output = _render_text(
            BatchDashboardState(None, "Batch data unavailable"),
            realtime,
        )

        self.assertIn("Batch data unavailable", output)
        self.assertIn("AAPL", output)

    def test_realtime_unavailable_does_not_hide_batch_data(self):
        realtime = RealtimeDashboardData(
            status="UNAVAILABLE",
            latest_tick=None,
            ticks_received=0,
            recent_anomaly_count=0,
            latest_anomaly_reason=None,
            warning="Real-time feed unavailable",
        )

        output = _render_text(BatchDashboardState(_dashboard_data()), realtime)

        self.assertIn("run-demo", output)
        self.assertIn("Top five breaks", output)
        self.assertIn("Real-time feed unavailable", output)


if __name__ == "__main__":
    unittest.main()
