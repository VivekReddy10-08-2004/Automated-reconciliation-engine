from datetime import datetime, timedelta, timezone
import os
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

import zmq

from reconciliation_engine.realtime import get_endpoint, get_scored_endpoint
from reconciliation_engine.realtime.anomaly_scorer import (
    ANNOTATION_FIELDS,
    score_and_publish_ticks,
)
from reconciliation_engine.realtime.publisher import (
    SIMULATED_PRICES,
    TICK_FIELDS,
    create_tick,
    publish_simulated_ticks,
    simulated_prices,
)
from reconciliation_engine.realtime.subscriber import receive_ticks, validate_tick


class RealtimeTickContractTests(unittest.TestCase):
    @patch("reconciliation_engine.realtime.load_dotenv")
    def test_endpoint_comes_from_the_environment(self, load_dotenv):
        with patch.dict(
            os.environ,
            {"REALTIME_ZMQ_ENDPOINT": "tcp://127.0.0.1:6000"},
        ):
            self.assertEqual(get_endpoint(), "tcp://127.0.0.1:6000")
        load_dotenv.assert_called_once_with("project.env", override=False)

    @patch("reconciliation_engine.realtime.load_dotenv")
    def test_scored_endpoint_comes_from_the_environment(self, load_dotenv):
        with patch.dict(
            os.environ,
            {"REALTIME_SCORED_ZMQ_ENDPOINT": "tcp://127.0.0.1:6001"},
        ):
            self.assertEqual(get_scored_endpoint(), "tcp://127.0.0.1:6001")
        load_dotenv.assert_called_once_with("project.env", override=False)

    def test_tick_has_exact_fields_utc_timestamps_and_calculated_latency(self):
        exchange_time = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
        receive_time = exchange_time + timedelta(milliseconds=12.5)

        tick = create_tick("aapl", 100.25, exchange_time, receive_time)

        self.assertEqual(tuple(tick), TICK_FIELDS)
        self.assertEqual(tick["symbol"], "AAPL")
        self.assertEqual(tick["latency_ms"], 12.5)
        self.assertEqual(validate_tick(tick), tick)
        self.assertTrue(tick["exchange_timestamp"].endswith("+00:00"))
        self.assertTrue(tick["received_timestamp"].endswith("+00:00"))

    def test_simulated_prices_repeat_deterministically(self):
        prices = simulated_prices()
        observed = [next(prices) for _ in range(len(SIMULATED_PRICES) + 2)]
        self.assertEqual(observed, [*SIMULATED_PRICES, *SIMULATED_PRICES[:2]])

    def test_negative_latency_is_rejected(self):
        receive_time = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
        with self.assertRaisesRegex(ValueError, "cannot be before"):
            create_tick(
                "AAPL",
                100,
                receive_time + timedelta(milliseconds=1),
                receive_time,
            )


class RealtimeZeroMqIntegrationTests(unittest.TestCase):
    def test_subscriber_receives_ten_valid_non_negative_simulated_ticks(self):
        endpoint = f"inproc://task-16-{uuid4()}"
        context = zmq.Context()
        publisher_errors = []

        def publish():
            try:
                publish_simulated_ticks(
                    endpoint,
                    count=10,
                    interval_seconds=0.005,
                    startup_delay_seconds=0.2,
                    context=context,
                    status_output=None,
                )
            except Exception as error:  # pragma: no cover - surfaced by assertion
                publisher_errors.append(error)

        publisher_thread = threading.Thread(target=publish, daemon=True)
        publisher_thread.start()
        try:
            ticks = receive_ticks(
                endpoint,
                count=10,
                timeout_ms=3000,
                context=context,
                output=None,
            )
        finally:
            publisher_thread.join(timeout=3)
            context.term()

        self.assertFalse(publisher_thread.is_alive())
        self.assertEqual(publisher_errors, [])
        self.assertEqual(len(ticks), 10)
        self.assertTrue(all(set(tick) == set(TICK_FIELDS) for tick in ticks))
        self.assertTrue(all(tick["latency_ms"] >= 0 for tick in ticks))

    def test_scorer_republishes_explained_anomaly(self):
        raw_endpoint = f"inproc://task-17-raw-{uuid4()}"
        scored_endpoint = f"inproc://task-17-scored-{uuid4()}"
        context = zmq.Context()
        raw_publisher = context.socket(zmq.PUB)
        scored_subscriber = context.socket(zmq.SUB)
        raw_publisher.setsockopt(zmq.LINGER, 0)
        scored_subscriber.setsockopt(zmq.LINGER, 0)
        scored_subscriber.setsockopt(zmq.SUBSCRIBE, b"")
        scored_subscriber.setsockopt(zmq.RCVTIMEO, 3000)
        errors = []

        raw_publisher.bind(raw_endpoint)

        def score():
            try:
                score_and_publish_ticks(
                    raw_endpoint,
                    scored_endpoint,
                    threshold_pct=1.0,
                    count=3,
                    timeout_ms=3000,
                    startup_delay_seconds=0.2,
                    context=context,
                    output=None,
                )
            except Exception as error:  # pragma: no cover - surfaced by assertion
                errors.append(error)

        scorer_thread = threading.Thread(target=score, daemon=True)
        scorer_thread.start()
        scored_subscriber.connect(scored_endpoint)
        time.sleep(0.3)
        try:
            for price in (100.0, 100.5, 102.0):
                now = datetime.now(timezone.utc)
                raw_publisher.send_json(
                    create_tick(
                        "AAPL",
                        price,
                        now - timedelta(milliseconds=5),
                        now,
                    )
                )
                time.sleep(0.02)
            annotated = [scored_subscriber.recv_json() for _ in range(3)]
        finally:
            scorer_thread.join(timeout=3)
            raw_publisher.close(linger=0)
            scored_subscriber.close(linger=0)
            context.term()

        self.assertFalse(scorer_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(all(set(TICK_FIELDS).issubset(item) for item in annotated))
        self.assertTrue(
            all(set(ANNOTATION_FIELDS).issubset(item) for item in annotated)
        )
        anomalies = [item for item in annotated if item["is_anomaly"]]
        self.assertEqual(len(anomalies), 1)
        self.assertIn("exceeded", anomalies[0]["reason"])


if __name__ == "__main__":
    unittest.main()
