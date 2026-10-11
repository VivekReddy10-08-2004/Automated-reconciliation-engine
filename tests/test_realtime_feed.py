from datetime import datetime, timedelta, timezone
import os
import threading
import unittest
from unittest.mock import patch
from uuid import uuid4

import zmq

from reconciliation_engine.realtime import get_endpoint
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


if __name__ == "__main__":
    unittest.main()
