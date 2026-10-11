from datetime import datetime, timedelta, timezone
import unittest

from reconciliation_engine.realtime.anomaly_scorer import (
    ANNOTATION_FIELDS,
    RuleBasedAnomalyScorer,
    isolation_forest_flags,
    score_tick,
)
from reconciliation_engine.realtime.publisher import TICK_FIELDS, create_tick


def tick(symbol: str, price: float) -> dict[str, str | float]:
    received = datetime(2026, 10, 11, 12, 0, tzinfo=timezone.utc)
    return create_tick(
        symbol,
        price,
        received - timedelta(milliseconds=5),
        received,
    )


class AnomalyRuleTests(unittest.TestCase):
    def test_normal_price_change_is_not_flagged(self):
        annotated = score_tick(tick("AAPL", 100.5), 100.0, threshold_pct=1.0)

        self.assertEqual(set(annotated), {*TICK_FIELDS, *ANNOTATION_FIELDS})
        self.assertEqual(annotated["pct_change"], 0.5)
        self.assertFalse(annotated["is_anomaly"])
        self.assertIn("did not exceed", annotated["reason"])

    def test_synthetic_outlier_is_flagged_with_an_explanation(self):
        annotated = score_tick(tick("AAPL", 102.0), 100.0, threshold_pct=1.0)

        self.assertEqual(annotated["pct_change"], 2.0)
        self.assertTrue(annotated["is_anomaly"])
        self.assertIn("exceeded", annotated["reason"])
        self.assertIn("1.000000% threshold", annotated["reason"])

    def test_first_tick_is_a_non_anomalous_warmup(self):
        annotated = score_tick(tick("AAPL", 100.0), None)

        self.assertIsNone(annotated["pct_change"])
        self.assertFalse(annotated["is_anomaly"])
        self.assertIn("warm-up", annotated["reason"])

    def test_prior_prices_are_tracked_independently_by_symbol(self):
        scorer = RuleBasedAnomalyScorer(threshold_pct=1.0)

        self.assertIsNone(scorer.score(tick("AAPL", 100.0))["pct_change"])
        self.assertIsNone(scorer.score(tick("MSFT", 200.0))["pct_change"])
        self.assertEqual(scorer.score(tick("AAPL", 101.0))["pct_change"], 1.0)

    def test_simulated_sequence_contains_explained_outlier_at_demo_threshold(self):
        scorer = RuleBasedAnomalyScorer(threshold_pct=0.1)
        scored = [
            scorer.score(tick("AAPL", price))
            for price in (100.00, 100.05, 99.98, 100.12, 100.08)
        ]

        anomalies = [item for item in scored if item["is_anomaly"]]
        self.assertGreaterEqual(len(anomalies), 1)
        self.assertTrue(all("exceeded" in item["reason"] for item in anomalies))


class OptionalIsolationForestTests(unittest.TestCase):
    def test_known_outlier_is_flagged_deterministically(self):
        changes = [0.01, -0.02, 0.0, 0.03, -0.01, 0.02, 0.01, -0.03, 0.0, 8.0]

        flags = isolation_forest_flags(changes, contamination=0.1)

        self.assertEqual(len(flags), len(changes))
        self.assertTrue(flags[-1])
        self.assertEqual(sum(flags), 1)


if __name__ == "__main__":
    unittest.main()
