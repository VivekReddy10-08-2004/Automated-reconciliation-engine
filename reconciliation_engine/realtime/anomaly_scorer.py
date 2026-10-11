"""Annotate real-time equity ticks with explainable price-change anomalies."""

from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable
import json
import math
import time

import zmq

from reconciliation_engine.realtime import (
    get_endpoint,
    get_scored_endpoint,
    validate_local_endpoint,
)
from reconciliation_engine.realtime.publisher import TICK_FIELDS
from reconciliation_engine.realtime.subscriber import validate_tick


ANNOTATION_FIELDS = ("pct_change", "is_anomaly", "reason")
ANNOTATED_TICK_FIELDS = (*TICK_FIELDS, *ANNOTATION_FIELDS)
DEFAULT_THRESHOLD_PCT = 1.0


def _validate_threshold(threshold_pct: float) -> float:
    if isinstance(threshold_pct, bool):
        raise ValueError("threshold_pct must be a non-negative finite number")
    threshold = float(threshold_pct)
    if not math.isfinite(threshold) or threshold < 0:
        raise ValueError("threshold_pct must be a non-negative finite number")
    return threshold


def score_tick(
    tick: object,
    previous_price: float | None,
    *,
    threshold_pct: float = DEFAULT_THRESHOLD_PCT,
) -> dict[str, object]:
    """Return one validated tick annotated by a transparent percentage rule."""
    valid_tick = validate_tick(tick)
    threshold = _validate_threshold(threshold_pct)

    if previous_price is None:
        pct_change = None
        is_anomaly = False
        reason = "No prior price for symbol; warm-up tick"
    else:
        if (
            isinstance(previous_price, bool)
            or not math.isfinite(float(previous_price))
            or float(previous_price) <= 0
        ):
            raise ValueError("previous_price must be a positive finite number or None")
        pct_change = round(
            (float(valid_tick["price"]) - float(previous_price))
            / float(previous_price)
            * 100,
            6,
        )
        is_anomaly = abs(pct_change) > threshold
        comparison = "exceeded" if is_anomaly else "did not exceed"
        reason = (
            f"Absolute price change {abs(pct_change):.6f}% {comparison} "
            f"{threshold:.6f}% threshold"
        )

    return {
        **valid_tick,
        "pct_change": pct_change,
        "is_anomaly": is_anomaly,
        "reason": reason,
    }


def validate_annotated_tick(message: object) -> dict[str, object]:
    """Validate the exact task 17 contract consumed by read-only clients."""
    if not isinstance(message, dict) or set(message) != set(ANNOTATED_TICK_FIELDS):
        raise ValueError(
            f"annotated tick fields must be exactly: "
            f"{', '.join(ANNOTATED_TICK_FIELDS)}"
        )
    validate_tick({field: message[field] for field in TICK_FIELDS})

    pct_change = message["pct_change"]
    if pct_change is not None and (
        isinstance(pct_change, bool)
        or not isinstance(pct_change, (int, float))
        or not math.isfinite(float(pct_change))
    ):
        raise ValueError("pct_change must be a finite number or None")
    if not isinstance(message["is_anomaly"], bool):
        raise ValueError("is_anomaly must be boolean")
    if not isinstance(message["reason"], str) or not message["reason"].strip():
        raise ValueError("reason must be a non-empty string")
    if message["is_anomaly"] and pct_change is None:
        raise ValueError("an anomalous tick must include pct_change")
    return message


class RuleBasedAnomalyScorer:
    """Track the last observed price independently for every symbol."""

    def __init__(self, threshold_pct: float = DEFAULT_THRESHOLD_PCT) -> None:
        self.threshold_pct = _validate_threshold(threshold_pct)
        self._previous_prices: dict[str, float] = {}

    def score(self, tick: object) -> dict[str, object]:
        valid_tick = validate_tick(tick)
        symbol = str(valid_tick["symbol"])
        annotated = score_tick(
            valid_tick,
            self._previous_prices.get(symbol),
            threshold_pct=self.threshold_pct,
        )
        self._previous_prices[symbol] = float(valid_tick["price"])
        return annotated


def isolation_forest_flags(
    pct_changes: Iterable[float],
    *,
    contamination: float = 0.1,
    random_state: int = 42,
) -> list[bool]:
    """Compare rule results with optional offline Isolation Forest flags."""
    values = [float(value) for value in pct_changes]
    if len(values) < 2:
        raise ValueError("Isolation Forest requires at least two percentage changes")
    if any(not math.isfinite(value) for value in values):
        raise ValueError("percentage changes must be finite")
    if not 0 < contamination <= 0.5:
        raise ValueError("contamination must be greater than 0 and at most 0.5")

    from sklearn.ensemble import IsolationForest

    predictions = IsolationForest(
        contamination=contamination,
        random_state=random_state,
    ).fit_predict([[value] for value in values])
    return [prediction == -1 for prediction in predictions]


def score_and_publish_ticks(
    input_endpoint: str,
    output_endpoint: str,
    *,
    threshold_pct: float = DEFAULT_THRESHOLD_PCT,
    count: int | None = None,
    timeout_ms: int | None = None,
    startup_delay_seconds: float = 0.5,
    context: zmq.Context | None = None,
    output: Callable[[str], None] | None = print,
) -> list[dict[str, object]]:
    """Consume raw ticks and republish rule-annotated ticks."""
    validate_local_endpoint(input_endpoint)
    validate_local_endpoint(output_endpoint)
    if input_endpoint == output_endpoint:
        raise ValueError("input and output endpoints must be different")
    if count is not None and count < 0:
        raise ValueError("count must be non-negative or None")
    if timeout_ms is not None and timeout_ms < 0:
        raise ValueError("timeout_ms must be non-negative or None")
    if startup_delay_seconds < 0:
        raise ValueError("startup_delay_seconds must be non-negative")

    scorer = RuleBasedAnomalyScorer(threshold_pct)
    owns_context = context is None
    active_context = context or zmq.Context()
    subscriber = active_context.socket(zmq.SUB)
    publisher = active_context.socket(zmq.PUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    publisher.setsockopt(zmq.LINGER, 0)
    if timeout_ms is not None:
        subscriber.setsockopt(zmq.RCVTIMEO, timeout_ms)

    annotated_ticks: list[dict[str, object]] = []
    processed = 0
    try:
        publisher.bind(output_endpoint)
        subscriber.connect(input_endpoint)
        time.sleep(startup_delay_seconds)
        while count is None or processed < count:
            try:
                annotated = scorer.score(subscriber.recv_json())
            except zmq.Again as error:
                raise TimeoutError("Timed out waiting for a raw real-time tick") from error
            publisher.send_json(annotated)
            processed += 1
            if count is not None:
                annotated_ticks.append(annotated)
            if output:
                output(f"[SCORED] {json.dumps(annotated, separators=(',', ':'))}")
    except KeyboardInterrupt:
        if output:
            output("Anomaly scorer stopped")
    finally:
        subscriber.close(linger=0)
        publisher.close(linger=0)
        if owns_context:
            active_context.term()

    return annotated_ticks


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--threshold-pct",
        type=float,
        default=DEFAULT_THRESHOLD_PCT,
        help="Flag absolute prior-tick price changes above this percentage",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=0,
        help="Number of ticks to score; 0 runs until Ctrl+C",
    )
    parser.add_argument("--timeout-ms", type=int)
    arguments = parser.parse_args()
    score_and_publish_ticks(
        get_endpoint(),
        get_scored_endpoint(),
        threshold_pct=arguments.threshold_pct,
        count=None if arguments.count == 0 else arguments.count,
        timeout_ms=arguments.timeout_ms,
    )


if __name__ == "__main__":
    main()
