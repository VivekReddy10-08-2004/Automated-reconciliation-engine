"""Aggregated operational alert rules and webhook delivery."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import threading
import time
from typing import Callable

import duckdb
from dotenv import load_dotenv
import requests

from reconciliation_engine.observability import configure_telemetry, get_telemetry, log_event


@dataclass(frozen=True)
class Alert:
    rule: str
    severity: str
    summary: str
    attributes: dict[str, object]

    @property
    def fingerprint(self) -> str:
        identity_attributes = {
            key: self.attributes[key]
            for key in ("provider", "instrument_type")
            if key in self.attributes
        }
        stable = json.dumps(
            {"rule": self.rule, "attributes": identity_attributes},
            default=str,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(stable.encode("utf-8")).hexdigest()[:24]


class AlertDispatcher:
    """Deliver structured alerts with process-local cooldown suppression."""

    def __init__(
        self,
        webhook_url: str | None,
        *,
        cooldown_seconds: int = 900,
        clock: Callable[[], float] = time.monotonic,
        session: requests.Session | None = None,
    ) -> None:
        if cooldown_seconds < 0:
            raise ValueError("cooldown_seconds must be non-negative")
        self.webhook_url = webhook_url
        self.cooldown_seconds = cooldown_seconds
        self.clock = clock
        self.session = session or requests.Session()
        self._last_sent: dict[str, float] = {}
        self._lock = threading.Lock()
        self.delivery_failures = 0

    @classmethod
    def from_environment(cls) -> "AlertDispatcher":
        load_dotenv("project.env", override=False)
        webhook_url = os.getenv("ALERT_WEBHOOK_URL") or os.getenv("WEBHOOK_URL")
        cooldown = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
        return cls(webhook_url, cooldown_seconds=cooldown)

    def dispatch(self, alert: Alert) -> bool:
        now = self.clock()
        with self._lock:
            last_sent = self._last_sent.get(alert.fingerprint)
            if last_sent is not None and now - last_sent < self.cooldown_seconds:
                log_event(
                    "alert_suppressed",
                    rule=alert.rule,
                    severity=alert.severity,
                    fingerprint=alert.fingerprint,
                )
                return False

        delivered = False
        if self.webhook_url:
            payload = {
                "text": f"[{alert.severity.upper()}] {alert.summary}",
                "alert": {
                    **asdict(alert),
                    "fingerprint": alert.fingerprint,
                    "observed_at": datetime.now(timezone.utc).isoformat(),
                },
            }
            try:
                with get_telemetry().span(
                    "alert.webhook.deliver",
                    **{"alert.rule": alert.rule, "alert.severity": alert.severity},
                ):
                    response = self.session.post(self.webhook_url, json=payload, timeout=10)
                    response.raise_for_status()
                    delivered = True
            except requests.RequestException:
                self.delivery_failures += 1
                log_event(
                    "alert_delivery_failed",
                    rule=alert.rule,
                    severity=alert.severity,
                    fingerprint=alert.fingerprint,
                )
        else:
            log_event(
                "alert_no_destination",
                rule=alert.rule,
                severity=alert.severity,
                fingerprint=alert.fingerprint,
            )

        if delivered:
            with self._lock:
                self._last_sent[alert.fingerprint] = now
            log_event(
                "alert_delivered",
                rule=alert.rule,
                severity=alert.severity,
                fingerprint=alert.fingerprint,
            )
        get_telemetry().record_alert(alert.rule, alert.severity, delivered)
        return delivered


@dataclass(frozen=True)
class AlertThresholds:
    lookback_minutes: int = 15
    provider_failure_rate: float = 0.20
    provider_latency_ms: float = 10_000.0

    @classmethod
    def from_environment(cls) -> "AlertThresholds":
        thresholds = cls(
            lookback_minutes=int(os.getenv("ALERT_LOOKBACK_MINUTES", "15")),
            provider_failure_rate=float(os.getenv("ALERT_PROVIDER_FAILURE_RATE", "0.20")),
            provider_latency_ms=float(os.getenv("ALERT_PROVIDER_LATENCY_MS", "10000")),
        )
        if thresholds.lookback_minutes <= 0:
            raise ValueError("ALERT_LOOKBACK_MINUTES must be positive")
        if not 0 <= thresholds.provider_failure_rate <= 1:
            raise ValueError("ALERT_PROVIDER_FAILURE_RATE must be between 0 and 1")
        if thresholds.provider_latency_ms <= 0:
            raise ValueError("ALERT_PROVIDER_LATENCY_MS must be positive")
        return thresholds


def evaluate_operational_alerts(
    db_path: str,
    *,
    dispatcher: AlertDispatcher | None = None,
    thresholds: AlertThresholds | None = None,
) -> list[Alert]:
    """Evaluate bounded aggregate rules without modifying reconciliation state."""
    dispatcher = dispatcher or AlertDispatcher.from_environment()
    thresholds = thresholds or AlertThresholds.from_environment()
    alerts: list[Alert] = []
    with get_telemetry().span("alerts.evaluate", **{"db.system": "duckdb"}):
        with duckdb.connect(db_path, read_only=True) as connection:
            overdue, unassigned = connection.execute(
                """
                SELECT count(*) FILTER (
                           WHERE status = 'OPEN' AND due_at < current_timestamp
                       ),
                       count(*) FILTER (
                           WHERE status = 'OPEN' AND owner = 'UNASSIGNED'
                       )
                FROM equity_exceptions
                """
            ).fetchone()
            if overdue:
                alerts.append(
                    Alert(
                        "overdue_exceptions",
                        "critical",
                        f"{overdue} reconciliation exceptions are past SLA",
                        {"overdue_count": overdue, "unassigned_count": unassigned},
                    )
                )

            failed_partitions = connection.execute(
                "SELECT count(*) FROM work_partitions WHERE status = 'FAILED'"
            ).fetchone()[0]
            if failed_partitions:
                alerts.append(
                    Alert(
                        "failed_partitions",
                        "critical",
                        f"{failed_partitions} ingestion partitions require retry or quarantine review",
                        {"failed_partition_count": failed_partitions},
                    )
                )

            provider_rows = connection.execute(
                """
                SELECT provider,
                       count(*) AS requests,
                       count(*) FILTER (WHERE outcome_status = 'FAILED') AS failures,
                       max(latency_ms) AS max_latency_ms
                FROM provider_request_metrics
                WHERE recorded_at >= current_timestamp - (? * INTERVAL '1 minute')
                GROUP BY provider
                """,
                [thresholds.lookback_minutes],
            ).fetchall()
            for provider, requests_count, failures, max_latency_ms in provider_rows:
                failure_rate = failures / requests_count if requests_count else 0.0
                if failure_rate >= thresholds.provider_failure_rate and failures:
                    alerts.append(
                        Alert(
                            "provider_failure_rate",
                            "critical",
                            f"{provider} failure rate is {failure_rate:.1%}",
                            {
                                "provider": provider,
                                "failure_rate": round(failure_rate, 4),
                                "request_count": requests_count,
                                "lookback_minutes": thresholds.lookback_minutes,
                            },
                        )
                    )
                if max_latency_ms is not None and max_latency_ms >= thresholds.provider_latency_ms:
                    alerts.append(
                        Alert(
                            "provider_latency",
                            "warning",
                            f"{provider} latency reached {max_latency_ms:.0f} ms",
                            {
                                "provider": provider,
                                "max_latency_ms": round(max_latency_ms, 3),
                                "lookback_minutes": thresholds.lookback_minutes,
                            },
                        )
                    )

    for alert in alerts:
        dispatcher.dispatch(alert)
    log_event("alert_evaluation_completed", alert_count=len(alerts))
    return alerts


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="reconciliation.duckdb")
    arguments = parser.parse_args()
    configure_telemetry(service_name="reconciliation-alerts")
    dispatcher = AlertDispatcher.from_environment()
    try:
        alerts = evaluate_operational_alerts(arguments.db, dispatcher=dispatcher)
    except (duckdb.Error, OSError, ValueError) as error:
        print(f"[FAIL] {type(error).__name__}: alert evaluation failed")
        return 1
    print(json.dumps([asdict(alert) for alert in alerts], default=str))
    if dispatcher.delivery_failures:
        print(f"[FAIL] {dispatcher.delivery_failures} alert deliveries failed")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
