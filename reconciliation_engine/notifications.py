"""Send run summaries without exposing notification credentials."""

import os

import requests
from dotenv import load_dotenv

from reconciliation_engine.observability import get_telemetry, log_event


def send_run_notification(summary: dict) -> None:
    load_dotenv("project.env")
    webhook_url = os.getenv("WEBHOOK_URL")

    status = summary.get("status")

    if status not in {"FAILED", "SUCCESS_WITH_BREAKS"}:
        return
    message = (
        f"Run ID: {summary['run_id']} | "
        f"Status: {status} | "
        f"Break count: {summary['break_count']}"
    )
    rule = "run_failed" if status == "FAILED" else "reconciliation_breaks"
    severity = "critical" if status == "FAILED" else "warning"

    if not webhook_url:
        get_telemetry().record_alert(rule, severity, False)
        log_event("run_alert_no_destination", rule=rule, severity=severity)
        print(message)
        return

    try:
        with get_telemetry().span(
            "alert.webhook.deliver",
            **{"alert.rule": rule, "alert.severity": severity},
        ):
            response = requests.post(
                webhook_url,
                json={"text": message},
                timeout=10,
            )
            response.raise_for_status()
        get_telemetry().record_alert(rule, severity, True)
        log_event("run_alert_delivered", rule=rule, severity=severity)
    except requests.RequestException:
        get_telemetry().record_alert(rule, severity, False)
        log_event("run_alert_delivery_failed", rule=rule, severity=severity)
        raise RuntimeError("Run notification delivery failed") from None
