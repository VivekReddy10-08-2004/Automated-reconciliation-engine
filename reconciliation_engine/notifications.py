"""Send run summaries without exposing notification credentials."""

import os

import requests
from dotenv import load_dotenv


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

    if not webhook_url:
        print(message)
        return

    try:
        response = requests.post(
            webhook_url,
            json={"text": message},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException:
        raise RuntimeError("Run notification delivery failed") from None
