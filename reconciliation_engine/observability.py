"""Small structured-event logger for batch and partition operations."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
from typing import Any


LOGGER = logging.getLogger("reconciliation_engine")


def log_event(event: str, **fields: Any) -> None:
    """Emit one JSON object; callers must pass identifiers, not credentials."""
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }
    LOGGER.info(json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")))
