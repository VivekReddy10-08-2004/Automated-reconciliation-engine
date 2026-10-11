"""Local ZeroMQ transport configuration for the real-time tick feed."""

from __future__ import annotations

import os

from dotenv import load_dotenv


ENDPOINT_ENV = "REALTIME_ZMQ_ENDPOINT"
DEFAULT_ENDPOINT = "tcp://127.0.0.1:5555"


def get_endpoint() -> str:
    """Return the configured local ZeroMQ endpoint."""
    load_dotenv("project.env", override=False)
    endpoint = os.getenv(ENDPOINT_ENV, DEFAULT_ENDPOINT).strip()
    if not endpoint:
        raise ValueError(f"{ENDPOINT_ENV} cannot be empty")
    return endpoint


def validate_local_endpoint(endpoint: str) -> str:
    """Reject non-local transports for this local-only MVP."""
    local_prefixes = (
        "tcp://127.0.0.1:",
        "tcp://localhost:",
        "tcp://[::1]:",
        "ipc://",
        "inproc://",
    )
    if not endpoint.startswith(local_prefixes):
        raise ValueError(
            "The real-time endpoint must use loopback TCP, IPC, or in-process transport"
        )
    return endpoint
