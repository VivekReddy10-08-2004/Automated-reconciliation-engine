"""Shared Massive REST transport, quota control, pagination, and retries."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import os
import threading
import time
from typing import Any
from urllib.parse import urlparse

import requests

from reconciliation_engine.observability import get_telemetry
from reconciliation_engine.security import load_massive_api_key


BASE_URL = "https://api.massive.com"
DEFAULT_CALLS_PER_MINUTE = 5
DEFAULT_TIMEOUT_SECONDS = 30.0
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class MassiveRateLimiter:
    """Process-wide, thread-safe fixed-spacing limiter for the free plan."""

    def __init__(self, calls_per_minute: int = DEFAULT_CALLS_PER_MINUTE) -> None:
        if calls_per_minute <= 0:
            raise ValueError("calls_per_minute must be positive")
        self.minimum_interval = 60.0 / calls_per_minute
        self._next_request_at = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._next_request_at - now
            if delay > 0:
                time.sleep(delay)
                now = time.monotonic()
            self._next_request_at = now + self.minimum_interval


_GLOBAL_LIMITER: MassiveRateLimiter | None = None
_GLOBAL_LIMITER_LOCK = threading.Lock()


def _get_global_limiter() -> MassiveRateLimiter:
    """Create the shared limiter after project.env has had a chance to load."""
    global _GLOBAL_LIMITER
    with _GLOBAL_LIMITER_LOCK:
        if _GLOBAL_LIMITER is None:
            calls_per_minute = int(
                os.getenv(
                    "MASSIVE_CALLS_PER_MINUTE", str(DEFAULT_CALLS_PER_MINUTE)
                )
            )
            _GLOBAL_LIMITER = MassiveRateLimiter(calls_per_minute)
        return _GLOBAL_LIMITER


class MassiveClient:
    """Small authenticated client shared by all Massive asset adapters."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        session: requests.Session | None = None,
        limiter: MassiveRateLimiter | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = 3,
    ) -> None:
        self._api_key = api_key or load_massive_api_key()
        self._session = session or requests.Session()
        self._limiter = limiter or _get_global_limiter()
        self._timeout = timeout
        self._max_retries = max_retries

    def get_json(
        self,
        path_or_url: str,
        params: Mapping[str, object] | None = None,
    ) -> dict[str, Any]:
        """GET one response with bounded retries and no key in the URL."""
        url = self._url(path_or_url)
        headers = {"Authorization": f"Bearer {self._api_key}"}

        for attempt in range(self._max_retries + 1):
            with get_telemetry().span(
                "massive.http.get",
                **{
                    "server.address": "api.massive.com",
                    "http.route": urlparse(url).path,
                    "retry.attempt": attempt,
                },
            ) as span:
                self._limiter.wait()
                response = self._session.get(
                    url,
                    params=dict(params or {}),
                    headers=headers,
                    timeout=self._timeout,
                )
                span.set_attribute("http.response.status_code", response.status_code)
            if response.status_code not in RETRYABLE_STATUS_CODES:
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Massive returned a non-object JSON response")
                return payload

            if attempt == self._max_retries:
                response.raise_for_status()
            retry_after = response.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else min(2**attempt, 30)
            time.sleep(max(0.0, delay))

        raise RuntimeError("Massive request retry loop exited unexpectedly")

    def iter_results(
        self,
        path_or_url: str,
        params: Mapping[str, object] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield every result while following Massive cursor pagination."""
        next_url: str | None = path_or_url
        next_params = params
        while next_url:
            payload = self.get_json(next_url, next_params)
            results = payload.get("results", [])
            if not isinstance(results, list):
                raise ValueError("Massive paginated response has non-list results")
            for result in results:
                if isinstance(result, dict):
                    yield result
            value = payload.get("next_url")
            next_url = str(value) if value else None
            next_params = None

    @staticmethod
    def _url(path_or_url: str) -> str:
        if path_or_url.startswith("/"):
            return f"{BASE_URL}{path_or_url}"
        parsed = urlparse(path_or_url)
        if parsed.scheme == "https" and parsed.netloc == "api.massive.com":
            return path_or_url
        raise ValueError("Massive URL must use https://api.massive.com")
