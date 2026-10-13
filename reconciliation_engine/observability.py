"""OpenTelemetry traces, metrics, and credential-safe structured events."""

from __future__ import annotations

import atexit
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
import json
import logging
import os
import threading
import time
from typing import Any, Callable, Iterator

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from opentelemetry.trace import Status, StatusCode
from dotenv import load_dotenv


LOGGER = logging.getLogger("reconciliation_engine")
INSTRUMENTATION_NAME = "reconciliation_engine"
INSTRUMENTATION_VERSION = "0.1.0"


def _environment_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() == "true"


def _signal_endpoint(signal: str) -> str:
    specific = os.getenv(f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT")
    if specific:
        return specific
    base = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
    return f"{base.rstrip('/')}/v1/{signal}"


@dataclass(frozen=True)
class TelemetryConfig:
    enabled: bool
    service_name: str
    service_version: str
    environment: str
    traces_endpoint: str
    metrics_endpoint: str
    metric_export_interval_ms: int

    @classmethod
    def from_environment(cls, *, service_name: str | None = None) -> "TelemetryConfig":
        interval = int(os.getenv("OTEL_METRIC_EXPORT_INTERVAL", "60000"))
        if interval <= 0:
            raise ValueError("OTEL_METRIC_EXPORT_INTERVAL must be positive")
        return cls(
            enabled=(
                _environment_bool("RECONCILIATION_OTEL_ENABLED")
                and not _environment_bool("OTEL_SDK_DISABLED")
            ),
            service_name=service_name or os.getenv("OTEL_SERVICE_NAME") or "reconciliation-engine",
            service_version=INSTRUMENTATION_VERSION,
            environment=os.getenv("RECONCILIATION_ENVIRONMENT", "local"),
            traces_endpoint=_signal_endpoint("traces"),
            metrics_endpoint=_signal_endpoint("metrics"),
            metric_export_interval_ms=interval,
        )


class TelemetryRuntime:
    """Typed application instruments backed by explicit SDK providers."""

    def __init__(
        self,
        *,
        enabled: bool,
        tracer_provider: trace.TracerProvider | None = None,
        meter_provider: metrics.MeterProvider | None = None,
    ) -> None:
        self.enabled = enabled
        self.tracer_provider = tracer_provider or trace.get_tracer_provider()
        self.meter_provider = meter_provider or metrics.get_meter_provider()
        self.tracer = self.tracer_provider.get_tracer(INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION)
        meter = self.meter_provider.get_meter(INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION)
        self.runs = meter.create_counter("reconciliation.runs", unit="{run}")
        self.reconciliation_records = meter.create_counter("reconciliation.records", unit="{record}")
        self.partitions = meter.create_counter("reconciliation.partitions", unit="{partition}")
        self.provider_requests = meter.create_counter("reconciliation.provider.requests", unit="{request}")
        self.provider_latency = meter.create_histogram("reconciliation.provider.duration", unit="ms")
        self.provider_rows = meter.create_counter("reconciliation.provider.rows", unit="{row}")
        self.quarantines = meter.create_counter("reconciliation.quarantines", unit="{record}")
        self.api_requests = meter.create_counter("reconciliation.api.requests", unit="{request}")
        self.api_duration = meter.create_histogram("reconciliation.api.duration", unit="ms")
        self.api_active_queries = meter.create_up_down_counter("reconciliation.api.db.active_queries", unit="{query}")
        self.api_db_duration = meter.create_histogram("reconciliation.api.db.duration", unit="ms")
        self.api_query_rejections = meter.create_counter("reconciliation.api.db.rejections", unit="{query}")
        self.exports = meter.create_counter("reconciliation.exports", unit="{export}")
        self.export_rows = meter.create_counter("reconciliation.export.rows", unit="{row}")
        self.alerts = meter.create_counter("reconciliation.alerts", unit="{alert}")

    @contextmanager
    def span(self, name: str, **attributes: object) -> Iterator[trace.Span]:
        with self.tracer.start_as_current_span(name, attributes=_safe_attributes(attributes)) as span:
            try:
                yield span
            except Exception as error:
                span.record_exception(error)
                span.set_status(Status(StatusCode.ERROR, type(error).__name__))
                raise

    def record_run(self, status: str, *, records: int, breaks: int) -> None:
        attributes = {"run.status": status}
        self.runs.add(1, attributes)
        self.reconciliation_records.add(records, attributes)
        if breaks:
            self.reconciliation_records.add(
                breaks, {"run.status": status, "reconciliation.status": "BREAK"}
            )

    def record_partition(
        self,
        *,
        provider: str,
        instrument_type: str,
        status: str,
        latency_ms: float,
        rows: int,
        reused_archive: bool,
    ) -> None:
        attributes = {
            "provider": provider,
            "instrument.type": instrument_type,
            "partition.status": status,
            "archive.reused": reused_archive,
        }
        self.partitions.add(1, attributes)
        self.provider_requests.add(1, attributes)
        self.provider_latency.record(max(0.0, latency_ms), attributes)
        self.provider_rows.add(max(0, rows), attributes)
        if status == "FAILED":
            self.quarantines.add(1, attributes)

    def record_api_request(self, method: str, route: str, status_code: int, duration_ms: float) -> None:
        attributes = {
            "http.request.method": method,
            "http.route": route,
            "http.response.status_code": status_code,
        }
        self.api_requests.add(1, attributes)
        self.api_duration.record(max(0.0, duration_ms), attributes)

    def record_export(self, output_format: str, rows: int) -> None:
        attributes = {"export.format": output_format}
        self.exports.add(1, attributes)
        self.export_rows.add(max(0, rows), attributes)

    def record_alert(self, rule: str, severity: str, delivered: bool) -> None:
        self.alerts.add(
            1,
            {"alert.rule": rule, "alert.severity": severity, "alert.delivered": delivered},
        )

    def force_flush(self, timeout_millis: int = 5000) -> None:
        for provider in (self.tracer_provider, self.meter_provider):
            flush = getattr(provider, "force_flush", None)
            if flush:
                flush(timeout_millis=timeout_millis)

    def shutdown(self) -> None:
        for provider in (self.meter_provider, self.tracer_provider):
            shutdown = getattr(provider, "shutdown", None)
            if shutdown:
                shutdown()


def _safe_attributes(attributes: dict[str, object]) -> dict[str, object]:
    safe: dict[str, object] = {}
    for key, value in attributes.items():
        if value is None:
            continue
        if isinstance(value, (bool, int, float)):
            safe[key] = value
        else:
            safe[key] = str(value)[:256]
    return safe


_runtime = TelemetryRuntime(enabled=False)
_configuration_lock = threading.Lock()
_configured = False


def build_telemetry_runtime(
    config: TelemetryConfig,
    *,
    span_exporter: SpanExporter | None = None,
    metric_reader: MetricReader | None = None,
) -> TelemetryRuntime:
    """Build an isolated runtime; injected exporters make it deterministic in tests."""
    if not config.enabled:
        return TelemetryRuntime(enabled=False)
    resource = Resource.create(
        {
            "service.name": config.service_name,
            "service.version": config.service_version,
            "deployment.environment.name": config.environment,
        }
    )
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(span_exporter or OTLPSpanExporter(endpoint=config.traces_endpoint))
    )
    reader = metric_reader or PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=config.metrics_endpoint),
        export_interval_millis=config.metric_export_interval_ms,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    return TelemetryRuntime(
        enabled=True,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
    )


def configure_telemetry(*, service_name: str | None = None) -> TelemetryRuntime:
    """Configure global OTLP export once; explicit opt-in prevents noisy local runs."""
    global _runtime, _configured
    with _configuration_lock:
        if _configured:
            return _runtime
        load_dotenv("project.env", override=False)
        config = TelemetryConfig.from_environment(service_name=service_name)
        _runtime = build_telemetry_runtime(config)
        if config.enabled:
            trace.set_tracer_provider(_runtime.tracer_provider)
            metrics.set_meter_provider(_runtime.meter_provider)
            atexit.register(_runtime.shutdown)
        _configured = True
        log_event(
            "telemetry_configured",
            enabled=config.enabled,
            service_name=config.service_name,
            environment=config.environment,
        )
        return _runtime


def get_telemetry() -> TelemetryRuntime:
    return _runtime


def set_telemetry_for_testing(runtime: TelemetryRuntime) -> TelemetryRuntime:
    """Swap the runtime and return the prior value for deterministic unit tests."""
    global _runtime
    previous = _runtime
    _runtime = runtime
    return previous


def traced(name: str) -> Callable:
    def decorator(function: Callable) -> Callable:
        @wraps(function)
        def wrapped(*args, **kwargs):
            with get_telemetry().span(name):
                return function(*args, **kwargs)
        return wrapped
    return decorator


@contextmanager
def timed_span(name: str, **attributes: object) -> Iterator[tuple[trace.Span, Callable[[], float]]]:
    started = time.monotonic()
    with get_telemetry().span(name, **attributes) as span:
        yield span, lambda: (time.monotonic() - started) * 1000


def log_event(event: str, **fields: Any) -> None:
    """Emit one JSON object; callers must pass identifiers, not credentials."""
    context = trace.get_current_span().get_span_context()
    payload = {"timestamp": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
    if context.is_valid:
        payload["trace_id"] = format(context.trace_id, "032x")
        payload["span_id"] = format(context.span_id, "016x")
    LOGGER.info(json.dumps(payload, default=str, sort_keys=True, separators=(",", ":")))
