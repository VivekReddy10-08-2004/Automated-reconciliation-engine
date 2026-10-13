import json
import os
from unittest.mock import Mock, patch
import unittest

from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from reconciliation_engine.observability import (
    TelemetryConfig,
    build_telemetry_runtime,
    log_event,
    set_telemetry_for_testing,
)


class OpenTelemetryTests(unittest.TestCase):
    def setUp(self):
        self.span_exporter = InMemorySpanExporter()
        self.metric_reader = InMemoryMetricReader()
        self.runtime = build_telemetry_runtime(
            TelemetryConfig(
                enabled=True,
                service_name="reconciliation-test",
                service_version="test",
                environment="unit",
                traces_endpoint="http://collector/v1/traces",
                metrics_endpoint="http://collector/v1/metrics",
                metric_export_interval_ms=60_000,
            ),
            span_exporter=self.span_exporter,
            metric_reader=self.metric_reader,
        )
        self.previous = set_telemetry_for_testing(self.runtime)

    def tearDown(self):
        set_telemetry_for_testing(self.previous)
        self.runtime.shutdown()

    def test_spans_metrics_and_log_correlation_share_context(self):
        with self.assertLogs("reconciliation_engine", level="INFO") as captured:
            with self.runtime.span("provider.fetch", provider="YAHOO"):
                log_event("provider_completed", provider="YAHOO")
                self.runtime.record_partition(
                    provider="YAHOO",
                    instrument_type="EQUITY",
                    status="SUCCESS",
                    latency_ms=12.5,
                    rows=8,
                    reused_archive=False,
                )
        self.runtime.force_flush()

        spans = self.span_exporter.get_finished_spans()
        self.assertEqual([span.name for span in spans], ["provider.fetch"])
        self.assertEqual(spans[0].attributes["provider"], "YAHOO")
        event = json.loads(captured.output[0].split(":", 2)[-1])
        self.assertEqual(event["event"], "provider_completed")
        self.assertEqual(len(event["trace_id"]), 32)
        self.assertEqual(len(event["span_id"]), 16)

        metric_names = {
            metric.name
            for resource in self.metric_reader.get_metrics_data().resource_metrics
            for scope in resource.scope_metrics
            for metric in scope.metrics
        }
        self.assertIn("reconciliation.partitions", metric_names)
        self.assertIn("reconciliation.provider.duration", metric_names)
        self.assertIn("reconciliation.provider.rows", metric_names)

    def test_span_records_exception_and_error_status(self):
        with self.assertRaisesRegex(RuntimeError, "provider unavailable"):
            with self.runtime.span("provider.fetch"):
                raise RuntimeError("provider unavailable")
        self.runtime.force_flush()

        span = self.span_exporter.get_finished_spans()[0]
        self.assertEqual(span.status.status_code.name, "ERROR")
        self.assertTrue(any(event.name == "exception" for event in span.events))

    def test_environment_configuration_is_explicitly_opt_in(self):
        with patch.dict(
            os.environ,
            {
                "RECONCILIATION_OTEL_ENABLED": "true",
                "OTEL_SERVICE_NAME": "configured-service",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
                "RECONCILIATION_ENVIRONMENT": "staging",
            },
            clear=True,
        ):
            config = TelemetryConfig.from_environment()

        self.assertTrue(config.enabled)
        self.assertEqual(config.service_name, "configured-service")
        self.assertEqual(config.environment, "staging")
        self.assertEqual(config.traces_endpoint, "http://collector:4318/v1/traces")
        self.assertEqual(config.metrics_endpoint, "http://collector:4318/v1/metrics")


if __name__ == "__main__":
    unittest.main()
