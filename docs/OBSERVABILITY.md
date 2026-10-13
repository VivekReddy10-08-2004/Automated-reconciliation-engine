# OpenTelemetry and Alerting

The reconciliation engine emits vendor-neutral OpenTelemetry traces and metrics
over OTLP/HTTP. Structured JSON events include the active `trace_id` and
`span_id`, allowing logs to be correlated without exporting credentials or raw
provider payloads.

Telemetry is explicitly disabled by default. Enable it only when a collector is
available:

```text
RECONCILIATION_OTEL_ENABLED=true
RECONCILIATION_ENVIRONMENT=local
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318
OTEL_METRIC_EXPORT_INTERVAL=60000
```

The implementation follows the standard OpenTelemetry resource and exporter
environment variables. Signal-specific
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` override the shared endpoint. OTLP headers,
TLS certificates, timeouts, compression, and metric temporality can be supplied
with the corresponding standard SDK variables.

## Signal flow

```text
batch / API / alert evaluator
        |
        | OTLP/HTTP traces + metrics
        v
OpenTelemetry Collector
        |-- traces --> Tempo --> Grafana
        `-- metrics --> Prometheus --> Grafana

operational rule evaluator --> authenticated webhook
```

For local evaluation, start the loopback-only reference stack:

```sh
docker compose --env-file project.env -f observability/docker-compose.yaml up -d
```

Then enable telemetry and run the batch or API normally. Grafana is available
at `http://127.0.0.1:3000`, Prometheus at `http://127.0.0.1:9090`, and Tempo at
`http://127.0.0.1:3200`. Compose refuses to start until
`GRAFANA_ADMIN_PASSWORD` is configured in the private environment file.
The reference stack retains traces for 24 hours; production retention belongs
in the selected telemetry backend.

The Collector configuration uses a memory limiter and batching. Prometheus
scrapes only the Collector's translated metrics endpoint. The application does
not expose a new unauthenticated metrics route.

## Traces

Implemented spans include:

| Span | Important attributes |
| --- | --- |
| `batch.reconciliation` | run ID, control date, symbol count |
| `ingestion.equity.partitions` | run ID, partition count, worker bound |
| `ingestion.partition` | partition ID, provider, bucket, row count, outcome |
| `provider.fetch` | provider, instrument type, symbol count |
| `massive.http.get` | safe route path, status code, retry attempt |
| `reconciliation.compare` | run ID |
| `storage.persist_run` | run ID and `db.system=duckdb` |
| FastAPI server spans | method, route, status, latency |
| `duckdb.query` | read operation and database system |
| `reconciliation_export_completed` event | format and row count |
| `alerts.evaluate` | operational alert evaluation |
| `alert.webhook.deliver` | alert rule and severity |

Worker tasks copy the parent context before entering the thread pool, so provider
and partition spans remain children of the batch ingestion trace. Exceptions are
recorded on spans with error status. URLs, bearer tokens, API keys, SQL text,
symbols, and exported rows are deliberately excluded from metric labels.

## Metrics

All metric labels are bounded. Run IDs, partition IDs, symbols, exception IDs,
and error messages remain in traces or controlled logs instead of metric labels.

| Metric | Type | Dimensions |
| --- | --- | --- |
| `reconciliation.runs` | Counter | run status |
| `reconciliation.records` | Counter | run/reconciliation status |
| `reconciliation.partitions` | Counter | provider, instrument, outcome, archive reuse |
| `reconciliation.provider.requests` | Counter | provider, instrument, outcome |
| `reconciliation.provider.duration` | Histogram, ms | provider, instrument, outcome |
| `reconciliation.provider.rows` | Counter | provider, instrument, outcome |
| `reconciliation.quarantines` | Counter | provider, instrument |
| `reconciliation.api.requests` | Counter | method, route, status code |
| `reconciliation.api.duration` | Histogram, ms | method, route, status code |
| `reconciliation.api.db.active_queries` | Up/down counter | DuckDB operation |
| `reconciliation.api.db.duration` | Histogram, ms | DuckDB operation |
| `reconciliation.api.db.rejections` | Counter | capacity rejection reason |
| `reconciliation.exports` | Counter | export format |
| `reconciliation.export.rows` | Counter | export format |
| `reconciliation.alerts` | Counter | rule, severity, delivery outcome |

Batch processes flush and shut down their providers at exit. The API exports
periodically while running. `OTEL_SDK_DISABLED=true` overrides the application
switch and keeps every SDK signal disabled.

## Alerting

Existing run alerts remain aggregated: a failed run or a run containing breaks
produces one summary, never one notification per break. The operational alert
evaluator adds these read-only rules:

- open exceptions past their SLA;
- failed ingestion partitions;
- provider failure rate above `ALERT_PROVIDER_FAILURE_RATE`; and
- provider maximum latency above `ALERT_PROVIDER_LATENCY_MS`.

Configuration:

```text
ALERT_WEBHOOK_URL=https://approved-alert-destination.example/hooks/...
ALERT_COOLDOWN_SECONDS=900
ALERT_LOOKBACK_MINUTES=15
ALERT_PROVIDER_FAILURE_RATE=0.20
ALERT_PROVIDER_LATENCY_MS=10000
```

`ALERT_WEBHOOK_URL` takes precedence over the existing `WEBHOOK_URL`. Payloads
contain a stable fingerprint, rule, severity, summary, observed time, and
aggregate attributes. Tokens, raw rows, exception details, and webhook URLs are
not logged. Duplicate alerts are suppressed within a long-running process for
the configured cooldown; downstream Alertmanager or incident tooling should
provide durable grouping and inhibition across process restarts.

Run the evaluator manually or every five minutes through the documented local
scheduler:

```sh
python -m reconciliation_engine.alerts --db reconciliation.duckdb
```

It opens DuckDB read-only, emits a JSON list of active alerts, delivers each
aggregate alert once, and exits nonzero if evaluation itself fails.

## Production requirements

- Send OTLP over TLS to a Collector, not directly to multiple vendors.
- Authenticate the Collector and alert destination with managed secrets.
- Keep telemetry endpoints private and enforce retention and access controls.
- Route delivery failures and Collector queue drops to a separate platform
  health alert.
- Use tail sampling only after preserving all errors and high-latency traces.
- Review metric label changes for cardinality before deployment.

Reference documentation:

- [OpenTelemetry Python exporters](https://opentelemetry.io/docs/languages/python/exporters/)
- [OTLP exporter configuration](https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/)
- [OpenTelemetry SDK environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/)
- [FastAPI instrumentation](https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/fastapi/fastapi.html)
