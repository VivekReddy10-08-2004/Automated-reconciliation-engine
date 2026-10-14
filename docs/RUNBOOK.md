# Reconciliation Engine Runbook

This runbook covers the live-verified Yahoo/Alpaca/Massive equity batch, the
offline-verified persisted Yahoo/Alpaca options command, DuckDB and Parquet
audit paths, dbt equity models, notifications, the simulated ZeroMQ path,
dashboard, local scheduling, and the batch Docker contract.

Options persistence is implemented and offline-tested but has not been
live-provider verified. Massive options and Yahoo/Massive futures remain
experimental library capabilities outside the supported persisted workflows.
Airflow is planned; the
supported scheduler is Windows Task Scheduler or cron. See the
[Feature Support Matrix](FEATURE_SUPPORT.md) before treating any component as
deployment-ready.

## 1. Runtime configuration

Copy the safe template and keep the populated file out of Git:

```sh
cp project.env.example project.env
chmod 600 project.env
```

On Windows, restrict `project.env` to the account that runs the control. Never paste credentials into commands, logs, screenshots, source files, or issue descriptions.

| Variable | Required | Purpose |
| --- | --- | --- |
| `ALPACA_API_KEY` | Yes for a live batch | Alpaca API key |
| `ALPACA_SECRET_KEY` | Yes for a live batch | Alpaca API secret |
| `MASSIVE_API_KEY` | Yes for a live batch | Massive API key |
| `MASSIVE_CALLS_PER_MINUTE` | No | Shared Massive process limit; defaults to `5` |
| `RECONCILIATION_SYMBOLS` | No | Comma-separated equity universe; defaults to `AAPL` |
| `RECONCILIATION_SYMBOL_BUCKETS` | No | Stable bucket count; defaults to `64` and should remain fixed after production use |
| `RECONCILIATION_MAX_WORKERS` | No | Bounded ingestion threads; defaults to `8`, maximum `64` |
| `RECONCILIATION_EXPORT_API_KEY` | Yes for REST access | Bearer token for every `/api/v1/*` endpoint |
| `RECONCILIATION_OTEL_ENABLED` | No | Enables OTLP trace and metric export; defaults to `false` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | When telemetry is enabled | Collector base URL; local example uses `http://127.0.0.1:4318` |
| `ALERT_WEBHOOK_URL` | No | Structured operational alert destination; falls back to `WEBHOOK_URL` |
| `ALERT_COOLDOWN_SECONDS` | No | Duplicate alert suppression in a running process; defaults to `900` |
| `WEBHOOK_URL` | No | Receives `FAILED` and `SUCCESS_WITH_BREAKS` summaries; output is printed if omitted |
| `REALTIME_ZMQ_ENDPOINT` | No | Raw simulated-tick endpoint; defaults to `tcp://127.0.0.1:5555` |
| `REALTIME_SCORED_ZMQ_ENDPOINT` | No | Annotated-tick endpoint; defaults to `tcp://127.0.0.1:5556` |
| `RECONCILIATION_DB_PATH` | No | DuckDB path used by dbt; the Python CLI uses `--db` |

Use different local endpoints for raw and scored ticks. The application rejects non-local real-time endpoints in this release.

## 2. Local setup and verification

From the repository root:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m reconciliation_engine.smoke
python -m unittest discover -s tests -v
```

On Windows PowerShell, activate with `.venv\Scripts\Activate.ps1`.

The smoke command imports release components, validates credentials without displaying them, validates the configured symbols and local ZeroMQ endpoints, and creates a temporary DuckDB schema. It makes no Yahoo, Alpaca, or Massive request. To inspect and migrate an existing database schema as part of the check:

```sh
python -m reconciliation_engine.smoke --db reconciliation.duckdb
```

## 3. Run the batch control

Run the configured universe:

```sh
python -m reconciliation_engine.batch
```

Override it for one run:

```sh
python -m reconciliation_engine.batch --symbol AAPL --symbol MSFT --symbol NVDA
```

The command prints a deterministic UUID `run_id`. The ID is derived from the US-market control date, sorted symbol universe, and reconciliation-rule version. It stores the run, staging rows, reconciliation results, and exceptions in `reconciliation.duckdb`, and writes immutable Parquet evidence beneath `data/`.

Retry the same logical run safely, or specify the date explicitly for a backfill:

```sh
python -m reconciliation_engine.batch --symbol AAPL --control-date 2026-10-09
```

If that date/universe/version is already complete, the command returns its stored summary without provider calls, writes, or duplicate notifications. If an earlier attempt stopped after writing only part of the Parquet archive, the retry reuses the immutable files and finishes the missing work.

Inspect partition progress, attempts, and failures:

```sh
python -c "import duckdb; c=duckdb.connect('reconciliation.duckdb', read_only=True); print(c.execute('select provider, status, count(*) partitions, sum(row_count) rows from work_partitions group by provider, status order by provider, status').df().to_string(index=False))"
```

Replay saved provider inputs without network access or credentials:

```sh
python -m reconciliation_engine.batch --replay-run-id <run-id>
```

Replay recomputes and prints the comparison; it does not create a second stored pipeline run.

Build and test the SQL transformation layer:

```sh
dbt build --project-dir dbt --profiles-dir dbt
```

### Run the offline-verified options control

Select an expiration that both accounts are entitled to access:

```sh
python -m reconciliation_engine.options_batch AAPL 2026-12-18 \
  --control-date 2026-10-13 --db reconciliation.duckdb
```

The command fetches Yahoo and Alpaca option chains, reconciles contracts by
underlying, expiration, strike, type, and trade date, and atomically writes
`option_runs`, `option_contracts_staging`, and
`option_reconciliation_results`. The logical ID is deterministic, so repeating
the same underlying, expiration, and control date performs no additional
database write.

This path is implemented and covered by offline database integration tests. It
has not been exercised with live provider credentials in the published release
evidence. Empty or incomparable provider data remains an explicit break; it is
not replaced with synthetic values. There is no persisted futures command in
v1.0.0.

Massive is not called by this options command. Its experimental adapter provides
contract metadata and historical aggregate bars, which require a matching price
basis and observation time before comparison with chain snapshots. Experimental
futures helpers similarly require contract-roll and session alignment. See
[derivatives scope and promotion criteria](FEATURE_SUPPORT.md#derivatives-scope-and-promotion-criteria).

## 4. Serve reconciliation data

Start the read-only API after the batch has completed:

```sh
python -m reconciliation_engine.api --db reconciliation.duckdb --host 127.0.0.1 --port 8000
```

Check health without authentication, then access data with the bearer token:

```sh
curl http://127.0.0.1:8000/health
curl -H "Authorization: Bearer $RECONCILIATION_EXPORT_API_KEY" \
  "http://127.0.0.1:8000/api/v1/runs?limit=20"
```

Use one API worker with local DuckDB. Connections are read-only, short-lived,
and bounded, but a query overlapping an external writer may receive `503`.
Detailed endpoint and export examples are in [Read-Only Export API](EXPORT_API.md).

## 5. Run observability and alert evaluation

Start the optional loopback-only Collector, Tempo, Prometheus, and Grafana
stack, then enable telemetry in `project.env`:

```sh
docker compose --env-file project.env -f observability/docker-compose.yaml up -d
```

Run the aggregate operational rules manually:

```sh
python -m reconciliation_engine.alerts --db reconciliation.duckdb
```

Schedule that command every five minutes for continuous SLA, failed-partition,
provider-failure, and provider-latency evaluation. See
[OpenTelemetry and Alerting](OBSERVABILITY.md) for the complete signal and alert
contract.

## 6. Run the real-time demonstration

Open three terminals in the repository root. Start the scorer first:

```sh
python -m reconciliation_engine.realtime.anomaly_scorer --count 10 --threshold-pct 0.1
```

Start the continuously refreshing dashboard second:

```sh
python -m reconciliation_engine.dashboard --db reconciliation.duckdb --refresh 0.5
```

Start the deterministic publisher last:

```sh
python -m reconciliation_engine.realtime.publisher --count 10
```

The dashboard remains usable if either DuckDB or the annotated feed is unavailable. A static batch-only snapshot is also available:

```sh
python -m reconciliation_engine.dashboard --db reconciliation.duckdb
```

Reference captures for both healthy and degraded modes are in [Dashboard demonstrations](DASHBOARD_DEMO.md).

## 7. Run with Docker

Docker packages only the one-shot equity batch. It does not contain Airflow, dbt, the real-time processes, or the dashboard.

```sh
docker compose -f orchestration/docker/compose.yaml build --pull
docker compose -f orchestration/docker/compose.yaml run --rm batch
```

The named volume `automated-reconciliation-engine-state` retains DuckDB and Parquet data. Verify the stored run count:

```sh
docker compose -f orchestration/docker/compose.yaml run --rm \
  --entrypoint python batch -c "import duckdb; c=duckdb.connect('/app/state/reconciliation.duckdb', read_only=True); print(c.execute('select count(*) from pipeline_runs').fetchone()[0])"
```

Do not use `docker compose down -v` unless deleting the persisted audit state is intentional.

## 8. Expected operating scenarios

### Clean run

- The command exits with code `0` and reports `SUCCESS`.
- Reconciled observations are `MATCH` in the Python control. Both Python and dbt classify a `WARN` above the greater of `$0.01` or `0.10%`, and a `BREAK` above the greater of `$0.05` or `0.50%`.
- One `pipeline_runs` row and matching DuckDB/Parquet evidence are created atomically for the logical run.
- No webhook notification is sent.

### Reconciliation break

- The command exits with code `0` and reports `SUCCESS_WITH_BREAKS`.
- Each `BREAK` creates an `OPEN` equity exception.
- Each exception starts as `HIGH` priority, `UNASSIGNED`, with a 24-hour due time and a provider-aware reason code.
- One summary notification is posted to `WEBHOOK_URL`, or printed if no webhook is configured.
- Investigate the source values, date, tolerance, provider coverage, and market calendar before resolving the break externally. The current exception table is append-only and has no resolution command.

### API or pipeline failure

- The command exits nonzero and emits one `FAILED` summary notification.
- No credentials or webhook URL should appear in output.
- Check provider availability, Alpaca entitlement, credentials, network access, and the process log, then rerun. DuckDB persistence is transactional, so a database write failure leaves no partial run, staging, result, or exception rows. A failure before successful persistence does not create a `pipeline_runs` row; the scheduler log and notification are the failure evidence for this local release.

### Missing market observation or mark

- A one-sided Yahoo, Alpaca, or Massive equity observation is retained and classified as `BREAK`, with the missing provider and pair stored explicitly.
- An unavailable option provider creates a `BREAK` for every contract from the available side, with an explicit missing-provider reason.
- Experimental futures library comparisons require an explicit Yahoo-continuous to Massive-contract mapping; an empty mapping is rejected. Operators must verify the dated roll and session alignment before using a comparison. No futures batch command is supported.

## 9. Inspect the audit trail

### Find recent runs

```sh
python -c "import duckdb; c=duckdb.connect('reconciliation.duckdb', read_only=True); print(c.execute('select * from pipeline_runs order by started_at desc limit 10').df().to_string(index=False))"
```

### Inspect one run ID

Replace `<run-id>` in each command:

```sh
python -c "import duckdb; c=duckdb.connect('reconciliation.duckdb', read_only=True); print(c.execute('select * from pipeline_runs where run_id = ?', ['<run-id>']).df().to_string(index=False))"

python -c "import duckdb; c=duckdb.connect('reconciliation.duckdb', read_only=True); print(c.execute('select * from equity_reconciliation_results where run_id = ? order by symbol, timestamp, metric_name', ['<run-id>']).df().to_string(index=False))"
```

### Inspect exceptions

```sh
python -c "import duckdb; c=duckdb.connect('reconciliation.duckdb', read_only=True); print(c.execute('select * from equity_exceptions where run_id = ? order by created_at', ['<run-id>']).df().to_string(index=False))"
```

### Inspect Parquet evidence

```sh
python -c "import duckdb; print(duckdb.read_parquet('data/reconciliation/equities/run_id=<run-id>/results.parquet').df().to_string(index=False))"
```

The corresponding provider evidence is located at:

```text
data/raw/YAHOO/equities/run_id=<run-id>/payload.parquet
data/raw/ALPACA/equities/run_id=<run-id>/payload.parquet
data/normalized/YAHOO/equities/run_id=<run-id>/data.parquet
data/normalized/ALPACA/equities/run_id=<run-id>/data.parquet
```

### Inspect scheduler logs

The supported local scheduler and log locations are documented in [Scheduling](SCHEDULING.md). Airflow is not included in this release, so there are no Airflow task logs to inspect. If orchestration is added, this section must document the workflow ID, manual trigger command, task-log location, retry behavior, and run-ID lookup before it is presented as runnable.

## 10. Release evidence and safety checks

Before publishing a revision:

```sh
python -m reconciliation_engine.smoke
python -m unittest discover -s tests -v
dbt build --project-dir dbt --profiles-dir dbt
git diff --check
git status --short
if git ls-files | grep -Ev '(^|/)project\.env\.example$' | grep -Eq '(^|/)(project\.env|.*\.duckdb|.*[Rr]ecovery.*[Cc]ode)'; then
  echo "Refusing release: a secret or runtime artifact is tracked"
  exit 1
fi
```

Then complete one live batch run with valid credentials, save its `run_id`, open the one-shot dashboard, and capture only non-sensitive output. Existing non-sensitive dashboard evidence is stored under `docs/dashboard_demos/`.

The latest checked release evidence and environment-specific limitations are recorded in [Release Evidence](RELEASE_EVIDENCE.md).

Do not publish `project.env`, databases, Parquet files, logs, recovery codes, virtual environments, or generated dbt artifacts. See [Data security](DATA_SECURITY.md) and [Data sources](../DATA_SOURCES.md) for the security and provider caveats.

