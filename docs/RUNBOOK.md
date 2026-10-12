# Reconciliation Engine Runbook

This runbook covers the runnable release in this repository: the Yahoo/Alpaca equity batch control, DuckDB and Parquet audit trail, dbt equity models, webhook notifications, simulated ZeroMQ feed, anomaly scorer, read-only dashboard, local scheduling, and the batch Docker image.

Derivative ingestion adapters and offline option reconciliation are present, but the unified derivative control reports from Task 10 are still planned. Airflow orchestration from Task 18 is also planned; the supported scheduler is currently Windows Task Scheduler or Ubuntu cron.

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
| `RECONCILIATION_SYMBOLS` | No | Comma-separated equity universe; defaults to `AAPL` |
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

The smoke command imports release components, validates credentials without displaying them, validates the configured symbols and local ZeroMQ endpoints, and creates a temporary DuckDB schema. It makes no Yahoo or Alpaca request. To inspect and migrate an existing database schema as part of the check:

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

Replay saved provider inputs without network access or credentials:

```sh
python -m reconciliation_engine.batch --replay-run-id <run-id>
```

Replay recomputes and prints the comparison; it does not create a second stored pipeline run.

Build and test the SQL transformation layer:

```sh
dbt build --project-dir dbt --profiles-dir dbt
```

## 4. Run the real-time demonstration

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

## 5. Run with Docker

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

## 6. Expected operating scenarios

### Clean run

- The command exits with code `0` and reports `SUCCESS`.
- Reconciled observations are `MATCH` in the Python control. Both Python and dbt classify a `WARN` above the greater of `$0.01` or `0.10%`, and a `BREAK` above the greater of `$0.05` or `0.50%`.
- One `pipeline_runs` row and matching DuckDB/Parquet evidence are created atomically for the logical run.
- No webhook notification is sent.

### Reconciliation break

- The command exits with code `0` and reports `SUCCESS_WITH_BREAKS`.
- Each `BREAK` creates an `OPEN` equity exception.
- One summary notification is posted to `WEBHOOK_URL`, or printed if no webhook is configured.
- Investigate the source values, date, tolerance, provider coverage, and market calendar before resolving the break externally. The current exception table is append-only and has no resolution command.

### API or pipeline failure

- The command exits nonzero and emits one `FAILED` summary notification.
- No credentials or webhook URL should appear in output.
- Check provider availability, Alpaca entitlement, credentials, network access, and the process log, then rerun. DuckDB persistence is transactional, so a database write failure leaves no partial run, staging, result, or exception rows. A failure before successful persistence does not create a `pipeline_runs` row; the scheduler log and notification are the failure evidence for this local release.

### Missing market observation or mark

- A Yahoo-only or Alpaca-only equity observation is retained and classified as `BREAK`, with the absent provider values stored as null.
- An unavailable option provider creates a `BREAK` for every contract from the available side, with an explicit missing-provider reason. Only two empty inputs produce an empty comparison.
- Futures quality reports and swap/forward `NO_MARK` outcomes belong to the planned Task 10 control layer and are not exposed by the current batch command.

## 7. Inspect the audit trail

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

The supported local scheduler and log locations are documented in [Scheduling](SCHEDULING.md). Airflow is not included in this release, so there are no Airflow task logs to inspect. When Task 18 is implemented, this section must be updated with the DAG ID, manual trigger command, task-log location, retry behavior, and run-ID lookup before Airflow is presented as runnable.

## 8. Release evidence and safety checks

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

