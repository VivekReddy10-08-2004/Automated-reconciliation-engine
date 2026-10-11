# Automated Financial Data Ingestion & Reconciliation Engine

A Python MVP that fetches equity data from Yahoo Finance and Alpaca, normalizes both sources into one schema, and flags cross-source closing-price differences.

## Current MVP

- Normalized Yahoo Finance equity bars: `symbol, trade_date, open, high, low, close, volume`
- Normalized Yahoo Finance option chains for calls and puts
- Normalized Alpaca option chains using the same canonical option schema
- Normalized Alpaca daily equity bars using the same equity schema
- Equity reconciliation report with Yahoo/Alpaca close and volume differences
- Versioned dbt models for typed staging, provider pairing, and reconciliation facts
- Offline-tested Yahoo/Alpaca option reconciliation by full contract identity
- `MATCH` / `BREAK` status using a configurable close-price tolerance
- Immutable raw and normalized Parquet archives partitioned by source and run ID
- Read-only DuckDB dashboard for latest breaks and operational analytics
- Local ZeroMQ publish/subscribe feed with deterministic simulated equity ticks
- Explainable prior-price anomaly scoring with a separate annotated-tick feed
- Non-root Docker Compose batch runner with persistent DuckDB and Parquet state
- Private local secret/database permissions and lazy credential loading

## Run

1. Create `project.env` with `ALPACA_API_KEY` and `ALPACA_SECRET_KEY`.
2. Install dependencies with `pip install -r requirements.txt`.
3. Run the reconciliation demo from the project root:

```powershell
python -m reconciliation_engine.batch
```

The command compares recent daily AAPL bars, saves the audit trail to DuckDB,
and archives Yahoo, Alpaca, and reconciliation data under `data/`. Each archive
contains its `run_id` and an existing run file is never overwritten.

Replay a saved run without calling Yahoo or Alpaca:

```powershell
python -m reconciliation_engine.batch --replay-run-id <run-id>
```

Provider files use this layout:

```text
data/raw/<SOURCE>/equities/run_id=<run-id>/payload.parquet
data/normalized/<SOURCE>/equities/run_id=<run-id>/data.parquet
data/reconciliation/equities/run_id=<run-id>/results.parquet
```

Run the offline reconciliation tests without API credentials or network access:

```powershell
python -m unittest discover -s tests -v
```

Build the SQL transformation layer against the local DuckDB file:

```powershell
dbt build --project-dir dbt --profiles-dir dbt
```

Set `RECONCILIATION_DB_PATH` to target another DuckDB file. A successful build
creates the queryable `main.fct_equity_reconciliation` table with `MATCH`,
`WARN`, and `BREAK` outcomes. Absolute close differences above `0.01` warn;
differences above `0.05`, or missing closes, break.

## Run with Docker Compose

Docker runs only the one-shot batch pipeline. Airflow and the real-time feed are
intentionally outside this image.

1. Create `project.env` from `project.env.example`, replace the two Alpaca
   placeholders, and optionally set `WEBHOOK_URL`.
2. Build the minimal batch image:

```sh
docker compose -f orchestration/docker/compose.yaml build --pull
```

3. Run one reconciliation:

```sh
docker compose -f orchestration/docker/compose.yaml run --rm batch
```

The named volume `automated-reconciliation-engine-state` retains
`reconciliation.duckdb` and the complete `data/` archive after the container
exits. Confirm persisted run count with:

```sh
docker compose -f orchestration/docker/compose.yaml run --rm \
  --entrypoint python batch -c "import duckdb; c=duckdb.connect('/app/state/reconciliation.duckdb', read_only=True); print(c.execute('select count(*) from pipeline_runs').fetchone()[0])"
```

Credentials are supplied from `project.env` at container runtime. The
deny-by-default `.dockerignore` prevents that file, local data, Git history, and
developer environments from entering the image build context.

## Run the Real-Time Feed

The initial feed is explicitly simulated and does not require API credentials.
Start the subscriber in one terminal:

```powershell
python -m reconciliation_engine.realtime.subscriber --count 10
```

Then start the publisher in another terminal:

```powershell
python -m reconciliation_engine.realtime.publisher --count 10
```

Both commands use `REALTIME_ZMQ_ENDPOINT`, which defaults to the local endpoint
`tcp://127.0.0.1:5555`. Each JSON tick contains exactly `symbol`, `price`,
`exchange_timestamp`, `received_timestamp`, and `latency_ms`. Omit `--count` to
run continuously and stop cleanly with Ctrl+C.

To score the simulated feed, start the scorer before the publisher:

```powershell
python -m reconciliation_engine.realtime.anomaly_scorer --count 10 --threshold-pct 0.1
```

Then run the publisher in another terminal:

```powershell
python -m reconciliation_engine.realtime.publisher --count 10
```

The scorer preserves every raw field and adds `pct_change`, `is_anomaly`, and
an explanatory `reason`. It republishes annotated ticks to
`REALTIME_SCORED_ZMQ_ENDPOINT`, which defaults to `tcp://127.0.0.1:5556`.
The default rule threshold is 1%; the lower `0.1` demo threshold intentionally
flags a movement in the deterministic simulated sequence. The optional
`isolation_forest_flags` function supports offline comparison without changing
the transparent rule used by the live scorer.

View a one-time reconciliation control-room snapshot. It remains usable when
either DuckDB or the annotated feed is unavailable:

```powershell
python -m reconciliation_engine.dashboard --db reconciliation.duckdb
```

Add `--refresh 0.5` for a continuously updating batch and real-time view:

```powershell
python -m reconciliation_engine.dashboard --db reconciliation.duckdb --refresh 0.5
```

The dashboard opens DuckDB in read-only mode and uses only a ZeroMQ `SUB`
socket. It displays the latest batch status and MATCH/WARN/BREAK counts, the top
five breaks, the most recent annotated tick and latency, anomalies observed in
the last 60 seconds, and the latest anomaly reason. Refresh requests below 0.5
seconds are bounded to 0.5 seconds.

| Panel | Operator question |
| --- | --- |
| Batch reconciliation | Did the control complete, what matched/warned/broke, and which five breaks are largest? |
| Real-time anomaly feed | What was the latest tick and latency, and were recent movements anomalous? |

Start the task 17 scorer and task 16 publisher as shown above to populate the
live panel. See [Dashboard degraded-mode demonstrations](docs/DASHBOARD_DEMO.md)
for captured examples of both panels available and each independent failure.

## Schedule the Daily Control

See [docs/SCHEDULING.md](docs/SCHEDULING.md) for complete Windows Task Scheduler
and Ubuntu cron setup. Both schedules run at 4:30 PM America/New_York on
weekdays, after the regular US equity-market close.

## Data Security

- Keep `project.env` local and set its permissions to owner-only (`chmod 600 project.env` on Linux/macOS).
- Rotate credentials immediately if they are printed, shared, or committed.
- API clients load credentials only when a provider call is made; tests and analytics do not need secrets.
- DuckDB files are restricted to the current OS user when initialized.
- The dashboard reads curated reconciliation results and aggregate metrics, not raw credential or payload data.
- If a key may have been exposed, revoke or rotate it at Alpaca; changing the local file does not invalidate a copied key.

See [docs/DATA_SECURITY.md](docs/DATA_SECURITY.md) for the threat model and production controls.

## Project Structure

```text
Automated-reconciliation-engine/
├── reconciliation_engine/        # Python application package
│   ├── batch.py                   # Daily batch command and archive replay
│   ├── dashboard.py               # Read-only reconciliation dashboard
│   ├── notifications.py           # Break and failure notifications
│   ├── security.py                # Credentials and file permissions
│   ├── ingestion/                 # Provider adapters and normalization
│   │   ├── alpaca_equities.py
│   │   ├── alpaca_options.py
│   │   ├── yahoo_equities.py
│   │   ├── yahoo_options.py
│   │   ├── futures.py
│   │   ├── swaps.py
│   │   └── forwards.py
│   ├── reconciliation/            # Comparison rules and exceptions
│   │   ├── equities.py
│   │   ├── options.py
│   │   └── exceptions.py
│   ├── storage/                   # Durable local data interfaces
│   │   ├── duckdb.py
│   │   └── data_lake.py
│   └── realtime/                  # ZeroMQ publish/subscribe path
│       ├── anomaly_scorer.py       # Explainable real-time anomaly scoring
│       ├── publisher.py
│       └── subscriber.py
├── dbt/                           # Versioned DuckDB SQL transformations
│   ├── models/
│   └── tests/
├── tests/                         # Offline unit and integration tests
├── docs/                          # Architecture, operations, and security
├── orchestration/                 # Deployment and future scheduling
│   └── docker/                    # Batch image and Compose definition
├── project.env.example            # Safe configuration template
├── pyproject.toml                 # Python project and dependencies
└── README.md
```

Application code belongs inside `reconciliation_engine/`; SQL transformations
belong inside `dbt/`; automated checks belong inside `tests/`; and operational
guidance belongs inside `docs/`. Runtime databases, Parquet data, logs, virtual
environments, and generated dbt artifacts stay local through `.gitignore`.
See [Codebase Organization](docs/CODEBASE_ORGANIZATION.md) for the placement
rules inherited by every project task.

## Options Data Availability

Comparable Alpaca option contracts may be unavailable because of account entitlements, subscription level, contract expiration, or provider coverage. The project does not replace missing Alpaca observations with Yahoo values or synthetic prices and does not report an unavailable comparison as a `MATCH`.

When either provider returns no option contracts, the reconciliation function returns an empty DataFrame with the documented output schema. A calling report should describe the live comparison as unavailable. Offline fixtures with small Yahoo and Alpaca DataFrames prove the `MATCH` and `BREAK` rules independently of vendor access.

## Planned Next Steps

- Extend DuckDB/Parquet/dbt coverage to options and the remaining derivative controls
- Add a live option reconciliation command with an explicit unavailable-data message
- Activate and smoke-test scheduled runs and webhook alerts
- Add exception aging and owner/SLA analytics
- Replace local secret files with a managed secret store for deployment
