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

View a one-time reconciliation control-room snapshot:

```powershell
python -m reconciliation_engine.dashboard --db reconciliation.duckdb
```

Add `--refresh 2` for a continuously updating view. The dashboard opens DuckDB
in read-only mode and displays the latest run, break-rate history, largest
mismatches, repeat offenders, and exception-queue counts.

| Panel | Operator question |
| --- | --- |
| Latest run | Did the control complete, and how many records matched or broke? |
| Largest mismatches | Which differences or missing observations need attention first? |
| Break-rate trend | Is reconciliation quality improving or deteriorating across runs? |
| Repeat offenders | Which instruments and metrics repeatedly fail the control? |
| Exception queue | How much unresolved work remains? |

This is operational analytics for triage and control monitoring. Exception age,
owners, SLA breaches, and provider/instrument grouping remain planned until the
corresponding fields are persisted.

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
│       ├── publisher.py
│       └── subscriber.py
├── dbt/                           # Versioned DuckDB SQL transformations
│   ├── models/
│   └── tests/
├── tests/                         # Offline unit and integration tests
├── docs/                          # Architecture, operations, and security
├── orchestration/                 # Future Airflow and deployment definitions
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
