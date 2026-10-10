# Automated Financial Data Ingestion & Reconciliation Engine

A Python MVP that fetches equity data from Yahoo Finance and Alpaca, normalizes both sources into one schema, and flags cross-source closing-price differences.

## Current MVP

- Normalized Yahoo Finance equity bars: `symbol, trade_date, open, high, low, close, volume`
- Normalized Yahoo Finance option chains for calls and puts
- Normalized Alpaca option chains using the same canonical option schema
- Normalized Alpaca daily equity bars using the same equity schema
- Equity reconciliation report with Yahoo/Alpaca close and volume differences
- Offline-tested Yahoo/Alpaca option reconciliation by full contract identity
- `MATCH` / `BREAK` status using a configurable close-price tolerance
- Immutable raw and normalized Parquet archives partitioned by source and run ID
- Read-only DuckDB dashboard for latest breaks and operational analytics
- Private local secret/database permissions and lazy credential loading

## Run

1. Create `project.env` with `ALPACA_API_KEY` and `ALPACA_SECRET_KEY`.
2. Install dependencies with `pip install -r requirements.txt`.
3. Run the reconciliation demo from the project root:

```powershell
python run_equity_reconciliation.py
```

The command compares recent daily AAPL bars, saves the audit trail to DuckDB,
and archives Yahoo, Alpaca, and reconciliation data under `data/`. Each archive
contains its `run_id` and an existing run file is never overwritten.

Replay a saved run without calling Yahoo or Alpaca:

```powershell
python run_equity_reconciliation.py --replay-run-id <run-id>
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

View a one-time reconciliation control-room snapshot:

```powershell
python -m Dashboard.dashboard --db reconciliation.duckdb
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
Ingestion/Equities_yf.py            Yahoo equity ingestion and normalization
Ingestion/options_yf.py             Yahoo options ingestion and normalization
Ingestion/Equities.py               Alpaca equity ingestion and normalization
Ingestion/equity_reconciliation.py  Yahoo vs. Alpaca equity comparison
Ingestion/options_reconciliation.py Yahoo vs. Alpaca option comparison logic
Dashboard/dashboard.py              Read-only break analytics dashboard
data_lake.py                         Immutable Parquet archive and replay helpers
run_equity_reconciliation.py         Live run and offline replay commands
security.py                         Local secret and file-permission controls
```

## Options Data Availability

Comparable Alpaca option contracts may be unavailable because of account entitlements, subscription level, contract expiration, or provider coverage. The project does not replace missing Alpaca observations with Yahoo values or synthetic prices and does not report an unavailable comparison as a `MATCH`.

When either provider returns no option contracts, the reconciliation function returns an empty DataFrame with the documented output schema. A calling report should describe the live comparison as unavailable. Offline fixtures with small Yahoo and Alpaca DataFrames prove the `MATCH` and `BREAK` rules independently of vendor access.

## Planned Next Steps

- Extend DuckDB/Parquet persistence to options and the remaining derivative controls
- Add a live option reconciliation command with an explicit unavailable-data message
- Activate and smoke-test scheduled runs and webhook alerts
- Add exception aging and owner/SLA analytics
- Replace local secret files with a managed secret store for deployment
