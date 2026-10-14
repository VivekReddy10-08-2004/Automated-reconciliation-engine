# System Design & Workflow

This document describes the current architecture and release boundary for the
**Automated Financial Reconciliation Engine**. The repository contains a
live-verified persisted equity control, an offline-verified persisted
Yahoo/Alpaca options control, and an independent simulated real-time
demonstration. Future deployment capabilities are labeled explicitly. See the
[Feature Support Matrix](FEATURE_SUPPORT.md) for the canonical evidence boundary.

---

## 1. High-Level Architecture
The system is a local Python application with independently runnable batch, real-time, scoring, and dashboard processes. Only the simulated real-time path communicates over **ZeroMQ**; the batch path persists to Parquet and DuckDB.

### The Microservices:
1. **Module A (Ingestion Engine):** Connects to Yahoo, Alpaca, and Massive HTTP APIs. Massive assets share one quota controller. A live WebSocket source is not implemented.
2. **Module E (The Bus):** The ZeroMQ network layer that distributes data.
3. **Module F (Anomaly Scoring):** An implemented prior-price percentage rule,
   with an optional offline Isolation Forest comparison.
4. **Module C (Reconciliation via dbt):** Uses dbt SQL models to compare vendor feeds.
5. **Module B (Storage Data Lakehouse):** Writes Parquet files and queries via DuckDB.
6. **Module D (Watchdog):** The alerting system.
7. **Module G (CLI Control-Room Dashboard):** An implemented read-only `rich` dashboard combining DuckDB reconciliation state with a ZeroMQ anomaly feed (see Section 5).
8. **Module O (Orchestration, planned):** No Apache Airflow DAG is included; Windows Task Scheduler and Ubuntu cron are documented.
9. **Infrastructure (Docker, contract verified):** Docker Compose defines the one-shot equity batch runner with persistent state, but image execution was unavailable on the evidence host.
10. **Security controls:** Owner-only local secrets and database files are implemented. Managed secrets, RBAC, TLS, backups, and immutable access auditing are future deployment controls.
11. **Read-Only Export API:** FastAPI serves authenticated, filtered run,
    reconciliation, exception, and export views through bounded DuckDB readers.
12. **Observability Plane:** OpenTelemetry signal production and aggregate alert
    rules are offline-tested. The optional Collector and backend stack were not
    runtime-verified on the evidence host.

---

## 2. Data Sources
The current integrations are:
*   **Source A (Baseline):** `yfinance` (Yahoo Finance) for unadjusted end-of-day equity prices and **options chains** (`Ticker.option_chain()` exposes calls/puts with strike, expiry, bid/ask, implied volatility, open interest).
*   **Source B (Cross-Reference):** Alpaca REST API for historical equities (`StockHistoricalDataClient`) and option snapshots (`OptionHistoricalDataClient` / `OptionChainRequest`). Equity batch requests explicitly use IEX; option coverage depends on account entitlement.
*   **Source C:** Massive REST is integrated into the equity control. Its options contract/aggregate and futures contract/session adapters are experimental library capabilities. Requests share one process-wide quota controller.
*   **Real-time demo input:** deterministic simulated equity ticks. No Alpaca WebSocket is connected in this release.

Instrument coverage and integration boundaries:

| Instrument | Sources | Notes |
| --- | --- | --- |
| Equities | Alpaca + yfinance + Massive | Massive supports per-symbol comparison and one-call grouped US-market EOD acquisition. |
| Options | Supported workflow: Alpaca + yfinance | Persisted and offline-verified. Massive contract/aggregate adapters and three-provider helpers are experimental. |
| Futures | Experimental libraries: yfinance + Massive | Continuous versus expiring contract/session alignment is required; persisted orchestration is planned. |

The persisted equity batch compares all three provider pairs and records
provider lineage in DuckDB, Parquet, dbt, exceptions, and the dashboard.
The supported persisted options command controls Yahoo against Alpaca and is
offline-verified only. Massive options ingestion and three-provider comparison
helpers are experimental: historical aggregate closes need matching observation
times and price definitions before comparison with chain snapshots. Experimental
Yahoo/Massive futures helpers require contract-roll and session alignment, and
compare closes while retaining Massive settlement separately. Live comparability
and persisted end-to-end evidence are required before promotion. See
[Data Sources](../DATA_SOURCES.md) for provenance and limitations.

---

## 3. The Process & Workflow

### Phase 1: Real-Time Ingestion & Latency Monitoring
1.  **Simulation:** A deterministic local publisher creates an equity tick.
2.  **Packet Arrival:** The tick carries controlled exchange and receive timestamps.
3.  **Latency Calculation:** The engine extracts the **Exchange Timestamp** from the payload and subtracts it from the **Local Receive Timestamp** to calculate the network latency in milliseconds.
4.  **Publishing:** The tick (with its calculated latency) is serialized into JSON and pushed onto the **ZeroMQ Publisher Socket**.

### Phase 2: Anomaly Detection
1.  **Subscribing:** The anomaly scorer subscribes to the ZeroMQ bus and receives each tick.
2.  **Feature Extraction:** It calculates the percentage change from the prior price for each symbol.
3.  **Prediction:** A configurable, transparent absolute-percentage threshold sets `is_anomaly`; an optional deterministic **Isolation Forest** function supports offline comparison.
4.  **Re-Publishing:** The scorer preserves the raw fields, adds `pct_change`, `is_anomaly`, and `reason`, then publishes the annotated tick on a separate local endpoint.

### Phase 3: Historical Batch Ingestion & Reconciliation (Data Engineering Workflow)
*(This process is run manually or by the documented local OS scheduler. Airflow is planned.)*
1.  **Idempotency Check:** A deterministic run ID is derived from the control date, sorted universe, and rule version. A completed retry returns without provider calls or writes.
2.  **Batch Fetch:** The Ingestion Engine requests recent daily OHLCV bars from Yahoo, Alpaca, and Massive for each configured equity.
3.  **Parquet Storage (Data Lake):** Payloads are saved once as immutable `.parquet` files partitioned by run ID. An interrupted retry reuses completed partitions.
4.  **Validation and Alignment:** Symbols and dates are normalized; duplicate keys, nulls, invalid types, negative values, and inconsistent OHLC ranges are rejected.
5.  **dbt Transformation:** A `dbt` project runs against **DuckDB** and joins equity sources on `(run_id, symbol, trade_date)`.
6.  **Break Detection:** Python and dbt classify equity rows as `MATCH`, `WARN`, or `BREAK` using the same thresholds. One-sided provider observations are retained and break.

### Phase 4: Storage & Alerting
1.  **Batch evidence:** Equity provider payloads, normalized bars, and reconciliation results are archived as Parquet; run, staging, result, and exception rows commit to DuckDB in one transaction or roll back together.
2.  **Notification:** The batch sends a generic webhook summary only for `FAILED` or `SUCCESS_WITH_BREAKS`; without a webhook it prints the same non-sensitive summary.
3.  **Current limit:** Real-time ticks and anomaly decisions are not persisted and do not trigger notifications.

### Phase 5: Read-Only Distribution

1. **Authentication:** `/api/v1/*` requests require a bearer token; `/health`
   remains public for availability probes.
2. **Queries:** Dashboards and services use paginated, parameterized filters over
   curated reconciliation tables rather than raw provider payloads.
3. **Exports:** Filtered result sets can be downloaded as CSV, JSON, or Parquet,
   subject to an explicit row limit.
4. **Concurrency:** One local API process uses short-lived read-only DuckDB
   connections behind a bounded semaphore. Scale-out deployments query a
   snapshot, transactional store, or warehouse instead of sharing the live file.

---

## 4. Why This Architecture Matters
*   **Fault Isolation:** If the Database goes down, the Ingestion Engine and Message Bus stay up. The system doesn't crash completely.
*   **Extensibility:** ZeroMQ separates the simulated publisher, scorer, and dashboard subscriber, while canonical batch schemas separate provider adapters from reconciliation logic.
*   **Observability:** The explicit tracking of latency and breaks proves an understanding of enterprise support operations.

---

## 5. CLI Control-Room Dashboard (Module G — Demo & Observability)

A terminal dashboard built with **`rich`** makes breaks and control health visible.
Independent batch and live panels poll DuckDB and the annotated ZeroMQ feed
without changing the dashboard's read-only role.

### What It Shows
The screen is a `rich` layout refreshed ~1–2×/second via `rich.live.Live`, divided into panels:

1.  **Latest Run (implemented):** Run ID, start time, status, matches, and breaks.
2.  **Largest Mismatches (implemented):** Source values and deltas for the latest run, with absent observations displayed as `MISSING`.
3.  **Operational Analytics (implemented):** Break-rate history, repeat offenders, and exception counts.
4.  **Exception Management Analytics (implemented foundation):** Owner, age, SLA breaches, and resolution fields are persisted; the dashboard shows unassigned, overdue, and oldest-open counts.
5.  **Ingestion Feed (implemented):** Most recent annotated tick, feed latency, and received count.
6.  **Anomaly Monitor (implemented):** Rolling recent anomaly count and latest explained anomaly.

### How It Gets Data (read-only consumer)
The dashboard is a **passive subscriber** — it never writes, so it can't affect the pipeline (consistent with the fault-isolation principle above):
*   **Batch panels (implemented)** connect to DuckDB with `read_only=True` and query curated run, reconciliation, and exception tables.
*   **Real-time panels (implemented)** use a `SUB`-only socket on the **ZeroMQ bus** (Module E), reading the same annotated ticks the scorer publishes.

### Tech & Effort
*   **Library:** `rich` (`Live`, `Layout`, `Table`, `Panel`, `Progress`). Already a lightweight, pure-Python dependency.
*   **Scope:** DuckDB batch state and ZeroMQ ingestion/anomaly panels are implemented. Richer exception analytics remain planned.
*   **Run once:** `python -m reconciliation_engine.dashboard --db reconciliation.duckdb`.
*   **Poll continuously:** `python -m reconciliation_engine.dashboard --db reconciliation.duckdb --refresh 0.5`.

### Why It's Worth It
*   **Demo impact:** A live, updating terminal UI is far more convincing in interviews than static logs or screenshots.
*   **Observability proof:** Reinforces the Trade-Ops "control room" mindset — one glance shows whether the pipeline is healthy and whether any breaks need action.

### Analytics That Support Reconciliation

Analytics make sense when they shorten investigation or reveal control weakness.
The implemented slice shows break rate by run, largest current deltas, repeat
offenders, and exception counts. The next slice adds open-exception age, SLA
breaches, and grouping by provider, instrument type, and reconciliation rule.
Predictive or portfolio analytics should wait until the control data is complete
and reliable.

The dashboard connects to DuckDB in read-only mode and reads curated results;
raw payloads and credentials are outside its query surface. Row-level trade or
counterparty fields must be masked according to the viewer's role.

### Analytics Contract

| Metric | Current source | State |
| --- | --- | --- |
| Latest matches and breaks | `pipeline_runs` | Implemented |
| Largest mismatches and missing observations | `equity_reconciliation_results` | Implemented |
| Break-rate history | `pipeline_runs` | Implemented |
| Repeat offenders | `equity_reconciliation_results` | Implemented |
| Exception counts by status | `equity_exceptions` | Implemented |
| Exception age, owner, and SLA | `equity_exceptions` | Implemented foundation |
| Provider, rule, and instrument grouping | Unified reconciliation facts | Planned |
| Live latency and anomaly panels | ZeroMQ annotated-tick endpoint | Implemented |

---

## 6. Data Security

The local MVP keeps secrets in a Git-ignored owner-only environment file and
restricts DuckDB files to the current OS user. Provider clients load credentials
only when a live request is made, so offline tests and dashboard use do not need
secret access.

A deployed version must use managed secrets with rotation, TLS, encryption at
rest, service identities, role-based access, explicit retention, encrypted
backups, and immutable audit events. Every reconciliation result should retain
the source lineage and rule/config version needed to prove its integrity. See
[DATA_SECURITY.md](DATA_SECURITY.md) for the complete control plan.
