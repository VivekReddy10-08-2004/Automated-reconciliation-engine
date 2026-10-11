# System Design & Workflow

This document outlines the architecture, data flow, and process logic for the **Automated Financial Reconciliation Engine**. The system is designed to mimic enterprise trading infrastructure, focusing on decoupling, real-time messaging, and data integrity.

---

## 1. High-Level Architecture
The system is built as a set of decoupled microservices. Instead of one large Python script, different "engines" run independently and communicate via a fast message broker (**ZeroMQ**).

### The Microservices:
1. **Module A (Ingestion Engine):** Connects to external APIs and WebSockets.
2. **Module E (The Bus):** The ZeroMQ network layer that distributes data.
3. **Module F (Anomaly Scoring):** An implemented prior-price percentage rule,
   with an optional offline Isolation Forest comparison.
4. **Module C (Reconciliation via dbt):** Uses dbt SQL models to compare vendor feeds.
5. **Module B (Storage Data Lakehouse):** Writes Parquet files and queries via DuckDB.
6. **Module D (Watchdog):** The alerting system.
7. **Module G (CLI Control-Room Dashboard):** An implemented read-only `rich` dashboard for DuckDB reconciliation analytics, with planned ZeroMQ panels for live feed and anomaly state (see Section 5).
8. **Module O (Orchestration):** Apache Airflow DAGs to run the batch ETL workflow.
9. **Infrastructure (Docker):** The entire stack is containerized using Docker and spun up via `docker-compose`.
10. **Security Control Plane:** Secrets, access roles, encryption, retention, masking, and audit events apply across every module.

---

## 2. Data Sources (100% Free Tier)
To ensure the project is fully open and free, we utilize:
*   **Source A (Baseline):** `yfinance` (Yahoo Finance) for end-of-day equity prices and **options chains** (`Ticker.option_chain()` exposes calls/puts with strike, expiry, bid/ask, implied volatility, open interest).
*   **Source B (Cross-Reference):** Alpaca REST API (free tier) for historical reconciliation of both **equities** (`StockHistoricalDataClient`) and **derivatives** (`OptionHistoricalDataClient` / `OptionChainRequest`).
*   **Source C (Real-Time):** Alpaca IEX WebSocket (free real-time US equities stream) for latency monitoring and ML ingestion.

**Derivatives** here means **options, futures, swaps, and forwards**. Sourcing differs by instrument because not everything has a free market feed:

| Instrument | Free source | Notes |
| --- | --- | --- |
| Equities | Alpaca + yfinance | OHLCV bars. |
| Options | Alpaca + yfinance | Full OPRA chains. |
| Futures (`reconciliation_engine/ingestion/futures.py`) | yfinance (`ES=F`, `CL=F`, …) | Continuous-contract OHLCV. Alpaca has no free futures feed. |
| Swaps (`reconciliation_engine/ingestion/swaps.py`) | Synthetic blotter + yfinance yields (`^IRX`, `^TNX`) | OTC: no public feed. Internal book marked vs a free reference rate. |
| Forwards (`reconciliation_engine/ingestion/forwards.py`) | Synthetic blotter + yfinance FX spot | OTC: booked forward marked vs a covered-interest-parity forward derived from live spot. |

All instrument classes flow through the same pipeline. **Equities/futures** are keyed by `(symbol, trade_date)`; **options** by the full OPRA identity `(underlying, expiry, strike, option_type, trade_date)`; **swaps/forwards** by `(trade_id, trade_date)` since OTC trades are identified by booking reference, not a listed symbol.

---

## 3. The Process & Workflow

### Phase 1: Real-Time Ingestion & Latency Monitoring
1.  **WebSocket Connection:** The Ingestion Engine connects to a live WebSocket feed (e.g., Binance crypto feed).
2.  **Packet Arrival:** A trade "tick" arrives.
3.  **Latency Calculation:** The engine extracts the **Exchange Timestamp** from the payload and subtracts it from the **Local Receive Timestamp** to calculate the network latency in milliseconds.
4.  **Publishing:** The tick (with its calculated latency) is serialized into JSON and pushed onto the **ZeroMQ Publisher Socket**.

### Phase 2: Anomaly Detection
1.  **Subscribing:** The anomaly scorer subscribes to the ZeroMQ bus and receives each tick.
2.  **Feature Extraction:** It calculates the percentage change from the prior price for each symbol.
3.  **Prediction:** A configurable, transparent absolute-percentage threshold sets `is_anomaly`; an optional deterministic **Isolation Forest** function supports offline comparison.
4.  **Re-Publishing:** The scorer preserves the raw fields, adds `pct_change`, `is_anomaly`, and `reason`, then publishes the annotated tick on a separate local endpoint.

### Phase 3: Historical Batch Ingestion & Reconciliation (Data Engineering Workflow)
*(This process is orchestrated by **Apache Airflow** and runs daily at Market Close)*
1.  **Batch Fetch:** The Ingestion Engine requests, from both Source A (Yahoo) and Source B (Alpaca):
    *   the daily closing price for each equity in the universe (e.g., `AAPL`), and
    *   the **options chain** for each underlying (e.g., the `SPY` chain), capturing per-contract `close`, `bid`, `ask`, `volume`, and `open_interest`.
2.  **Parquet Storage (Data Lake):** All payloads are saved as `.parquet` files (partitioned by instrument type) instead of being loaded directly into memory or a transactional DB.
3.  **dbt Transformation:** A `dbt` project runs against **DuckDB**. It reads the Parquet files as sources.
4.  **The Auditor SQL Models:** Two `dbt` models run the cross-reference:
    *   **Equities:** join the two sources on `(Ticker, Date)` and calculate `abs(Source_A_Price - Source_B_Price)`.
    *   **Derivatives:** join on the full contract key `(underlying, expiry, strike, option_type, Date)` and calculate the delta on the contract `close` (with the bid/ask midpoint as a fallback when last-trade prices are stale).
5.  **Break Detection:** If a delta exceeds our acceptable threshold, `dbt` flags a "Reconciliation Break" in the final transformed table. Options breaks additionally record the contract identity so the alert is actionable.

### Phase 4: Storage & Alerting
1.  **Data Lakehouse Architecture:** All raw ticks, anomalies, and reconciliation states are stored locally as Parquet files and queried via DuckDB, demonstrating modern DE storage patterns.
2.  **The Watchdog:** A separate script monitors the database and the message bus. If it detects an anomaly, a high-latency spike, or a reconciliation break, it triggers an HTTP POST request to the **Telegram Bot API**.
3.  **Notification:** The developer receives a push notification on their phone:
    *   *Equity, e.g., "⚠️ ALERT: Reconciliation Break on AAPL. Yahoo Finance: $150.00 | Alpaca: $150.75"*
    *   *Derivative, e.g., "⚠️ ALERT: Options Break on SPY 2024-12-20 C500. Yahoo Finance: $12.40 | Alpaca: $12.95"*

---

## 4. Why This Architecture Matters
*   **Fault Isolation:** If the Database goes down, the Ingestion Engine and Message Bus stay up. The system doesn't crash completely.
*   **Scalability:** Because we use ZeroMQ (Pub/Sub), we can add 5 more data sources tomorrow without rewriting the Storage Engine. The Storage Engine just subscribes to new topics.
*   **Observability:** The explicit tracking of latency and breaks proves an understanding of enterprise support operations.

---

## 5. CLI Control-Room Dashboard (Module G — Demo & Observability)

A terminal dashboard built with **`rich`** makes breaks and control health visible.
The implemented batch view polls DuckDB; the planned live view will add ZeroMQ
feed and anomaly panels without changing the dashboard's read-only role.

### What It Shows
The screen is a `rich` layout refreshed ~1–2×/second via `rich.live.Live`, divided into panels:

1.  **Latest Run (implemented):** Run ID, start time, status, matches, and breaks.
2.  **Largest Mismatches (implemented):** Source values and deltas for the latest run, with absent observations displayed as `MISSING`.
3.  **Operational Analytics (implemented):** Break-rate history, repeat offenders, and exception counts.
4.  **Exception Management Analytics (planned):** Owner, age, SLA breaches, and resolution trends after those fields are persisted.
5.  **Ingestion Feed (planned):** Recent ticks/bars, source, feed latency, and throughput.
6.  **Anomaly Monitor (planned):** Scored/flagged counts and explained recent anomalies.

### How It Gets Data (read-only consumer)
The dashboard is a **passive subscriber** — it never writes, so it can't affect the pipeline (consistent with the fault-isolation principle above):
*   **Batch panels (implemented)** connect to DuckDB with `read_only=True` and query curated run, reconciliation, and exception tables.
*   **Real-time panels (planned)** subscribe to the **ZeroMQ bus** (Module E), reading the same annotated ticks the ML engine publishes.

### Tech & Effort
*   **Library:** `rich` (`Live`, `Layout`, `Table`, `Panel`, `Progress`). Already a lightweight, pure-Python dependency.
*   **Scope:** The DuckDB analytics slice is implemented. ZeroMQ ingestion/anomaly panels and richer exception analytics remain planned.
*   **Run once:** `python -m reconciliation_engine.dashboard --db reconciliation.duckdb`.
*   **Poll continuously:** `python -m reconciliation_engine.dashboard --db reconciliation.duckdb --refresh 2`.

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
| Exception age, owner, and SLA | Expanded exception schema | Planned |
| Provider, rule, and instrument grouping | Unified reconciliation facts | Planned |
| Live latency and anomaly panels | ZeroMQ topics | Planned |

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
