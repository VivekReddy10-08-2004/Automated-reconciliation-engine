# System Design & Workflow

This document outlines the architecture, data flow, and process logic for the **Automated Financial Reconciliation Engine**. The system is designed to mimic enterprise trading infrastructure, focusing on decoupling, real-time messaging, and data integrity.

---

## 1. High-Level Architecture
The system is built as a set of decoupled microservices. Instead of one large Python script, different "engines" run independently and communicate via a fast message broker (**ZeroMQ**).

### The Microservices:
1. **Module A (Ingestion Engine):** Connects to external APIs and WebSockets.
2. **Module E (The Bus):** The ZeroMQ network layer that distributes data.
3. **Module F (Machine Learning):** The real-time anomaly predictor.
4. **Module C (Reconciliation via dbt):** Uses dbt SQL models to compare vendor feeds.
5. **Module B (Storage Data Lakehouse):** Writes Parquet files and queries via DuckDB.
6. **Module D (Watchdog):** The alerting system.
7. **Module G (Live CLI Dashboard):** A `rich`-based terminal dashboard that visualizes pipeline status in real time (see Section 5).
8. **Module O (Orchestration):** Apache Airflow DAGs to run the batch ETL workflow.
9. **Infrastructure (Docker):** The entire stack is containerized using Docker and spun up via `docker-compose`.

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
| Futures (`Ingestion/futures.py`) | yfinance (`ES=F`, `CL=F`, …) | Continuous-contract OHLCV. Alpaca has no free futures feed. |
| Swaps (`Ingestion/swaps.py`) | Synthetic blotter + yfinance yields (`^IRX`, `^TNX`) | OTC: no public feed. Internal book marked vs a free reference rate. |
| Forwards (`Ingestion/forwards.py`) | Synthetic blotter + yfinance FX spot | OTC: booked forward marked vs a covered-interest-parity forward derived from live spot. |

All instrument classes flow through the same pipeline. **Equities/futures** are keyed by `(symbol, trade_date)`; **options** by the full OPRA identity `(underlying, expiry, strike, option_type, trade_date)`; **swaps/forwards** by `(trade_id, trade_date)` since OTC trades are identified by booking reference, not a listed symbol.

---

## 3. The Process & Workflow

### Phase 1: Real-Time Ingestion & Latency Monitoring
1.  **WebSocket Connection:** The Ingestion Engine connects to a live WebSocket feed (e.g., Binance crypto feed).
2.  **Packet Arrival:** A trade "tick" arrives.
3.  **Latency Calculation:** The engine extracts the **Exchange Timestamp** from the payload and subtracts it from the **Local Receive Timestamp** to calculate the network latency in milliseconds.
4.  **Publishing:** The tick (with its calculated latency) is serialized into JSON and pushed onto the **ZeroMQ Publisher Socket**.

### Phase 2: Anomaly Detection (Machine Learning)
1.  **Subscribing:** The ML Engine is subscribed to the ZeroMQ bus and receives the tick instantly.
2.  **Feature Extraction:** It calculates rolling metrics (e.g., % change from the last 10 ticks).
3.  **Prediction:** An **Isolation Forest** model evaluates the tick. If it falls outside the statistical norm, it is flagged as `IS_ANOMALY = True`.
4.  **Re-Publishing:** The ML Engine pushes the annotated tick back to the bus.

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

## 5. Live CLI Dashboard (Module G — Demo & Observability)

A single-screen terminal dashboard built with **`rich`** that makes the pipeline *visible while it runs*. It turns a demo from "I built this" into "watch me run it" — the operator sees data flowing, anomalies firing, and reconciliation results updating live.

### What It Shows
The screen is a `rich` layout refreshed ~1–2×/second via `rich.live.Live`, divided into panels:

1.  **Ingestion Feed (live):** Rolling table of the most recent ticks/bars per source and instrument type (equity, option, future, swap, forward) — symbol, price, source (A/B), and **feed latency (ms)**. Throughput counters per source.
2.  **Anomaly Monitor (ML):** Count of ticks scored, count flagged `IS_ANOMALY`, and a rolling list of the latest flagged items (symbol, value, why). Color-coded (green = clean, red = anomaly).
3.  **Reconciliation Results:** Live PASS / WARN / BREAK tallies from the latest run, plus the **top breaks** (instrument, source A vs B, % delta) — equities, options, futures, swaps, and forwards in one view.
4.  **Pipeline Health:** Current `run_id`, run status (`RUNNING` / `SUCCESS` / `SUCCESS_WITH_BREAKS` / `FAILED`), uptime, and last Watchdog alert sent.

### How It Gets Data (read-only consumer)
The dashboard is a **passive subscriber** — it never writes, so it can't affect the pipeline (consistent with the fault-isolation principle above):
*   **Real-time panels** (ingestion, anomalies) subscribe to the **ZeroMQ bus** (Module E), reading the same annotated ticks the ML engine publishes.
*   **Batch panels** (reconciliation, health) poll **DuckDB / Parquet** (Module B) for the latest `recon_results` and `pipeline_runs` rows on each refresh.

### Tech & Effort
*   **Library:** `rich` (`Live`, `Layout`, `Table`, `Panel`, `Progress`). Already a lightweight, pure-Python dependency.
*   **Scope:** ~1 day. Start by polling DuckDB only (no bus needed) to render the recon + health panels, then wire the ZeroMQ subscriber for the live ingestion/anomaly panels.
*   **Run:** `uv run python -m dashboard` (suggested entry point `dashboard.py` / `Dashboard/`).

### Why It's Worth It
*   **Demo impact:** A live, updating terminal UI is far more convincing in interviews than static logs or screenshots.
*   **Observability proof:** Reinforces the Trade-Ops "control room" mindset — one glance shows whether the pipeline is healthy and whether any breaks need action.
