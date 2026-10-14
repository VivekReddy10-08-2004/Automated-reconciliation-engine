# Automated Reconciliation Engine: Current System and Roadmap

This document contains two deliberately separate views:

1. the components that exist in release `v1.0.0`; and
2. the target architecture and Task 1–21 roadmap.

Boxes in the target diagrams are not claims of current operation. The canonical
verification boundary is the [Feature Support Matrix](FEATURE_SUPPORT.md).

Tasks 1–9, 11–17, and 20 represent the implemented local equity-control and
simulated real-time release. The options persistence command is implemented and
offline-tested, while live options execution is unverified. Task 19's batch
container contract is implemented but awaits a Docker host. Task 10 futures
persistence and Task 18 Airflow orchestration remain planned. Managed secrets,
RBAC, TLS, and backup controls are shared-deployment targets, not local-runtime
features.

The supported options workflow uses Yahoo/Alpaca. Massive options and
Yahoo/Massive futures are experimental library capabilities outside the current
persisted architecture. Future derivatives expansion depends on verified
field/time/contract comparability; see the
[promotion criteria](FEATURE_SUPPORT.md#derivatives-scope-and-promotion-criteria).

## 1. Current v1.0.0 Architecture

```mermaid
flowchart LR
    providers["Yahoo + Alpaca + Massive"] --> ingestion["Validated partitioned ingestion"]
    ingestion --> parquet["Immutable Parquet checkpoints"]
    ingestion --> equity["Three-provider equity reconciliation"]
    options["Yahoo + Alpaca option chains"] --> optionControl["Persisted option reconciliation\noffline verified"]
    equity --> duckdb["Atomic DuckDB control state"]
    optionControl --> duckdb
    duckdb --> dbt["dbt equity facts"]
    duckdb --> snapshot["Atomic read-only snapshot"]
    snapshot --> api["Authenticated export API"]
    duckdb --> dashboard["Read-only dashboard"]
    simulator["Simulated ticks"] --> zeromq["ZeroMQ"]
    zeromq --> scorer["Explainable anomaly scorer"]
    scorer --> dashboard
    ingestion & equity & api --> telemetry["OpenTelemetry instrumentation\noffline verified"]
    scheduler["cron / Windows Task Scheduler"] --> ingestion
```

Current exclusions are equally important: no Airflow DAG, no persisted futures
workflow, no live-verified options evidence, no verified Docker execution, and
no verified external telemetry backend.

## 2. Future Target Architecture

The following is a roadmap diagram. Massive derivatives paths are conditional
on comparable provider observations and live evidence. Airflow, managed secrets, RBAC, TLS,
encrypted backups, cross-instrument dashboards, and immutable access auditing
are future deployment capabilities unless the support matrix says otherwise.

```mermaid
flowchart LR
    subgraph sources["Market and Trade Sources"]
        yahoo["Yahoo Finance: equities, options, futures"]
        alpaca["Alpaca: equities, options, live ticks"]
        massive["Massive: equities, options, futures"]
        simulator["Deterministic tick simulator"]
    end

    subgraph orchestration["Triggers and Orchestration"]
        localScheduler["Local weekday scheduler (Task 11)"]
        airflow["Airflow DAG (Task 18)"]
    end

    subgraph batch["Batch Control Pipeline"]
        batchRunner["Equity control runner (Tasks 5-8)"]
        ingestion["Provider ingestion (Tasks 1-3 and 9-10)"]
        normalization["Canonical schemas (Tasks 4 and 9)"]
        quality["Schema and data-quality controls (Tasks 6 and 10)"]
        reconciliation["Equity and option reconciliation (Tasks 5 and 9)"]
        markControls["Mapped futures controls"]
        exceptionManager["Exception lifecycle (Task 8)"]
        notifier["Break and failure notifications (Task 11)"]
    end

    subgraph data["Audit and Transformation Layer"]
        parquet["Raw and normalized Parquet archive (Task 14)"]
        duckdb["DuckDB staging, runs, results, exceptions (Task 7)"]
        dbt["dbt staging and reconciliation models (Task 15)"]
    end

    subgraph realtime["Real-Time Monitoring Path"]
        publisher["Live or simulated tick publisher (Task 16)"]
        rawBus["ZeroMQ raw tick topic (Task 16)"]
        scorer["Rule-based and optional ML anomaly scorer (Task 17)"]
        scoredBus["ZeroMQ annotated tick topic (Task 17)"]
    end

    subgraph presentation["Operator Experience"]
        cliReport["Single-run CLI report (Task 8)"]
        staticDashboard["Read-only break analytics dashboard (Task 12)"]
        liveDashboard["Batch and real-time control room (Task 20)"]
    end

    subgraph security["Security and Governance"]
        secrets["Managed secrets and credential rotation"]
        access["Service identities, RBAC, and masked views"]
        protection["TLS, encryption at rest, retention, and backups"]
        auditEvents["Immutable access and change audit events"]
    end

    subgraph delivery["Release and Deployment"]
        docker["Docker Compose and persistent volumes (Task 19)"]
        mvpRelease["Safe GitHub MVP release (Task 13)"]
        runbook["Full-system runbook and smoke checks (Task 21)"]
    end

    localScheduler -->|"Starts local run"| batchRunner
    airflow -->|"Orchestrates idempotent run"| batchRunner
    batchRunner -->|"Invokes providers"| ingestion
    yahoo -->|"Historical market data"| ingestion
    alpaca -->|"Historical market data"| ingestion
    massive -->|"Historical market data"| ingestion
    ingestion -->|"Archives provider payloads"| parquet
    ingestion -->|"Maps vendor fields"| normalization
    normalization -->|"Archives canonical data"| parquet
    normalization -->|"Validates records"| quality
    quality -->|"Valid equity and option rows"| reconciliation
    quality -->|"Mapped futures inputs"| markControls
    reconciliation -->|"Stores MATCH, WARN, BREAK"| duckdb
    markControls -->|"Stores MATCH or BREAK"| duckdb
    reconciliation -->|"Creates break records"| exceptionManager
    markControls -->|"Creates control breaks"| exceptionManager
    exceptionManager -->|"Persists open exceptions"| duckdb
    exceptionManager -->|"Signals actionable breaks"| notifier
    batchRunner -->|"Signals run failure"| notifier
    duckdb -->|"Provides staged sources"| dbt
    dbt -->|"Materializes reconciliation facts"| duckdb
    duckdb -->|"Supplies run summary"| cliReport
    duckdb -->|"Supplies latest batch state"| staticDashboard
    duckdb -->|"Supplies batch panel"| liveDashboard
    alpaca -->|"Live ticks when available"| publisher
    simulator -->|"Fallback simulated ticks"| publisher
    publisher -->|"Publishes tick and latency"| rawBus
    rawBus -->|"Feeds scorer"| scorer
    scorer -->|"Publishes annotated ticks"| scoredBus
    scoredBus -->|"Supplies anomaly panel"| liveDashboard
    secrets -->|"Supplies credentials at runtime"| ingestion
    access -->|"Limits curated query surface"| staticDashboard
    access -->|"Limits curated query surface"| liveDashboard
    protection -->|"Protects archives and state"| parquet
    protection -->|"Protects database and backups"| duckdb
    batchRunner -->|"Records execution events"| auditEvents
    exceptionManager -->|"Records state changes"| auditEvents
    staticDashboard -->|"Records exports and access"| auditEvents
    docker -->|"Packages batch services"| batchRunner
    docker -->|"Persists data volumes"| duckdb
    mvpRelease -->|"Documents local MVP"| runbook
    cliReport -->|"Operational evidence"| runbook
    liveDashboard -->|"Demo evidence"| runbook
```

## 3. Future End-State Batch and Real-Time Workflow

This workflow intentionally includes planned cross-instrument and failure-state
behavior. It is not the runbook for the current release.

```mermaid
flowchart TD
    trigger["Weekday trigger after US market close"] --> config["Load configuration and validate credentials"]
    config --> runId["Create run ID and RUNNING audit record"]
    runId --> ingestYahoo["Fetch Yahoo equities, options, and futures"]
    runId --> ingestAlpaca["Fetch Alpaca equities and options"]
    runId --> ingestMassive["Fetch Massive equities, options, and futures"]

    ingestYahoo --> archiveRaw["Archive immutable raw payloads by run ID"]
    ingestAlpaca --> archiveRaw
    ingestMassive --> archiveRaw
    archiveRaw --> normalize["Normalize instrument-specific canonical schemas"]
    normalize --> validate["Validate schema, dates, values, and required market marks"]
    validate --> stage["Write normalized Parquet and DuckDB staging rows"]

    stage --> equityRecon["Reconcile equities by symbol and trade date"]
    stage --> optionRecon["Reconcile options by full contract identity"]
    stage --> derivativeControls["Run mapped futures reconciliation"]

    equityRecon --> transform["Build dbt reconciliation facts"]
    optionRecon --> transform
    derivativeControls --> results["Persist control results"]
    transform --> results
    results --> outcome{"Any BREAK or failure?"}

    outcome -->|"No"| success["Finish as SUCCESS"]
    outcome -->|"Break or missing mark"| exceptions["Create OPEN exceptions"]
    exceptions --> notifyBreak["Send one action-required summary"]
    notifyBreak --> withBreaks["Finish as SUCCESS_WITH_BREAKS"]
    outcome -->|"Pipeline failure"| notifyFailure["Persist failure and send notification"]
    notifyFailure --> failed["Finish as FAILED"]

    success --> audit["Retain run metrics, evidence, and lineage"]
    withBreaks --> audit
    failed --> audit
    audit --> batchPanel["CLI report and read-only break analytics dashboard"]

    liveSource["Alpaca stream or simulated ticks"] --> publish["Publish timestamped ticks"]
    publish --> latency["Calculate non-negative latency in milliseconds"]
    latency --> anomaly["Score percentage-change anomalies"]
    anomaly --> annotated["Publish annotated ticks with reason"]
    annotated --> realtimePanel["Read-only real-time dashboard panel"]
    batchPanel --> controlRoom["Combined control-room dashboard"]
    realtimePanel --> controlRoom
```

## 4. Task 1–21 Dependency Roadmap

```mermaid
flowchart LR
    task1["Task 1: Alpaca equities"] --> task2["Task 2: Alpaca options"]
    task2 --> task3["Task 3: Yahoo secondary source"]
    task3 --> task4["Task 4: Yahoo normalization"]
    task4 --> task5["Task 5: Equity reconciliation"]
    task5 --> task6["Task 6: Quality checks and tests"]
    task6 --> task7["Task 7: DuckDB audit storage"]
    task7 --> task8["Task 8: Exceptions and CLI report"]
    task8 --> task13["Task 13: GitHub MVP release"]

    task2 --> task9["Task 9: Option reconciliation"]
    task4 --> task9
    task5 --> task9
    task8 --> task10["Task 10: Derivative mark controls"]
    task8 --> task11["Task 11: Alerting and local scheduling"]
    task7 --> task12["Task 12: Read-only break analytics dashboard"]

    task7 --> task14["Task 14: Parquet data lake"]
    task8 --> task14
    task7 --> task15["Task 15: dbt DuckDB models"]
    task14 --> task15
    task13 --> task16["Task 16: ZeroMQ real-time feed"]
    task16 --> task17["Task 17: Anomaly detection"]
    task11 --> task18["Task 18: Airflow orchestration"]
    task8 --> task18
    task18 --> task19["Task 19: Containerization"]
    task12 --> task20["Task 20: Live dashboard integration"]
    task16 --> task20
    task17 --> task20

    task9 --> task21["Task 21: Final system release"]
    task10 --> task21
    task15 --> task21
    task18 --> task21
    task19 --> task21
    task20 --> task21

    classDef complete fill:#d1fae5,stroke:#047857,color:#064e3b
    classDef verification fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef planned fill:#e0e7ff,stroke:#4338ca,color:#312e81
    class task1,task2,task3,task4,task5,task6,task7,task8,task9,task11,task12,task13,task14,task15,task16,task17,task20 complete
    class task19,task21 verification
    class task10,task18 planned
```

Green nodes are implemented, including persisted offline-verified option reconciliation, local
notification/scheduling, the DuckDB break-analytics slice of Task 12, the
equity Parquet and dbt layers, the simulated ZeroMQ feed, the explainable
anomaly scorer, and the fault-isolated dashboard. Task 19 is amber until its
batch image is built and run on a Docker host. Task 21 is amber because its
release verification assets are implemented while Tasks 10 and 18 still gate a
literal full-architecture release. Purple nodes remain on the roadmap.

## 5. Core Data Contracts

| Domain | Canonical identity | Main control outcome |
| --- | --- | --- |
| Equity | `symbol, trade_date` | `MATCH`, `WARN`, `BREAK` |
| Option | `underlying, expiry, strike, option_type` | `MATCH`, `WARN`, `BREAK` |
| Future | `symbol, trade_date` | data-quality `PASS` or `BREAK` |
| Real-time tick | `symbol, exchange_timestamp` | normal or explained anomaly |

## 6. Dashboard and Security Acceptance Criteria

| Capability | Local MVP | Shared deployment target |
| --- | --- | --- |
| Break visualization | Latest run, missing observations, largest deltas | Unified cross-instrument views |
| Analytics | Break trend, repeat offenders, exception counts | Age, owner, SLA, provider, rule, and resolution trends |
| Database access | DuckDB read-only connection | Dedicated least-privilege service identity and curated views |
| Secrets | Git-ignored owner-only file, lazy loading | Managed secret store and rotation policy |
| Data protection | Owner-only database file | TLS, encryption at rest, encrypted backups, retention jobs |
| Sensitive fields | Raw staging excluded from dashboard | Role-based masking and export controls |
| Audit | Run and exception lineage | Immutable access, export, rule-change, and admin events |

The batch and real-time paths remain intentionally independent. DuckDB or ZeroMQ can fail without forcing the other dashboard panel to stop, and the dashboard remains a read-only consumer throughout.

The analytics layer is operational rather than predictive: it prioritizes the
current largest mismatches, missing observations, break-rate history, repeat
offenders, exception age, and SLA breaches. Security controls apply end to end:
runtime secret access, encrypted transport/storage, least-privilege curated
views, masking of sensitive trade fields, retention, and immutable audit events.
