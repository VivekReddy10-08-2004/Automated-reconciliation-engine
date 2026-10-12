# Release Verification Evidence

Verification was refreshed on October 12, 2026 from the repository root.

## Automated checks

| Check | Result |
| --- | --- |
| `python -m reconciliation_engine.smoke` | Passed: 13 imports, configuration, credentials, two local endpoints, and four DuckDB tables; no market-data request |
| `python -m unittest discover -s tests -v` | Passed: 73 tests |
| `dbt build --project-dir dbt --profiles-dir dbt` | Passed: 19 models/tests, 0 warnings, 0 errors |
| `git diff --check` | Passed |
| Tracked secret/runtime filename check | Passed; only the intentionally tracked `project.env.example` template matched the initial broad search |

## Live batch and replay

An isolated AAPL batch run completed against Yahoo and Alpaca using temporary storage:

```text
Run ID: 7d6f0211-480e-58fc-a631-a9d0c5ec199d
Status: SUCCESS
Yahoo rows: 5
Alpaca rows: 5
MATCH: 5
WARN: 0
BREAK: 0
Open exceptions: 0
```

Yahoo and Alpaca returned the same five market dates. Their largest closing-price difference was approximately `$0.22`, but it remained below the `0.10%` warning band and was correctly classified as a legitimate cross-feed match. An immediate retry returned the same run ID without provider calls or duplicate writes.

![DuckDB audit query for the verified run](release_evidence/duckdb-audit.svg)

## Dashboard evidence

The Task 20 captures remain valid release evidence for the read-only control room:

- [Both batch and real-time panels available](dashboard_demos/both-available.svg)
- [Batch unavailable while real-time remains available](dashboard_demos/batch-unavailable.svg)
- [Real-time unavailable while batch remains available](dashboard_demos/realtime-unavailable.svg)

The full suite also passed two live in-process ZeroMQ integration tests: ten valid simulated ticks, and scorer republication of an explained anomaly.

## Environment limitations

- Docker could not be built or run on this host because the `docker` executable is not installed. The Dockerfile and Compose contract tests passed, but Task 19 remains awaiting runtime verification.
- Airflow was not exercised because no Task 18 DAG exists in the repository. It remains planned and is not described as runnable.
- Unified futures, swap, and forward control reports were not exercised because Task 10 remains planned.

