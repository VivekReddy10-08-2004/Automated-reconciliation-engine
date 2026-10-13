# Release Verification Evidence

Verification was refreshed on October 13, 2026 from the repository root.

## Automated checks

| Check | Result |
| --- | --- |
| `python -m reconciliation_engine.smoke` | Passed: 26 imports, configuration, credentials, two local endpoints, and seven DuckDB tables; no market-data request |
| `python -m unittest discover -s tests -v` | Passed: 88 tests |
| `dbt build --project-dir dbt --profiles-dir dbt` | Passed: 23 models/tests, 0 warnings, 0 errors |
| `git diff --check` | Passed |
| Tracked secret/runtime filename check | Passed; only the intentionally tracked `project.env.example` template matched the initial broad search |

The release now also has a GitHub Actions gate for tests, dbt, Ruff formatting
and lint, Bandit, dependency auditing, the container contract, correctness,
recovery, and a minimal offline end-to-end integration.

## Benchmark, recovery, and load evidence

The reproducible harness and raw results are in [the benchmark package](../benchmarks/README.md).

| Verification | Result |
| --- | --- |
| 100K reconciliation | 3.045 s, 32,840 rows/s, 276.04 MB peak RSS |
| 1M reconciliation | 30.173 s, 33,142 rows/s, 1,066.08 MB peak RSS |
| Correctness oracle | No missing rows, duplicate keys, or missed deliberate breaks |
| Process crash boundaries | Passed before archive, after archive, and after commit |
| Logical-run idempotency | Second write rejected; one run remained |
| API load, 1/10/25/50 clients | Zero errors; p50/p95/p99 and throughput committed as CSV |
| Fault injection | Timeout and 429 retry; corrupt input and disk failure quarantined |
| Offline end-to-end demo | 2,000 input rows, one deliberate break, recovery, CSV export, trace |

An atomic DuckDB snapshot publisher now separates active batch writes from API
reads. The API load fixture uses the published read-only pattern.

## Live batch and replay

An isolated AAPL batch run completed against Yahoo, Alpaca, and Massive using temporary storage:

```text
Run ID: 26fc8ab8-34dc-5b8a-941b-6bb3e7435229
Status: SUCCESS
Yahoo rows: 8
Alpaca rows: 8
Massive rows: 8
MATCH: 24
WARN: 0
BREAK: 0
Open exceptions: 0
```

All providers returned the same eight market dates. The three provider pairs produced 24 comparisons. Their largest closing-price difference was approximately `$0.22`, but it remained below the `0.10%` warning band and was correctly classified as a legitimate cross-feed match. An immediate retry returned the same run ID without provider calls or duplicate writes.

![DuckDB audit query for the verified run](release_evidence/duckdb-audit.svg)

## Dashboard evidence

The Task 20 captures remain valid release evidence for the read-only control room:

- [Both batch and real-time panels available](dashboard_demos/both-available.svg)
- [Batch unavailable while real-time remains available](dashboard_demos/batch-unavailable.svg)
- [Real-time unavailable while batch remains available](dashboard_demos/realtime-unavailable.svg)

The full suite also passed two live in-process ZeroMQ integration tests: ten valid simulated ticks, and scorer republication of an explained anomaly.

## Environment limitations

- Docker could not be built or run on this host because the `docker` executable is not installed. The Dockerfile, Compose contract, persistent-volume configuration, and secret exclusion tests passed; exact runtime verification commands are published in `benchmarks/README.md`.
- The same limitation prevented actual Collector/Tempo/Prometheus/Grafana execution. In-memory span/metric integration and the full telemetry unit suite passed, but backend screenshots or queries require a Docker-capable host.
- Airflow was not exercised because no Task 18 DAG exists in the repository. It remains planned and is not described as runnable.
- Persisted option and futures batch controls were not exercised; their provider-aware reconciliation libraries are covered by offline tests.

