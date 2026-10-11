# Codebase Organization

Keep Python application code inside `reconciliation_engine/` and extend the
existing domain package instead of adding new root-level modules.

| Responsibility | Location |
| --- | --- |
| Provider ingestion and normalization | `reconciliation_engine/ingestion/` |
| Reconciliation rules and exceptions | `reconciliation_engine/reconciliation/` |
| DuckDB and Parquet access | `reconciliation_engine/storage/` |
| ZeroMQ and live consumers | `reconciliation_engine/realtime/` |
| Batch orchestration | `reconciliation_engine/batch.py` |
| Read-only operator dashboard | `reconciliation_engine/dashboard.py` |
| dbt transformations | `dbt/` |
| Docker deployment | `orchestration/docker/` |
| Future Airflow definitions | `orchestration/airflow/` |
| Automated checks | `tests/` |

Use lowercase `snake_case` module names, run application entry points with
`python -m`, and mirror modules with focused tests. Tool-required root files,
such as `.dockerignore`, are allowed; feature implementation does not belong at
the repository root. Databases, Parquet files, logs, virtual environments,
secrets, caches, and generated dbt artifacts must remain outside source control.
