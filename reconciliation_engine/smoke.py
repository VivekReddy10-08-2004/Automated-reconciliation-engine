"""Offline release-readiness checks for the reconciliation engine."""

from __future__ import annotations

import argparse
import importlib
import os
from pathlib import Path
import tempfile

import duckdb
from dotenv import load_dotenv

from reconciliation_engine.batch import (
    DEFAULT_RECONCILIATION_SYMBOLS,
    SYMBOLS_ENV,
    normalize_symbols,
)
from reconciliation_engine.realtime import (
    DEFAULT_ENDPOINT,
    DEFAULT_SCORED_ENDPOINT,
    ENDPOINT_ENV,
    SCORED_ENDPOINT_ENV,
    validate_local_endpoint,
)
from reconciliation_engine.security import load_alpaca_credentials, load_massive_api_key
from reconciliation_engine.storage.duckdb import initialize_database


REQUIRED_MODULES = (
    "reconciliation_engine.batch",
    "reconciliation_engine.dashboard",
    "reconciliation_engine.notifications",
    "reconciliation_engine.ingestion.alpaca_equities",
    "reconciliation_engine.ingestion.alpaca_options",
    "reconciliation_engine.ingestion.massive",
    "reconciliation_engine.ingestion.massive_equities",
    "reconciliation_engine.ingestion.massive_options",
    "reconciliation_engine.ingestion.massive_futures",
    "reconciliation_engine.ingestion.providers",
    "reconciliation_engine.ingestion.yahoo_equities",
    "reconciliation_engine.ingestion.yahoo_options",
    "reconciliation_engine.reconciliation.equities",
    "reconciliation_engine.reconciliation.options",
    "reconciliation_engine.reconciliation.futures",
    "reconciliation_engine.realtime.publisher",
    "reconciliation_engine.realtime.anomaly_scorer",
    "reconciliation_engine.observability",
    "reconciliation_engine.scaling.partitions",
    "reconciliation_engine.scaling.equity_ingestion",
    "reconciliation_engine.storage.data_lake",
    "reconciliation_engine.storage.partitioned_data_lake",
    "reconciliation_engine.storage.retention",
    "reconciliation_engine.storage.duckdb",
)

REQUIRED_TABLES = {
    "pipeline_runs",
    "equity_bars_staging",
    "equity_reconciliation_results",
    "equity_exceptions",
    "work_partitions",
    "provider_request_metrics",
    "quarantine_records",
}


def check_imports() -> tuple[str, ...]:
    """Import every runnable release component without calling a provider."""
    for module_name in REQUIRED_MODULES:
        importlib.import_module(module_name)
    return REQUIRED_MODULES


def check_configuration(env_path: str | Path = "project.env") -> dict[str, object]:
    """Validate secrets, symbols, and local endpoints without revealing values."""
    env_path = Path(env_path)
    api_key, secret_key = load_alpaca_credentials(env_path)
    if api_key.lower().startswith("your_") or secret_key.lower().startswith("your_"):
        raise RuntimeError("Alpaca credentials still contain example placeholders")
    massive_api_key = load_massive_api_key(env_path)
    if massive_api_key.lower().startswith("your_"):
        raise RuntimeError("Massive credentials still contain example placeholders")

    load_dotenv(env_path, override=False)
    symbol_value = os.getenv(SYMBOLS_ENV)
    symbols = normalize_symbols(
        [symbol_value] if symbol_value is not None else DEFAULT_RECONCILIATION_SYMBOLS
    )
    raw_endpoint = os.getenv(ENDPOINT_ENV, DEFAULT_ENDPOINT).strip()
    scored_endpoint = os.getenv(SCORED_ENDPOINT_ENV, DEFAULT_SCORED_ENDPOINT).strip()
    validate_local_endpoint(raw_endpoint)
    validate_local_endpoint(scored_endpoint)
    if raw_endpoint == scored_endpoint:
        raise ValueError("Raw and scored ZeroMQ endpoints must be different")

    return {
        "credentials_configured": True,
        "massive_credentials_configured": True,
        "symbols": symbols,
        "raw_endpoint": raw_endpoint,
        "scored_endpoint": scored_endpoint,
    }


def check_database(db_path: str | Path | None = None) -> tuple[str, ...]:
    """Initialize and query the DuckDB schema without inserting market data."""

    def initialize_and_inspect(path: Path) -> tuple[str, ...]:
        initialize_database(str(path))
        with duckdb.connect(str(path), read_only=True) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                    """
                ).fetchall()
            }
            connection.execute("SELECT count(*) FROM pipeline_runs").fetchone()
        missing = REQUIRED_TABLES - tables
        if missing:
            raise RuntimeError(f"DuckDB schema is missing tables: {sorted(missing)}")
        return tuple(sorted(REQUIRED_TABLES))

    if db_path is not None:
        return initialize_and_inspect(Path(db_path))

    with tempfile.TemporaryDirectory(prefix="reconciliation-smoke-") as directory:
        return initialize_and_inspect(Path(directory) / "smoke.duckdb")


def run_smoke_checks(
    *,
    env_path: str | Path = "project.env",
    db_path: str | Path | None = None,
) -> dict[str, object]:
    """Run all non-network release checks and return non-sensitive details."""
    modules = check_imports()
    configuration = check_configuration(env_path)
    tables = check_database(db_path)
    return {
        "module_count": len(modules),
        "symbols": configuration["symbols"],
        "raw_endpoint": configuration["raw_endpoint"],
        "scored_endpoint": configuration["scored_endpoint"],
        "tables": tables,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path("project.env"),
        help="Private runtime environment file (default: project.env)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=None,
        help="Optional DuckDB path to initialize and inspect; defaults to a temporary probe",
    )
    arguments = parser.parse_args()

    try:
        result = run_smoke_checks(env_path=arguments.env_file, db_path=arguments.db)
    except Exception as error:
        print(f"[FAIL] {type(error).__name__}: {error}")
        return 1

    print(f"[PASS] imports: {result['module_count']} application modules")
    print(f"[PASS] configuration: {','.join(result['symbols'])}")
    print("[PASS] credentials: configured (values not displayed)")
    print(
        "[PASS] ZeroMQ endpoints: "
        f"{result['raw_endpoint']} -> {result['scored_endpoint']}"
    )
    print(f"[PASS] DuckDB schema: {len(result['tables'])} required tables")
    print("Smoke checks completed without fetching market data.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
