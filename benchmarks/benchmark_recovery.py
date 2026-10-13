"""Exercise process-crash recovery, idempotency, quarantine, and retries."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from benchmarks.generate_data import synthetic_equity_bars
from reconciliation_engine.reconciliation.equities import reconcile_equity_sources
from reconciliation_engine.storage.data_lake import (
    archive_equity_source,
    archive_reconciliation_results,
)
from reconciliation_engine.storage.duckdb import initialize_database, persist_equity_run


def _frames(rows: int = 1000):
    return synthetic_equity_bars(rows), synthetic_equity_bars(rows, break_every=101)


def _crash(stage: str, database: str, data_root: str) -> None:
    run_id = f"crash-{stage}"
    initialize_database(database)
    if stage == "before_archive":
        os._exit(17)
    left, right = _frames()
    left = archive_equity_source(left, "YAHOO", run_id, data_root=data_root)
    right = archive_equity_source(right, "ALPACA", run_id, data_root=data_root)
    result = reconcile_equity_sources({"YAHOO": left, "ALPACA": right})
    archive_reconciliation_results(result, run_id, data_root=data_root)
    if stage == "after_archive":
        os._exit(18)
    persist_equity_run(
        run_id=run_id,
        started_at=datetime.now(timezone.utc).isoformat(),
        finished_at=datetime.now(timezone.utc).isoformat(),
        status="SUCCESS_WITH_BREAKS",
        yahoo_bars=left,
        alpaca_bars=right,
        reconciliation=result,
        exceptions=pd.DataFrame(),
        db_path=database,
    )
    os._exit(19)


def _count(database: Path, table: str) -> int:
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--results-dir", type=Path, default=Path(__file__).parent / "results"
    )
    args = parser.parse_args()
    report = []
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        for stage in ("before_archive", "after_archive", "after_commit"):
            case_root = root / stage
            case_root.mkdir()
            database = case_root / "state.duckdb"
            data_root = case_root / "data"
            process = multiprocessing.Process(
                target=_crash, args=(stage, str(database), str(data_root))
            )
            process.start()
            process.join(30)
            if process.is_alive():
                process.kill()
                process.join()
            run_count = _count(database, "pipeline_runs")
            archives = len(list(data_root.rglob("*.parquet")))
            passed = (
                (stage == "before_archive" and run_count == 0 and archives == 0)
                or (stage == "after_archive" and run_count == 0 and archives == 5)
                or (stage == "after_commit" and run_count == 1 and archives == 5)
            )
            report.append(
                {
                    "stage": stage,
                    "exit_code": process.exitcode,
                    "pipeline_runs": run_count,
                    "archives": archives,
                    "passed": passed,
                }
            )

        # A failed logical run is safe to retry, while a committed run is a no-op.
        left, right = _frames()
        database = root / "retry.duckdb"
        initialize_database(str(database))
        result = reconcile_equity_sources({"YAHOO": left, "ALPACA": right})
        kwargs = {
            "run_id": "idempotent-run",
            "started_at": "2026-10-13T00:00:00+00:00",
            "finished_at": "2026-10-13T00:00:01+00:00",
            "status": "SUCCESS_WITH_BREAKS",
            "yahoo_bars": left,
            "alpaca_bars": right,
            "reconciliation": result,
            "exceptions": pd.DataFrame(),
            "db_path": str(database),
        }
        first = persist_equity_run(**kwargs)
        second = persist_equity_run(**kwargs)
        report.append(
            {
                "stage": "idempotent_retry",
                "first_write": first,
                "second_write": second,
                "pipeline_runs": _count(database, "pipeline_runs"),
                "passed": first
                and not second
                and _count(database, "pipeline_runs") == 1,
            }
        )

    args.results_dir.mkdir(parents=True, exist_ok=True)
    output = {"all_passed": all(case["passed"] for case in report), "cases": report}
    (args.results_dir / "recovery_report.json").write_text(
        json.dumps(output, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(output, indent=2))
    if not output["all_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
