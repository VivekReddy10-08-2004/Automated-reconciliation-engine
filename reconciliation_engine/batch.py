import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd

from alpaca.data.timeframe import TimeFrame

from reconciliation_engine.storage.data_lake import (
    EQUITY_COLUMNS,
    EQUITY_INSTRUMENT,
    archive_equity_source,
    archive_reconciliation_results,
    read_dataframe,
)
from reconciliation_engine.reconciliation.exceptions import create_equity_exceptions
from reconciliation_engine.ingestion.alpaca_equities import fetch_stock_bars
from reconciliation_engine.ingestion.yahoo_equities import (
    fetch_equity_bars as fetch_yahoo_bars,
)
from reconciliation_engine.reconciliation.equities import reconcile_equity_bars
from reconciliation_engine.notifications import send_run_notification
from reconciliation_engine.storage.duckdb import (
    initialize_database,
    save_equity_bars,
    save_exceptions,
    save_pipeline_run,
    save_reconciliation_results,
)


def fetch_equity_sources() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch the two provider datasets used by the daily equity control."""
    start = datetime.now(timezone.utc) - timedelta(days=5)
    end = datetime.now(timezone.utc)

    yahoo_bars = fetch_yahoo_bars("AAPL", period="5d", interval="1d")
    alpaca_bars = fetch_stock_bars("AAPL", start, end, TimeFrame.Day)
    return yahoo_bars, alpaca_bars


def reconciliation() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch and reconcile data without persistence (kept for library callers)."""
    yahoo_bars, alpaca_bars = fetch_equity_sources()

    reconciliation_df = reconcile_equity_bars(yahoo_bars, alpaca_bars)
    return yahoo_bars, alpaca_bars, reconciliation_df


def replay_archived_run(
    run_id: str,
    data_root: str | Path = "data",
) -> pd.DataFrame:
    """Recompute reconciliation from saved Parquet inputs without API calls."""
    yahoo_bars = read_dataframe(
        "YAHOO",
        EQUITY_INSTRUMENT,
        run_id,
        layer="normalized",
        data_root=data_root,
    )
    alpaca_bars = read_dataframe(
        "ALPACA",
        EQUITY_INSTRUMENT,
        run_id,
        layer="normalized",
        data_root=data_root,
    )

    if yahoo_bars.empty or alpaca_bars.empty:
        raise FileNotFoundError(
            f"Normalized Yahoo and Alpaca archives are required for run {run_id}"
        )

    return reconcile_equity_bars(
        yahoo_bars[EQUITY_COLUMNS],
        alpaca_bars[EQUITY_COLUMNS],
    )


def main(
    db_path: str = "reconciliation.duckdb",
    data_root: str | Path = "data",
) -> dict:
    run_id = str(uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    break_count = 0

    try:
        initialize_database(db_path)
        yahoo_raw, alpaca_raw = fetch_equity_sources()

        yahoo_bars = archive_equity_source(
            yahoo_raw,
            "YAHOO",
            run_id,
            data_root=data_root,
        )
        alpaca_bars = archive_equity_source(
            alpaca_raw,
            "ALPACA",
            run_id,
            data_root=data_root,
        )

        reconciliation_df = reconcile_equity_bars(yahoo_bars, alpaca_bars)
        archive_reconciliation_results(
            reconciliation_df,
            run_id,
            data_root=data_root,
        )
        match_count = len(reconciliation_df[reconciliation_df["status"] == "MATCH"])
        break_count = len(reconciliation_df[reconciliation_df["status"] == "BREAK"])
        finished_at = datetime.now(timezone.utc).isoformat()
        run_status = "SUCCESS" if break_count == 0 else "SUCCESS_WITH_BREAKS"
        largest_difference = reconciliation_df["close_difference"].abs().max()

        save_pipeline_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=run_status,
            yahoo_row_count=len(yahoo_bars),
            alpaca_row_count=len(alpaca_bars),
            match_count=match_count,
            break_count=break_count,
            db_path=db_path,
        )
        save_equity_bars(
            yahoo_bars,
            source="YAHOO",
            run_id=run_id,
            db_path=db_path,
        )
        save_equity_bars(
            alpaca_bars,
            source="ALPACA",
            run_id=run_id,
            db_path=db_path,
        )
        save_reconciliation_results(
            reconciliation_df,
            run_id=run_id,
            db_path=db_path,
        )

        exceptions_df = create_equity_exceptions(reconciliation_df, run_id)
        save_exceptions(exceptions_df, db_path=db_path)

        summary = {
            "run_id": run_id,
            "status": run_status,
            "break_count": break_count,
        }

        print(
            f"Run ID: {run_id} | "
            f"Yahoo rows: {len(yahoo_bars)} | "
            f"Alpaca rows: {len(alpaca_bars)} | "
            f"MATCH: {match_count} | "
            f"BREAK: {break_count} | "
            f"Largest difference: {largest_difference}"
        )
        print("Yahoo dates:")
        print(yahoo_bars["trade_date"].tolist())
        print("Alpaca dates:")
        print(alpaca_bars["trade_date"].tolist())
        print(reconciliation_df)
    except Exception:
        failed_summary = {
            "run_id": run_id,
            "status": "FAILED",
            "break_count": break_count,
        }
        send_run_notification(failed_summary)
        raise

    send_run_notification(summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the live equity control or replay archived provider data."
    )
    parser.add_argument("--db", default="reconciliation.duckdb")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--replay-run-id",
        help="Recompute a report from normalized Parquet files without API calls",
    )
    arguments = parser.parse_args()

    if arguments.replay_run_id:
        replay = replay_archived_run(arguments.replay_run_id, arguments.data_root)
        print(replay)
        print(
            f"Replayed run {arguments.replay_run_id} | "
            f"MATCH: {(replay['status'] == 'MATCH').sum()} | "
            f"BREAK: {(replay['status'] == 'BREAK').sum()}"
        )
    else:
        main(arguments.db, arguments.data_root)
