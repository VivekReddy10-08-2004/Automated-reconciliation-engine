from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pandas as pd

from alpaca.data.timeframe import TimeFrame

from exception import create_equity_exceptions
from Ingestion.Equities import fetch_stock_bars
from Ingestion.Equities_yf import fetch_equity_bars as fetch_yahoo_bars
from Ingestion.equity_reconciliation import reconcile_equity_bars
from Storage.storage import (
    initialize_database,
    save_equity_bars,
    save_exceptions,
    save_pipeline_run,
    save_reconciliation_results,
)
def reconciliation() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    start = datetime.now(timezone.utc) - timedelta(days=5)
    end = datetime.now(timezone.utc)

    yahoo_bars = fetch_yahoo_bars("AAPL", period="5d", interval="1d")
    alpaca_bars = fetch_stock_bars("AAPL", start, end, TimeFrame.Day)

    reconciliation = reconcile_equity_bars(yahoo_bars, alpaca_bars)
    return yahoo_bars, alpaca_bars, reconciliation



if __name__ == "__main__":
    run_id = str(uuid4())
    db_path = "reconciliation.duckdb"
    started_at = datetime.now(timezone.utc).isoformat()
    initialize_database(db_path)
    yahoo_bars, alpaca_bars, reconciliation_df = reconciliation()
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
    





