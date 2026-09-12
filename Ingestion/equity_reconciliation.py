"""Compare daily equity bars from Yahoo Finance and Alpaca.

The module validates both datasets, matches records by symbol and trade date,
compares closing prices and volumes, and labels each record as ``MATCH`` or
``BREAK`` based on the configured closing-price tolerance. When run directly,
it fetches recent AAPL data from both sources and prints a reconciliation
summary.
"""



from datetime import datetime, timedelta, timezone
from alpaca.data.timeframe import TimeFrame
import pandas as pd
from Ingestion.Equities_yf import fetch_equity_bars as fetch_yahoo_bars
from Ingestion.Equities import fetch_stock_bars as fetch_alpaca_bars
from Storage.storage import (
    initialize_database, 
    save_equity_bars, 
    save_pipeline_run,
    save_reconciliation_results,
    
)
from uuid import uuid4


RECONCILIATION_COLUMNS = [
    "symbol",
    "trade_date",
    "yahoo_close",
    "alpaca_close",
    "close_difference",
    "yahoo_volume",
    "alpaca_volume",
    "volume_difference",
    "status",
]



def reconcile_equity_bars(
    yahoo_bars: pd.DataFrame,
    alpaca_bars: pd.DataFrame,
    close_tolerance: float = 0.01,
) -> pd.DataFrame:
    """
    Reconcile equity bars from Yahoo Finance and Alpaca.
    """
    yahoo_bars = yahoo_bars.copy()
    alpaca_bars = alpaca_bars.copy()
    validate_equity_bars(yahoo_bars)
    validate_equity_bars(alpaca_bars)
    yahoo_bars["trade_date"] = pd.to_datetime(yahoo_bars["trade_date"]).dt.date
    alpaca_bars["trade_date"] = pd.to_datetime(alpaca_bars["trade_date"]).dt.date

    # Merge the two DataFrames on trade_date
    merged = pd.merge(
        yahoo_bars,
        alpaca_bars,
        on=["symbol", "trade_date"],
        suffixes=("_yahoo", "_alpaca"),
        how="inner"
    )

    merged = merged.rename(columns={
        "close_yahoo": "yahoo_close",
        "close_alpaca": "alpaca_close",
        "volume_yahoo": "yahoo_volume",
        "volume_alpaca": "alpaca_volume",
    })
    merged["close_difference"] = merged["yahoo_close"] - merged["alpaca_close"]
    merged["volume_difference"] = merged["yahoo_volume"] - merged["alpaca_volume"]
    merged["status"] = "MATCH"
    merged.loc[merged["close_difference"].abs() > close_tolerance, "status"] = "BREAK"

    return merged[RECONCILIATION_COLUMNS]

def validate_equity_bars(df: pd.DataFrame) -> bool:
    required_columns = ["symbol", "trade_date", "open", "high", "low", "close", "volume"]
    if list(df.columns) != required_columns:
        raise ValueError(f"DataFrame must contain the following columns: {required_columns}")    
    return True


""" Flow: 
initialize database
        ↓
fetch Yahoo data
        ↓
fetch Alpaca data
        ↓
save Yahoo raw bars
        ↓
save Alpaca raw bars
        ↓
reconcile both datasets
        ↓
save reconciliation results
        ↓
print summary

"""

if __name__ == "__main__":
    run_id = str(uuid4())
    db_path = "reconciliation.duckdb"
    started_at = datetime.now(timezone.utc).isoformat()

    initialize_database(db_path)

    yahoo_bars = fetch_yahoo_bars(
        "AAPL",
        period="5d",
        interval="1d",
    )

    now = datetime.now(timezone.utc)

    alpaca_bars = fetch_alpaca_bars(
        "AAPL",
        now - timedelta(days=5),
        now,
        TimeFrame.Day,
    )

    reconciliation = reconcile_equity_bars(
        yahoo_bars,
        alpaca_bars,
    )
    break_count = int((reconciliation["status"] == "BREAK").sum())
    match_count = int((reconciliation["status"] == "MATCH").sum())
    status = "SUCCESS" if break_count == 0 else "SUCCESS_WITH_BREAKS"
    finished_at = datetime.now(timezone.utc).isoformat()
    save_pipeline_run(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
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
        reconciliation,
        run_id=run_id,
        db_path=db_path,
    )

    print(reconciliation.head())
    print(f"Run ID: {run_id}")
    print(f"Breaks: {break_count}")