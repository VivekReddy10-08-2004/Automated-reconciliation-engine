"""Compare daily equity bars from Yahoo Finance and Alpaca.

The module validates both datasets, matches records by symbol and trade date,
compares closing prices and volumes, and labels each record as ``MATCH`` or
``BREAK`` based on the configured closing-price tolerance. When run directly,
it fetches recent AAPL data from both sources and prints a reconciliation
summary.
"""



from datetime import datetime, timedelta, timezone
import math
from alpaca.data.timeframe import TimeFrame
import pandas as pd
from reconciliation_engine.ingestion.yahoo_equities import (
    fetch_equity_bars as fetch_yahoo_bars,
)
from reconciliation_engine.ingestion.alpaca_equities import (
    fetch_stock_bars as fetch_alpaca_bars,
)
from reconciliation_engine.storage.duckdb import (
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
    warn_tolerance: float | None = None,
    break_pct_tolerance: float | None = None,
    warn_pct_tolerance: float | None = None,
) -> pd.DataFrame:
    """
    Reconcile equity bars from Yahoo Finance and Alpaca.

    ``close_tolerance`` remains the break threshold for backwards
    compatibility. When ``warn_tolerance`` is provided, differences above it
    and at or below ``close_tolerance`` are labelled ``WARN``. This optional
    mode is the Python reference for the dbt reconciliation fact model.
    """
    if close_tolerance < 0:
        raise ValueError("close_tolerance must be non-negative")
    if warn_tolerance is not None and not 0 <= warn_tolerance <= close_tolerance:
        raise ValueError(
            "warn_tolerance must be non-negative and no greater than close_tolerance"
        )
    for name, tolerance in (
        ("break_pct_tolerance", break_pct_tolerance),
        ("warn_pct_tolerance", warn_pct_tolerance),
    ):
        if tolerance is not None and tolerance < 0:
            raise ValueError(f"{name} must be non-negative")
    if (
        warn_pct_tolerance is not None
        and break_pct_tolerance is not None
        and warn_pct_tolerance > break_pct_tolerance
    ):
        raise ValueError(
            "warn_pct_tolerance must be no greater than break_pct_tolerance"
        )
    yahoo_bars = yahoo_bars.copy()
    alpaca_bars = alpaca_bars.copy()
    validate_equity_bars(yahoo_bars)
    validate_equity_bars(alpaca_bars)
    for frame in (yahoo_bars, alpaca_bars):
        frame["symbol"] = frame["symbol"].astype("string").str.strip().str.upper()
        frame["trade_date"] = pd.to_datetime(
            frame["trade_date"], utc=True
        ).dt.date

    # Preserve one-sided observations. Missing provider data is an actionable
    # control break, not a row that should silently disappear.
    merged = pd.merge(
        yahoo_bars,
        alpaca_bars,
        on=["symbol", "trade_date"],
        suffixes=("_yahoo", "_alpaca"),
        how="outer",
    )

    merged = merged.rename(columns={
        "close_yahoo": "yahoo_close",
        "close_alpaca": "alpaca_close",
        "volume_yahoo": "yahoo_volume",
        "volume_alpaca": "alpaca_volume",
    })

    
    merged["close_difference"] = merged["yahoo_close"] - merged["alpaca_close"]
    merged["volume_difference"] = merged["yahoo_volume"] - merged["alpaca_volume"]
    reference_price = merged[["yahoo_close", "alpaca_close"]].abs().max(axis=1)
    break_limit = pd.Series(close_tolerance, index=merged.index, dtype="float64")
    if break_pct_tolerance is not None:
        break_limit = pd.concat(
            [break_limit, reference_price * break_pct_tolerance], axis=1
        ).max(axis=1)
    merged["status"] = "MATCH"
    merged.loc[
        (merged["close_difference"].abs() > break_limit)
        | merged["close_difference"].isna(),
        "status",
    ] = "BREAK"
    if warn_tolerance is not None:
        warn_limit = pd.Series(warn_tolerance, index=merged.index, dtype="float64")
        if warn_pct_tolerance is not None:
            warn_limit = pd.concat(
                [warn_limit, reference_price * warn_pct_tolerance], axis=1
            ).max(axis=1)
        merged.loc[
            (merged["close_difference"].abs() > warn_limit)
            & (merged["close_difference"].abs() <= break_limit),
            "status",
        ] = "WARN"

    return merged[RECONCILIATION_COLUMNS]

def validate_equity_bars(df: pd.DataFrame) -> bool:
    """Validate canonical equity bars without mutating the caller's frame."""
    required_columns = [
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Equity data is missing required columns: {missing}")

    canonical = df[required_columns]
    if canonical.isna().any().any():
        null_columns = canonical.columns[canonical.isna().any()].tolist()
        raise ValueError(f"Equity data contains null values in: {null_columns}")

    symbols = canonical["symbol"].astype("string").str.strip()
    if (symbols == "").any():
        raise ValueError("Equity data contains an empty symbol")

    parsed_dates = pd.to_datetime(canonical["trade_date"], errors="coerce", utc=True)
    if parsed_dates.isna().any():
        raise ValueError("Equity data contains an invalid trade_date")

    numeric_columns = ["open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        if not pd.api.types.is_numeric_dtype(canonical[column]):
            raise ValueError(f"Equity column {column} must have a numeric dtype")
        if not canonical[column].map(lambda value: math.isfinite(float(value))).all():
            raise ValueError(f"Equity column {column} contains a non-finite value")

    prices = canonical[["open", "high", "low", "close"]]
    if (prices < 0).any().any():
        raise ValueError("Equity OHLC values must be non-negative")
    if (canonical["volume"] < 0).any():
        raise ValueError("Equity volume must be non-negative")
    if not canonical["volume"].map(lambda value: float(value).is_integer()).all():
        raise ValueError("Equity volume must contain whole numbers")

    if (
        (canonical["high"] < canonical[["open", "close", "low"]].max(axis=1))
        | (canonical["low"] > canonical[["open", "close", "high"]].min(axis=1))
    ).any():
        raise ValueError("Equity data violates OHLC high/low consistency")

    business_keys = pd.DataFrame(
        {
            "symbol": symbols.str.upper(),
            "trade_date": parsed_dates.dt.date,
        }
    )
    if business_keys.duplicated().any():
        raise ValueError("Equity data contains duplicate symbol/trade_date keys")
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
