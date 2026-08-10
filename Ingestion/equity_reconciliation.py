from datetime import datetime, timedelta, timezone
from alpaca.data.timeframe import TimeFrame
import pandas as pd
from Ingestion.Equities_yf import fetch_equity_bars as fetch_yahoo_bars
from Ingestion.Equities import fetch_stock_bars as fetch_alpaca_bars

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

if __name__ == "__main__":
    yahoo_bars = fetch_yahoo_bars("AAPL", period="5d", interval="1d")
    now = datetime.now(timezone.utc)
    alpaca_bars = fetch_alpaca_bars("AAPL", now - timedelta(days=5), now, TimeFrame.Day)
    reconciliation = reconcile_equity_bars(yahoo_bars, alpaca_bars)
    print(reconciliation.head())
    print(f"Breaks: {(reconciliation['status'] == 'BREAK').sum()}")
