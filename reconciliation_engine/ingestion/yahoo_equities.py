"""Fetch and normalize Yahoo Finance equity bars."""

import pandas as pd
import yfinance as yf
from datetime import datetime

#The output columns we wanted. 
EQUITY_COLUMNS = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

#ingestion
def fetch_equity_bars(
    symbol: str,
    period: str = "5d",
    interval: str = "1h",
    start: datetime | None = None,
    end: datetime | None = None,
) -> pd.DataFrame:
    """Fetch and normalize historical equity bars from Yahoo Finance."""
    # Alpaca returns unadjusted exchange bars. Request the same price basis from
    # Yahoo so corporate-action adjustment policy does not create artificial
    # reconciliation differences.
    request = {
        "interval": interval,
        "auto_adjust": False,
        "actions": False,
    }
    if start is None and end is None:
        request["period"] = period
    else:
        request["start"] = start
        request["end"] = end
    bars = yf.Ticker(symbol).history(**request)

    if bars.empty:
        return pd.DataFrame(columns=EQUITY_COLUMNS)
    
    #normalize the DataFrame to match the expected output format
    bars = bars.reset_index()
    bars.columns = [str(column).lower() for column in bars.columns]
    bars = bars.rename(columns={bars.columns[0]: "trade_date"})
    bars["symbol"] = symbol

    return bars[EQUITY_COLUMNS]

def validate_columns(df: pd.DataFrame, expected_columns: list[str]) -> bool:
    """
    Validate that the DataFrame has the expected columns.
    """
    return list(df.columns) == expected_columns


if __name__ == "__main__":
    equity_bars = fetch_equity_bars("AAPL")
    print(equity_bars.head())
    print(validate_columns(equity_bars, EQUITY_COLUMNS))
