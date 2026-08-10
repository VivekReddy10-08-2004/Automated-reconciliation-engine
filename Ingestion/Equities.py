import os 
from dotenv import load_dotenv 
from datetime import datetime, timedelta, timezone
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
import pandas as pd

EQUITY_COLUMNS = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
]

load_dotenv("project.env")

api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_SECRET_KEY")

#Notes:
""" Check in yf file if the period and interval are native to yfinance. 
If not, we need to convert them to the appropriate TimeFrame and TimeFrameUnit for Alpaca.
"""
# Initializing the client
client = StockHistoricalDataClient(api_key, secret_key)

def fetch_stock_bars(symbol: str, start_date: datetime, end_date: datetime, timeframe: TimeFrame) -> pd.DataFrame:
    """
    Fetch historical stock bars for a given symbol and time range.
    """
    request_params = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=timeframe,
        start=start_date,
        end=end_date
    )

    stock_bars = client.get_stock_bars(request_params)
    bars = stock_bars.df

    if bars.empty:
        return pd.DataFrame(columns=EQUITY_COLUMNS)

    bars = bars.reset_index().rename(columns={"timestamp": "trade_date"})
    bars["trade_date"] = pd.to_datetime(bars["trade_date"]).dt.date
    return bars[EQUITY_COLUMNS]



def validate_columns(df: pd.DataFrame, expected_columns: list[str]) -> bool:
    """
    Validate that the DataFrame has the expected columns.
    """
    return list(df.columns) == expected_columns

if __name__ == "__main__":
    now = datetime.now(timezone.utc)
    equity_bars = fetch_stock_bars("AAPL", now - timedelta(days=5), now, TimeFrame.Day)
    print(equity_bars.head())
    print(validate_columns(equity_bars, EQUITY_COLUMNS))
