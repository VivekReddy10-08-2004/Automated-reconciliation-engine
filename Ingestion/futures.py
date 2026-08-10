"""
Futures ingestion (Module A).

Alpaca does not provide futures market data on its free tier, so this module
sources continuous futures contracts from Yahoo Finance via ``yfinance``
(e.g. ``ES=F`` = E-mini S&P 500, ``CL=F`` = WTI Crude, ``GC=F`` = Gold).

Output is a normalized OHLCV DataFrame keyed by ``(symbol, trade_date)``,
aligned with the canonical futures schema in WORKFLOW_DESIGN.md so it can flow
straight into the same reconciliation engine as equities and options.
"""
from __future__ import annotations

import pandas as pd
import yfinance as yf

# Default futures universe: root contract -> Yahoo continuous-future symbol.
DEFAULT_FUTURES = {
    "ES": "ES=F",  # E-mini S&P 500
    "NQ": "NQ=F",  # E-mini Nasdaq 100
    "CL": "CL=F",  # WTI Crude Oil
    "GC": "GC=F",  # Gold
    "ZN": "ZN=F",  # 10-Year T-Note
}

CANONICAL_COLUMNS = ["symbol", "trade_date", "open", "high", "low", "close", "volume"]


def fetch_futures_bars(symbols=None, period: str = "5d", interval: str = "1h") -> pd.DataFrame:
    """Fetch OHLCV bars for futures contracts from Yahoo Finance.

    Args:
        symbols: Iterable of Yahoo futures symbols (e.g. ``["ES=F", "CL=F"]``).
            Defaults to the values of :data:`DEFAULT_FUTURES`.
        period: yfinance lookback window (e.g. ``"5d"``, ``"1mo"``).
        interval: Bar size (e.g. ``"1h"``, ``"1d"``).

    Returns:
        Long-format DataFrame with columns :data:`CANONICAL_COLUMNS`. Returns an
        empty (but correctly-typed) DataFrame when no data is available.
    """
    if symbols is None:
        symbols = list(DEFAULT_FUTURES.values())
    symbols = list(symbols)

    raw = yf.download(
        tickers=symbols,
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        progress=False,
    )

    if raw is None or raw.empty:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    frames = []
    for sym in symbols:
        # With multiple tickers yfinance returns a column MultiIndex (sym, field);
        # with a single ticker it can return a flat frame.
        if isinstance(raw.columns, pd.MultiIndex):
            if sym not in raw.columns.get_level_values(0):
                continue
            df = raw[sym].copy()
        else:
            df = raw.copy()

        df = df.dropna(how="all")
        if df.empty:
            continue

        df = df.reset_index()
        df.columns = [str(c).lower() for c in df.columns]
        # yfinance names the index "Datetime" for intraday and "Date" for daily bars.
        ts_col = "datetime" if "datetime" in df.columns else "date"
        df = df.rename(columns={ts_col: "trade_date"})
        df["symbol"] = sym
        frames.append(df.reindex(columns=CANONICAL_COLUMNS))

    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    return pd.concat(frames, ignore_index=True)


if __name__ == "__main__":
    bars = fetch_futures_bars()
    print(bars)
