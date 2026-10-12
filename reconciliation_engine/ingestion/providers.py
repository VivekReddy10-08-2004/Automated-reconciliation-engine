"""Canonical provider interfaces independent of vendor response formats."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from alpaca.data.timeframe import TimeFrame
import pandas as pd

from reconciliation_engine.ingestion.alpaca_equities import fetch_stock_bars
from reconciliation_engine.ingestion.massive import MassiveClient
from reconciliation_engine.ingestion.massive_equities import fetch_massive_equity_bars
from reconciliation_engine.ingestion.yahoo_equities import fetch_equity_bars


class EquityProviderAdapter(Protocol):
    name: str

    def fetch(
        self, symbols: tuple[str, ...], start: datetime, end: datetime
    ) -> pd.DataFrame: ...


@dataclass(frozen=True)
class FunctionEquityAdapter:
    """Adapter around a canonical per-symbol fetch function."""

    name: str
    fetch_symbol: Callable[[str, datetime, datetime], pd.DataFrame]

    def fetch(
        self, symbols: tuple[str, ...], start: datetime, end: datetime
    ) -> pd.DataFrame:
        frames = [self.fetch_symbol(symbol, start, end) for symbol in symbols]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


def default_equity_adapters() -> dict[str, EquityProviderAdapter]:
    """Return the configured providers behind one canonical fetch contract."""
    massive_client = MassiveClient()
    return {
        "YAHOO": FunctionEquityAdapter(
            "YAHOO",
            lambda symbol, start, end: fetch_equity_bars(
                symbol, start=start, end=end, interval="1d"
            ),
        ),
        "ALPACA": FunctionEquityAdapter(
            "ALPACA",
            lambda symbol, start, end: fetch_stock_bars(
                symbol, start, end, TimeFrame.Day
            ),
        ),
        "MASSIVE": FunctionEquityAdapter(
            "MASSIVE",
            lambda symbol, start, end: fetch_massive_equity_bars(
                symbol, start, end, adjusted=False, client=massive_client
            ),
        ),
    }
