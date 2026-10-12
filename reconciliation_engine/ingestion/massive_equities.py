"""Massive equity aggregates normalized to the canonical equity schema."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from reconciliation_engine.ingestion.alpaca_equities import EQUITY_COLUMNS
from reconciliation_engine.ingestion.massive import MassiveClient


def _date_text(value: date | datetime | str) -> str:
    return value.date().isoformat() if isinstance(value, datetime) else str(value)


def _normalize(rows: list[dict], fallback_symbol: str | None = None) -> pd.DataFrame:
    normalized = pd.DataFrame(
        {
            "symbol": [row.get("T") or row.get("ticker") or fallback_symbol for row in rows],
            "trade_date": [pd.to_datetime(row.get("t"), unit="ms", utc=True).date() for row in rows],
            "open": [row.get("o") for row in rows],
            "high": [row.get("h") for row in rows],
            "low": [row.get("l") for row in rows],
            "close": [row.get("c") for row in rows],
            "volume": [row.get("v") for row in rows],
        }
    )
    return normalized.reindex(columns=EQUITY_COLUMNS)


def fetch_massive_equity_bars(
    symbol: str,
    start: date | datetime | str,
    end: date | datetime | str,
    *,
    adjusted: bool = False,
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Fetch daily bars for one stock; unadjusted by default for alignment."""
    massive = client or MassiveClient()
    payload = massive.get_json(
        f"/v2/aggs/ticker/{symbol.upper()}/range/1/day/{_date_text(start)}/{_date_text(end)}",
        {"adjusted": str(adjusted).lower(), "sort": "asc", "limit": 50000},
    )
    return _normalize(list(payload.get("results", [])), symbol.upper())


def fetch_massive_grouped_daily(
    trade_date: date | str,
    *,
    adjusted: bool = False,
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Fetch the complete US equity EOD market in a single API call."""
    massive = client or MassiveClient()
    payload = massive.get_json(
        f"/v2/aggs/grouped/locale/us/market/stocks/{trade_date}",
        {"adjusted": str(adjusted).lower(), "include_otc": "true"},
    )
    return _normalize(list(payload.get("results", [])))
