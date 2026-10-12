"""Massive futures contract discovery and session/minute aggregates."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from reconciliation_engine.ingestion.massive import MassiveClient


FUTURES_COLUMNS = [
    "symbol", "session_date", "window_start", "open", "high", "low", "close",
    "settlement_price", "volume", "transactions",
]


def fetch_massive_futures_contracts(
    *,
    product_code: str | None = None,
    active: bool | None = True,
    as_of: date | str | None = None,
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Discover futures contracts without conflating them with continuous roots."""
    massive = client or MassiveClient()
    params: dict[str, object] = {"limit": 1000, "sort": "product_code.asc"}
    if product_code:
        params["product_code"] = product_code.upper()
    if active is not None:
        params["active"] = str(active).lower()
    if as_of is not None:
        params["as_of"] = str(as_of)
    return pd.DataFrame(list(massive.iter_results("/futures/v1/contracts", params)))


def fetch_massive_futures_bars(
    contract_symbol: str,
    start: date | datetime | str,
    end: date | datetime | str,
    *,
    resolution: str = "1session",
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Fetch bars for one expiring futures contract and preserve session dates."""
    massive = client or MassiveClient()
    start_text = start.date().isoformat() if isinstance(start, datetime) else str(start)
    end_text = end.date().isoformat() if isinstance(end, datetime) else str(end)
    rows = list(
        massive.iter_results(
            f"/futures/v1/aggs/{contract_symbol.upper()}",
            {
                "resolution": resolution,
                "window_start.gte": start_text,
                "window_start.lte": end_text,
                "sort": "window_start.asc",
                "limit": 50000,
            },
        )
    )
    frame = pd.DataFrame(
        {
            "symbol": [row.get("ticker", contract_symbol.upper()) for row in rows],
            "session_date": [row.get("session_end_date") for row in rows],
            "window_start": [pd.to_datetime(row.get("window_start"), unit="ns", utc=True) for row in rows],
            "open": [row.get("open") for row in rows],
            "high": [row.get("high") for row in rows],
            "low": [row.get("low") for row in rows],
            "close": [row.get("close") for row in rows],
            "settlement_price": [row.get("settlement_price") for row in rows],
            "volume": [row.get("volume") for row in rows],
            "transactions": [row.get("transactions") for row in rows],
        }
    )
    return frame.reindex(columns=FUTURES_COLUMNS)
