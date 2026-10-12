"""Massive option reference and aggregate ingestion."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from reconciliation_engine.ingestion.massive import MassiveClient
from reconciliation_engine.ingestion.yahoo_options import options_columns


CONTRACT_COLUMNS = [
    "contract_symbol", "underlying", "expiry", "strike", "option_type",
    "exercise_style", "shares_per_contract", "primary_exchange",
]


def fetch_massive_option_contracts(
    underlying: str,
    *,
    expiration: date | str | None = None,
    as_of: date | str | None = None,
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Discover option contracts with cursor-safe pagination."""
    massive = client or MassiveClient()
    params: dict[str, object] = {
        "underlying_ticker": underlying.upper(),
        "limit": 1000,
        "sort": "ticker",
        "order": "asc",
    }
    if expiration is not None:
        params["expiration_date"] = str(expiration)
    if as_of is not None:
        params["as_of"] = str(as_of)
    rows = list(massive.iter_results("/v3/reference/options/contracts", params))
    frame = pd.DataFrame(
        {
            "contract_symbol": [row.get("ticker") for row in rows],
            "underlying": [row.get("underlying_ticker") for row in rows],
            "expiry": [row.get("expiration_date") for row in rows],
            "strike": [row.get("strike_price") for row in rows],
            "option_type": [str(row.get("contract_type", ""))[:1].upper() for row in rows],
            "exercise_style": [row.get("exercise_style") for row in rows],
            "shares_per_contract": [row.get("shares_per_contract") for row in rows],
            "primary_exchange": [row.get("primary_exchange") for row in rows],
        }
    )
    return frame.reindex(columns=CONTRACT_COLUMNS)


def fetch_massive_option_bars(
    contract: dict | pd.Series,
    start: date | datetime | str,
    end: date | datetime | str,
    *,
    client: MassiveClient | None = None,
) -> pd.DataFrame:
    """Fetch EOD trade aggregates for one explicitly selected contract."""
    massive = client or MassiveClient()
    symbol = str(contract["contract_symbol"])
    start_text = start.date().isoformat() if isinstance(start, datetime) else str(start)
    end_text = end.date().isoformat() if isinstance(end, datetime) else str(end)
    payload = massive.get_json(
        f"/v2/aggs/ticker/{symbol}/range/1/day/{start_text}/{end_text}",
        {"adjusted": "false", "sort": "asc", "limit": 50000},
    )
    rows = list(payload.get("results", []))
    frame = pd.DataFrame(
        {
            "underlying": [contract["underlying"]] * len(rows),
            "expiry": [contract["expiry"]] * len(rows),
            "strike": [contract["strike"]] * len(rows),
            "option_type": [contract["option_type"]] * len(rows),
            "contract_symbol": [symbol] * len(rows),
            "trade_date": [pd.to_datetime(row.get("t"), unit="ms", utc=True).date() for row in rows],
            "close": [row.get("c") for row in rows],
            "bid": [pd.NA] * len(rows),
            "ask": [pd.NA] * len(rows),
            "volume": [row.get("v") for row in rows],
            "open_interest": [pd.NA] * len(rows),
        }
    )
    return frame.reindex(columns=options_columns)
