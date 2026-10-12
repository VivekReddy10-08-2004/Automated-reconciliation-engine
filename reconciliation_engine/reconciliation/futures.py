"""Reconcile Yahoo continuous futures with mapped Massive contracts."""

from __future__ import annotations

import math
import pandas as pd


OUTPUT_COLUMNS = [
    "yahoo_symbol", "massive_contract", "session_date", "yahoo_close",
    "massive_close", "massive_settlement_price", "close_difference", "status",
    "break_reason",
]


def _validate_prices(frame: pd.DataFrame, columns: list[str], provider: str) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"{provider} futures data is missing columns: {missing}")
    for column in ("close",):
        numeric = pd.to_numeric(frame[column], errors="coerce")
        if numeric.notna().any() and not numeric.dropna().map(
            lambda value: math.isfinite(float(value)) and float(value) >= 0
        ).all():
            raise ValueError(f"{provider} futures {column} contains invalid values")


def reconcile_futures_bars(
    yahoo_bars: pd.DataFrame,
    massive_bars: pd.DataFrame,
    contract_map: dict[str, str],
    *,
    close_tolerance: float = 0.05,
    break_pct_tolerance: float = 0.005,
) -> pd.DataFrame:
    """Compare aligned session closes using an explicit roll/contract map."""
    if not contract_map:
        raise ValueError("A Yahoo-to-Massive futures contract mapping is required")
    if close_tolerance < 0 or break_pct_tolerance < 0:
        raise ValueError("Futures tolerances must be non-negative")
    _validate_prices(yahoo_bars, ["symbol", "trade_date", "close"], "Yahoo")
    _validate_prices(
        massive_bars,
        ["symbol", "session_date", "close", "settlement_price"],
        "Massive",
    )

    yahoo = yahoo_bars[["symbol", "trade_date", "close"]].copy()
    yahoo["symbol"] = yahoo["symbol"].astype("string").str.upper()
    yahoo["session_date"] = pd.to_datetime(
        yahoo.pop("trade_date"), errors="raise", utc=True
    ).dt.date
    yahoo["massive_contract"] = yahoo["symbol"].map(
        {key.upper(): value.upper() for key, value in contract_map.items()}
    )
    yahoo = yahoo[yahoo["massive_contract"].notna()].rename(
        columns={"symbol": "yahoo_symbol", "close": "yahoo_close"}
    )

    massive = massive_bars[
        ["symbol", "session_date", "close", "settlement_price"]
    ].copy()
    massive["massive_contract"] = massive.pop("symbol").astype("string").str.upper()
    massive["session_date"] = pd.to_datetime(
        massive["session_date"], errors="raise", utc=True
    ).dt.date
    massive = massive.rename(
        columns={
            "close": "massive_close",
            "settlement_price": "massive_settlement_price",
        }
    )

    if yahoo.duplicated(["massive_contract", "session_date"]).any():
        raise ValueError("Yahoo futures data contains duplicate mapped session keys")
    if massive.duplicated(["massive_contract", "session_date"]).any():
        raise ValueError("Massive futures data contains duplicate contract/session keys")

    merged = yahoo.merge(
        massive,
        on=["massive_contract", "session_date"],
        how="outer",
    )
    reverse_map = {value.upper(): key.upper() for key, value in contract_map.items()}
    merged["yahoo_symbol"] = merged["yahoo_symbol"].combine_first(
        merged["massive_contract"].map(reverse_map)
    )
    merged["close_difference"] = merged["yahoo_close"] - merged["massive_close"]
    reference = merged[["yahoo_close", "massive_close"]].abs().max(axis=1)
    limit = pd.concat(
        [
            pd.Series(close_tolerance, index=merged.index),
            reference * break_pct_tolerance,
        ],
        axis=1,
    ).max(axis=1)
    merged["status"] = "MATCH"
    merged["break_reason"] = pd.NA
    missing_yahoo = merged["yahoo_close"].isna()
    missing_massive = merged["massive_close"].isna()
    merged.loc[missing_yahoo, ["status", "break_reason"]] = [
        "BREAK", "MISSING_YAHOO_OBSERVATION"
    ]
    merged.loc[missing_massive, ["status", "break_reason"]] = [
        "BREAK", "MISSING_MASSIVE_OBSERVATION"
    ]
    difference = ~missing_yahoo & ~missing_massive & (
        merged["close_difference"].abs() > limit
    )
    merged.loc[difference, ["status", "break_reason"]] = [
        "BREAK", "PRICE_DIFFERENCE"
    ]
    return merged[OUTPUT_COLUMNS]
