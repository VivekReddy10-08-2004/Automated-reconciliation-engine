"""Reconcile normalized Yahoo and Alpaca option chains."""

import pandas as pd

OUTPUT_COLUMNS = [
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "contract_symbol",
    "yahoo_close",
    "alpaca_close",
    "close_difference",
    "yahoo_bid",
    "yahoo_ask",
    "alpaca_bid",
    "alpaca_ask",
    "status",
]

MATCH_COLUMNS = [
    "underlying",
    "expiry",
    "strike",
    "option_type",
]


def reconcile_option_chain(
    yahoo_options: pd.DataFrame,
    alpaca_options: pd.DataFrame,
    close_tolerance: float = 0.05,
) -> pd.DataFrame:
    # An unavailable provider is not a MATCH or a BREAK. There is no valid
    # cross-vendor comparison to make, so preserve the output contract and let
    # the calling report explain why the result is empty.
    if yahoo_options.empty or alpaca_options.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    yahoo = yahoo_options[
        MATCH_COLUMNS + ["contract_symbol", "close", "bid", "ask"]
    ].copy()

    alpaca = alpaca_options[
        MATCH_COLUMNS + ["close", "bid", "ask"]
    ].copy()

    yahoo = yahoo.rename(
        columns={
            "close": "yahoo_close",
            "bid": "yahoo_bid",
            "ask": "yahoo_ask",
        }
    )

    alpaca = alpaca.rename(
        columns={
            "close": "alpaca_close",
            "bid": "alpaca_bid",
            "ask": "alpaca_ask",
        }
    )

    merged = pd.merge(
        yahoo,
        alpaca,
        on=MATCH_COLUMNS,
        how="inner",
    )

    merged["close_difference"] = (
        merged["yahoo_close"] - merged["alpaca_close"]
    )

    merged["status"] = "MATCH"
    merged.loc[
        (merged["close_difference"].abs() > close_tolerance)
        | merged["close_difference"].isna(),
        "status",
    ] = "BREAK"

    return merged[OUTPUT_COLUMNS]
