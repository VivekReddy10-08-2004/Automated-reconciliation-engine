"""Reconcile normalized Yahoo and Alpaca option chains."""

import pandas as pd
from itertools import combinations

OUTPUT_COLUMNS = [
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "contract_symbol",
    "trade_date",
    "yahoo_close",
    "alpaca_close",
    "close_difference",
    "yahoo_bid",
    "yahoo_ask",
    "alpaca_bid",
    "alpaca_ask",
    "status",
    "break_reason",
]

MATCH_COLUMNS = [
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "trade_date",
]

PAIRWISE_OUTPUT_COLUMNS = [
    "underlying", "expiry", "strike", "option_type", "contract_symbol",
    "trade_date", "source_a", "source_b", "source_a_close", "source_b_close",
    "close_difference", "source_a_bid", "source_a_ask", "source_b_bid",
    "source_b_ask", "status", "break_reason",
]


def reconcile_option_sources(
    provider_options: dict[str, pd.DataFrame],
    close_tolerance: float = 0.05,
) -> pd.DataFrame:
    """Reconcile every available option-provider pair by contract identity."""
    if len(provider_options) < 2:
        raise ValueError("At least two option providers are required")
    results: list[pd.DataFrame] = []
    for (source_a, options_a), (source_b, options_b) in combinations(
        provider_options.items(), 2
    ):
        pair = reconcile_option_chain(
            options_a, options_b, close_tolerance=close_tolerance
        ).rename(
            columns={
                "yahoo_close": "source_a_close",
                "alpaca_close": "source_b_close",
                "yahoo_bid": "source_a_bid",
                "yahoo_ask": "source_a_ask",
                "alpaca_bid": "source_b_bid",
                "alpaca_ask": "source_b_ask",
            }
        )
        pair["source_a"] = source_a.upper()
        pair["source_b"] = source_b.upper()
        pair.loc[
            pair["break_reason"] == "MISSING_ALPACA_CONTRACT", "break_reason"
        ] = f"MISSING_{source_b.upper()}_CONTRACT"
        pair.loc[
            pair["break_reason"] == "MISSING_YAHOO_CONTRACT", "break_reason"
        ] = f"MISSING_{source_a.upper()}_CONTRACT"
        results.append(pair[PAIRWISE_OUTPUT_COLUMNS])
    return pd.concat(results, ignore_index=True)


def _prepare_options(frame: pd.DataFrame, provider: str) -> pd.DataFrame:
    required = MATCH_COLUMNS + ["contract_symbol", "close", "bid", "ask"]
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"{provider} option data is missing columns: {missing}")

    prepared = frame[required].copy()
    prepared["underlying"] = prepared["underlying"].astype("string").str.upper()
    prepared["expiry"] = pd.to_datetime(
        prepared["expiry"], errors="raise", utc=True
    ).dt.date
    prepared["trade_date"] = pd.to_datetime(
        prepared["trade_date"], errors="raise", utc=True
    ).dt.date
    prepared["strike"] = pd.to_numeric(prepared["strike"], errors="raise")
    prepared["option_type"] = (
        prepared["option_type"].astype("string").str.upper()
    )
    if prepared[MATCH_COLUMNS].isna().any().any():
        raise ValueError(f"{provider} option data contains a null contract key")
    if prepared.duplicated(MATCH_COLUMNS).any():
        raise ValueError(f"{provider} option data contains duplicate contract keys")
    return prepared.rename(
        columns={
            "contract_symbol": f"{provider}_contract_symbol",
            "close": f"{provider}_close",
            "bid": f"{provider}_bid",
            "ask": f"{provider}_ask",
        }
    )


def reconcile_option_chain(
    yahoo_options: pd.DataFrame,
    alpaca_options: pd.DataFrame,
    close_tolerance: float = 0.05,
) -> pd.DataFrame:
    if close_tolerance < 0:
        raise ValueError("close_tolerance must be non-negative")
    if yahoo_options.empty and alpaca_options.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    yahoo = _prepare_options(yahoo_options, "yahoo")
    alpaca = _prepare_options(alpaca_options, "alpaca")

    merged = pd.merge(
        yahoo,
        alpaca,
        on=MATCH_COLUMNS,
        how="outer",
        indicator=True,
    )

    merged["contract_symbol"] = merged["yahoo_contract_symbol"].combine_first(
        merged["alpaca_contract_symbol"]
    )

    merged["close_difference"] = (
        merged["yahoo_close"] - merged["alpaca_close"]
    )

    merged["status"] = "MATCH"
    merged["break_reason"] = pd.NA
    merged.loc[merged["_merge"] == "left_only", ["status", "break_reason"]] = [
        "BREAK",
        "MISSING_ALPACA_CONTRACT",
    ]
    merged.loc[merged["_merge"] == "right_only", ["status", "break_reason"]] = [
        "BREAK",
        "MISSING_YAHOO_CONTRACT",
    ]
    missing_price = (
        (merged["_merge"] == "both")
        & (merged[["yahoo_close", "alpaca_close"]].isna().any(axis=1))
    )
    merged.loc[missing_price, ["status", "break_reason"]] = [
        "BREAK",
        "MISSING_COMPARISON_PRICE",
    ]
    price_break = (
        (merged["_merge"] == "both")
        & ~missing_price
        & (merged["close_difference"].abs() > close_tolerance)
    )
    merged.loc[price_break, ["status", "break_reason"]] = [
        "BREAK",
        "PRICE_DIFFERENCE",
    ]

    return merged[OUTPUT_COLUMNS]
