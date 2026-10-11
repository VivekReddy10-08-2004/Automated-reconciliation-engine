from collections.abc import Mapping
from datetime import date, datetime, timezone

import pandas as pd
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.requests import OptionChainRequest

from reconciliation_engine.security import load_alpaca_credentials

OPTIONS_COLUMNS = [
    "underlying",
    "expiry",
    "strike",
    "option_type",
    "contract_symbol",
    "trade_date",
    "close",
    "bid",
    "ask",
    "volume",
    "open_interest",
]


def _value(source: object, name: str, default: object = None) -> object:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _contract_details(contract_symbol: str) -> tuple[str, float, str]:
    """Read expiration, strike, and type from an OCC option symbol."""
    if len(contract_symbol) < 15:
        raise ValueError(f"Invalid OCC option symbol: {contract_symbol}")

    option_type = contract_symbol[12]
    strike = int(contract_symbol[13:]) / 1000
    expiry = date.fromisoformat(
        f"20{contract_symbol[6:8]}-{contract_symbol[8:10]}-{contract_symbol[10:12]}"
    ).isoformat()
    return expiry, strike, option_type


def fetch_alpaca_option_chain(
    underlying: str,
    expiration: str,
) -> pd.DataFrame:
    """Fetch and normalize one Alpaca option chain expiration."""
    api_key, secret_key = load_alpaca_credentials()

    expiration_date = date.fromisoformat(expiration)
    client = OptionHistoricalDataClient(api_key, secret_key)
    request = OptionChainRequest(
        underlying_symbol=underlying,
        expiration_dates=[expiration_date],
    )
    chain = client.get_option_chain(request)

    rows: list[dict[str, object]] = []
    trade_date = datetime.now(timezone.utc).date()
    for contract_symbol, snapshot in (chain or {}).items():
        try:
            expiry, strike, option_type = _contract_details(contract_symbol)
        except (TypeError, ValueError):
            continue

        latest_trade = _value(snapshot, "latest_trade", {})
        latest_quote = _value(snapshot, "latest_quote", {})
        daily_bar = _value(snapshot, "daily_bar", {})
        rows.append(
            {
                "underlying": underlying,
                "expiry": expiry,
                "strike": strike,
                "option_type": option_type,
                "contract_symbol": contract_symbol,
                "trade_date": trade_date,
                "close": _value(latest_trade, "price"),
                "bid": _value(latest_quote, "bid_price"),
                "ask": _value(latest_quote, "ask_price"),
                "volume": _value(daily_bar, "volume"),
                "open_interest": _value(snapshot, "open_interest"),
            }
        )

    return pd.DataFrame(rows, columns=OPTIONS_COLUMNS)
