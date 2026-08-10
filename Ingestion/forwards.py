"""
FX forward ingestion (Module A).

Like swaps, FX forwards are OTC: no free feed delivers forward outright rates
directly. This module joins free sources to mark a booked forward against the
market, using only ``yfinance``:
  * Source A (internal book): a synthetic FX-forward trade blotter.
  * Source B (market): live FX spot from Yahoo Finance (e.g. ``EURUSD=X``), from
    which a market-implied forward is derived via covered interest parity (CIP).

The USD leg's rate is proxied from a free Yahoo Treasury-yield series (``^IRX``);
the foreign leg uses :data:`FOREIGN_RATES`, a documented stand-in for each
currency's free reference curve (Yahoo has no per-currency money-market ticker).
This lets the reconciliation engine compare ``booked_forward_rate`` against
``market_forward_rate``. Output is aligned with the canonical forwards schema in
WORKFLOW_DESIGN.md.
"""
from __future__ import annotations

import pandas as pd
import yfinance as yf

# Synthetic internal FX-forward blotter (stands in for the firm's booking system).
FORWARD_BLOTTER = [
    {
        "trade_id": "FXF-0001",
        "pair": "EURUSD",
        "yahoo_symbol": "EURUSD=X",
        "base_ccy": "EUR",
        "quote_ccy": "USD",
        "notional": 5_000_000,
        "booked_forward_rate": 1.0925,
        "trade_date": "2026-05-01",
        "value_date": "2026-11-02",
    },
    {
        "trade_id": "FXF-0002",
        "pair": "GBPUSD",
        "yahoo_symbol": "GBPUSD=X",
        "base_ccy": "GBP",
        "quote_ccy": "USD",
        "notional": 3_000_000,
        "booked_forward_rate": 1.2710,
        "trade_date": "2026-05-15",
        "value_date": "2026-08-17",
    },
    {
        "trade_id": "FXF-0003",
        "pair": "USDJPY",
        "yahoo_symbol": "USDJPY=X",
        "base_ccy": "USD",
        "quote_ccy": "JPY",
        "notional": 8_000_000,
        "booked_forward_rate": 156.40,
        "trade_date": "2026-06-01",
        "value_date": "2026-12-01",
    },
]

# Annualized reference rates (decimal). USD is pulled live from a free Yahoo
# proxy; the rest stand in for each currency's free money-market curve.
USD_RATE_PROXY = "^IRX"  # 13-week T-bill yield, in percent
FOREIGN_RATES = {
    "EUR": 0.0325,
    "GBP": 0.0450,
    "JPY": 0.0010,
}
DAY_COUNT = 360.0


def fetch_usd_rate(period: str = "5d") -> float:
    """Return the live USD short rate (decimal) from a free Yahoo proxy."""
    hist = yf.Ticker(USD_RATE_PROXY).history(period=period)
    if hist.empty:
        return FOREIGN_RATES.get("USD", 0.05)
    return float(hist["Close"].iloc[-1]) / 100.0


def fetch_spot(yahoo_symbol: str, period: str = "5d") -> float | None:
    """Return the latest FX spot rate for a Yahoo FX symbol (e.g. ``EURUSD=X``)."""
    hist = yf.Ticker(yahoo_symbol).history(period=period)
    if hist.empty:
        return None
    return round(float(hist["Close"].iloc[-1]), 6)


def _rate_for(ccy: str, usd_rate: float) -> float:
    """Annualized decimal rate for a currency leg."""
    return usd_rate if ccy == "USD" else FOREIGN_RATES.get(ccy, 0.0)


def market_forward(spot: float, base_ccy: str, quote_ccy: str, days: int, usd_rate: float) -> float:
    """Covered-interest-parity forward: spot adjusted by the rate differential.

    For a ``base/quote`` quote (units of quote per 1 base), the forward is
    ``spot * (1 + r_quote * t) / (1 + r_base * t)`` with ``t = days / 360``.
    """
    t = days / DAY_COUNT
    r_base = _rate_for(base_ccy, usd_rate)
    r_quote = _rate_for(quote_ccy, usd_rate)
    return round(spot * (1 + r_quote * t) / (1 + r_base * t), 6)


def ingest_forwards(blotter=None) -> pd.DataFrame:
    """Ingest the FX-forward blotter and mark each trade against a market forward.

    Args:
        blotter: Iterable of forward dicts. Defaults to :data:`FORWARD_BLOTTER`.

    Returns:
        DataFrame with booked vs market-implied forward rates and the difference
        in pips, plus the ingestion ``trade_date``.
    """
    blotter = blotter if blotter is not None else FORWARD_BLOTTER
    usd_rate = fetch_usd_rate()
    asof = pd.Timestamp.now(tz="UTC").normalize().tz_localize(None)

    rows = []
    for trade in blotter:
        spot = fetch_spot(trade["yahoo_symbol"])
        row = dict(trade)
        row["spot_rate"] = spot

        days = (pd.Timestamp(trade["value_date"]) - asof).days
        row["days_to_value"] = days

        if spot is None or days <= 0:
            row["market_forward_rate"] = None
            row["diff_pips"] = None
        else:
            mkt_fwd = market_forward(
                spot, trade["base_ccy"], trade["quote_ccy"], days, usd_rate
            )
            row["market_forward_rate"] = mkt_fwd
            # JPY pairs are quoted to 2dp (pip = 0.01); most majors to 4dp (pip = 0.0001).
            pip = 0.01 if trade["quote_ccy"] == "JPY" else 0.0001
            row["diff_pips"] = round((trade["booked_forward_rate"] - mkt_fwd) / pip, 1)

        rows.append(row)

    df = pd.DataFrame(rows)
    df["ingested_date"] = asof.date()
    return df


if __name__ == "__main__":
    print(ingest_forwards())
