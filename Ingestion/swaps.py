"""
Interest-rate swap ingestion (Module A).

Swaps are OTC instruments: there is no consolidated, free market-data feed that
lets you "download a swap chain" the way you can with listed options or futures.
In real trade-ops, swap records originate in the firm's internal booking system
and are then *marked* against reference rate curves.

This module mirrors that pattern using only free sources:
  * Source A (internal book): a synthetic swap trade blotter (the firm's book).
  * Source B (market reference): the floating-leg reference rate, proxied from a
    free Yahoo Finance Treasury-yield series (e.g. ``^IRX`` ~ short rate /SOFR
    proxy, ``^TNX`` = 10Y yield).

Joining them produces a record per swap with the booked fixed rate alongside a
current market reference rate, ready for the reconciliation engine. Output is
aligned with the canonical swaps schema in WORKFLOW_DESIGN.md.
"""
from __future__ import annotations

import pandas as pd
import yfinance as yf

# Synthetic internal swap blotter (stands in for the firm's booking system).
# In production this would be read from the trade-capture DB / CSV export.
SWAP_BLOTTER = [
    {
        "trade_id": "IRS-0001",
        "counterparty": "BankA",
        "currency": "USD",
        "notional": 10_000_000,
        "pay_receive": "PAY_FIXED",
        "fixed_rate_pct": 3.85,
        "floating_index": "USD-SOFR",
        "pay_freq": "3M",
        "effective_date": "2024-01-15",
        "maturity_date": "2029-01-15",
    },
    {
        "trade_id": "IRS-0002",
        "counterparty": "BankB",
        "currency": "USD",
        "notional": 25_000_000,
        "pay_receive": "RECEIVE_FIXED",
        "fixed_rate_pct": 4.10,
        "floating_index": "USD-SOFR",
        "pay_freq": "3M",
        "effective_date": "2023-09-20",
        "maturity_date": "2030-09-20",
    },
    {
        "trade_id": "IRS-0003",
        "counterparty": "BankA",
        "currency": "USD",
        "notional": 5_000_000,
        "pay_receive": "PAY_FIXED",
        "fixed_rate_pct": 4.45,
        "floating_index": "USD-10Y",
        "pay_freq": "6M",
        "effective_date": "2024-03-01",
        "maturity_date": "2034-03-01",
    },
]

# Free reference-rate proxies (Yahoo Finance yield indices), quoted in percent.
# These stand in for each floating index's market curve.
RATE_PROXIES = {
    "USD-SOFR": "^IRX",  # 13-week T-bill yield, used as a short-rate / SOFR proxy
    "USD-10Y": "^TNX",   # 10-year Treasury yield
}


def fetch_reference_rate(floating_index: str, period: str = "5d") -> float | None:
    """Return the latest market reference rate (in percent) for a floating index.

    Falls back to the 10Y proxy when the index is unknown. Returns ``None`` if
    Yahoo Finance has no data for the proxy.
    """
    proxy = RATE_PROXIES.get(floating_index, "^TNX")
    hist = yf.Ticker(proxy).history(period=period)
    if hist.empty:
        return None
    return round(float(hist["Close"].iloc[-1]), 4)


def ingest_swaps(blotter=None) -> pd.DataFrame:
    """Ingest the swap blotter and enrich each trade with its market reference rate.

    Args:
        blotter: Iterable of swap dicts. Defaults to :data:`SWAP_BLOTTER`.

    Returns:
        DataFrame of swap positions with ``reference_rate_pct`` and an indicative
        ``fixed_minus_float_pct`` carry, plus the ingestion ``trade_date``.
    """
    blotter = blotter if blotter is not None else SWAP_BLOTTER

    # Fetch each distinct reference rate once.
    indices = {trade["floating_index"] for trade in blotter}
    rate_cache = {idx: fetch_reference_rate(idx) for idx in indices}

    rows = []
    for trade in blotter:
        ref = rate_cache.get(trade["floating_index"])
        row = dict(trade)
        row["reference_rate_pct"] = ref
        row["fixed_minus_float_pct"] = (
            None if ref is None else round(trade["fixed_rate_pct"] - ref, 4)
        )
        rows.append(row)

    df = pd.DataFrame(rows)
    df["trade_date"] = pd.Timestamp.now(tz="UTC").date()
    return df


if __name__ == "__main__":
    print(ingest_swaps())
