"""Generate deterministic synthetic equity bars without provider credentials."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def synthetic_equity_bars(
    rows: int,
    *,
    source_offset: float = 0.0,
    break_every: int = 0,
) -> pd.DataFrame:
    """Return unique, deterministic bars suitable for correctness benchmarks."""
    if rows <= 0:
        raise ValueError("rows must be positive")
    index = np.arange(rows, dtype=np.int64)
    dates = np.datetime64("2000-01-01") + index.astype("timedelta64[D]")
    close = 100.0 + (index % 1000) / 100.0 + source_offset
    if break_every:
        close = close.copy()
        close[index % break_every == 0] += 1.0
    return pd.DataFrame(
        {
            "symbol": pd.Series(
                np.char.add("S", (index % 10000).astype(str)), dtype="string"
            ),
            "trade_date": dates,
            "open": close - 0.25,
            "high": close + 0.50,
            "low": close - 0.50,
            "close": close,
            "volume": 1_000.0 + index % 100_000,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--break-every", type=int, default=0)
    args = parser.parse_args()
    frame = synthetic_equity_bars(args.rows, break_every=args.break_every)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(args.output, index=False)
    print(f"wrote {len(frame)} deterministic rows to {args.output}")


if __name__ == "__main__":
    main()
