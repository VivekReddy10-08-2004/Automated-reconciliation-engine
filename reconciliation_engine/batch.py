import argparse
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import re
from collections.abc import Iterable
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv

from alpaca.data.timeframe import TimeFrame

from reconciliation_engine.storage.data_lake import (
    EQUITY_COLUMNS,
    EQUITY_INSTRUMENT,
    archive_equity_source,
    archive_reconciliation_results,
    read_dataframe,
)
from reconciliation_engine.reconciliation.exceptions import create_equity_exceptions
from reconciliation_engine.ingestion.alpaca_equities import fetch_stock_bars
from reconciliation_engine.ingestion.yahoo_equities import (
    fetch_equity_bars as fetch_yahoo_bars,
)
from reconciliation_engine.reconciliation.equities import reconcile_equity_bars
from reconciliation_engine.notifications import send_run_notification
from reconciliation_engine.storage.duckdb import (
    get_pipeline_run,
    initialize_database,
    persist_equity_run,
)


SYMBOLS_ENV = "RECONCILIATION_SYMBOLS"
DEFAULT_RECONCILIATION_SYMBOLS = ("AAPL",)
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,14}$")
RUN_DEFINITION_VERSION = "equity-daily-v3"
DEFAULT_WARN_TOLERANCE = 0.01
DEFAULT_BREAK_TOLERANCE = 0.05
DEFAULT_WARN_PCT_TOLERANCE = 0.001
DEFAULT_BREAK_PCT_TOLERANCE = 0.005
EQUITY_LOOKBACK_CALENDAR_DAYS = 10
MARKET_TIMEZONE = ZoneInfo("America/New_York")


def normalize_symbols(values: Iterable[str]) -> tuple[str, ...]:
    """Normalize, validate, and de-duplicate configured equity symbols."""
    if isinstance(values, str):
        values = [values]
    symbols: list[str] = []
    for value in values:
        for candidate in str(value).split(","):
            symbol = candidate.strip().upper()
            if not symbol:
                continue
            if not SYMBOL_PATTERN.fullmatch(symbol):
                raise ValueError(
                    f"Invalid equity symbol {candidate!r}; use 1-15 letters, "
                    "numbers, dots, or hyphens"
                )
            if symbol not in symbols:
                symbols.append(symbol)
    if not symbols:
        raise ValueError("At least one reconciliation symbol is required")
    return tuple(symbols)


def get_reconciliation_symbols(
    configured_symbols: Iterable[str] | None = None,
) -> tuple[str, ...]:
    """Resolve CLI symbols first, then project.env, then the demo default."""
    if configured_symbols is not None:
        return normalize_symbols(configured_symbols)

    load_dotenv("project.env", override=False)
    environment_value = os.getenv(SYMBOLS_ENV)
    if environment_value is None:
        return DEFAULT_RECONCILIATION_SYMBOLS
    return normalize_symbols([environment_value])


def get_control_date(value: date | str | None = None) -> date:
    """Return the logical US-market date used to make retries deterministic."""
    if value is None:
        return datetime.now(MARKET_TIMEZONE).date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def build_run_id(symbols: Iterable[str], control_date: date | str) -> str:
    """Build one stable run ID per control date, universe, and rule version."""
    selected_symbols = tuple(sorted(normalize_symbols(symbols)))
    logical_key = (
        f"{RUN_DEFINITION_VERSION}|{get_control_date(control_date).isoformat()}|"
        f"{','.join(selected_symbols)}"
    )
    return str(uuid5(NAMESPACE_URL, logical_key))


def fetch_equity_sources(
    symbols: Iterable[str] = DEFAULT_RECONCILIATION_SYMBOLS,
    control_date: date | str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch the two provider datasets used by the daily equity control."""
    selected_symbols = normalize_symbols(symbols)
    effective_control_date = get_control_date(control_date)
    end = datetime.combine(
        effective_control_date + timedelta(days=1),
        datetime.min.time(),
        tzinfo=timezone.utc,
    )
    start = end - timedelta(days=EQUITY_LOOKBACK_CALENDAR_DAYS)

    yahoo_frames = [
        fetch_yahoo_bars(symbol, start=start, end=end, interval="1d")
        for symbol in selected_symbols
    ]
    alpaca_frames = [
        fetch_stock_bars(symbol, start, end, TimeFrame.Day)
        for symbol in selected_symbols
    ]
    return (
        pd.concat(yahoo_frames, ignore_index=True),
        pd.concat(alpaca_frames, ignore_index=True),
    )


def reconciliation(
    symbols: Iterable[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Fetch and reconcile data without persistence (kept for library callers)."""
    yahoo_bars, alpaca_bars = fetch_equity_sources(
        get_reconciliation_symbols(symbols)
    )

    reconciliation_df = reconcile_equity_bars(yahoo_bars, alpaca_bars)
    return yahoo_bars, alpaca_bars, reconciliation_df


def replay_archived_run(
    run_id: str,
    data_root: str | Path = "data",
) -> pd.DataFrame:
    """Recompute reconciliation from saved Parquet inputs without API calls."""
    yahoo_bars = read_dataframe(
        "YAHOO",
        EQUITY_INSTRUMENT,
        run_id,
        layer="normalized",
        data_root=data_root,
    )
    alpaca_bars = read_dataframe(
        "ALPACA",
        EQUITY_INSTRUMENT,
        run_id,
        layer="normalized",
        data_root=data_root,
    )

    if yahoo_bars.empty or alpaca_bars.empty:
        raise FileNotFoundError(
            f"Normalized Yahoo and Alpaca archives are required for run {run_id}"
        )

    return reconcile_equity_bars(
        yahoo_bars[EQUITY_COLUMNS],
        alpaca_bars[EQUITY_COLUMNS],
    )


def main(
    db_path: str = "reconciliation.duckdb",
    data_root: str | Path = "data",
    symbols: Iterable[str] | None = None,
    control_date: date | str | None = None,
) -> dict:
    selected_symbols = get_reconciliation_symbols(symbols)
    effective_control_date = get_control_date(control_date)
    run_id = build_run_id(selected_symbols, effective_control_date)
    started_at = datetime.now(timezone.utc).isoformat()
    break_count = 0

    try:
        initialize_database(db_path)
        existing_run = get_pipeline_run(run_id, db_path)
        if existing_run is not None:
            summary = {
                "run_id": run_id,
                "status": existing_run["status"],
                "break_count": existing_run["break_count"],
                "symbols": list(selected_symbols),
                "control_date": effective_control_date.isoformat(),
                "already_completed": True,
            }
            print(
                f"Run ID: {run_id} | Status: {existing_run['status']} | "
                "Already completed; no provider calls or writes performed"
            )
            return summary

        yahoo_raw, alpaca_raw = fetch_equity_sources(
            selected_symbols,
            effective_control_date,
        )

        yahoo_bars = archive_equity_source(
            yahoo_raw,
            "YAHOO",
            run_id,
            data_root=data_root,
            idempotent=True,
        )
        alpaca_bars = archive_equity_source(
            alpaca_raw,
            "ALPACA",
            run_id,
            data_root=data_root,
            idempotent=True,
        )

        reconciliation_df = reconcile_equity_bars(
            yahoo_bars,
            alpaca_bars,
            close_tolerance=DEFAULT_BREAK_TOLERANCE,
            warn_tolerance=DEFAULT_WARN_TOLERANCE,
            break_pct_tolerance=DEFAULT_BREAK_PCT_TOLERANCE,
            warn_pct_tolerance=DEFAULT_WARN_PCT_TOLERANCE,
        )
        archive_reconciliation_results(
            reconciliation_df,
            run_id,
            data_root=data_root,
            idempotent=True,
        )
        match_count = len(reconciliation_df[reconciliation_df["status"] == "MATCH"])
        warn_count = len(reconciliation_df[reconciliation_df["status"] == "WARN"])
        break_count = len(reconciliation_df[reconciliation_df["status"] == "BREAK"])
        finished_at = datetime.now(timezone.utc).isoformat()
        run_status = "SUCCESS" if break_count == 0 else "SUCCESS_WITH_BREAKS"
        largest_difference = reconciliation_df["close_difference"].abs().max()

        exceptions_df = create_equity_exceptions(reconciliation_df, run_id)
        persisted = persist_equity_run(
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            status=run_status,
            yahoo_bars=yahoo_bars,
            alpaca_bars=alpaca_bars,
            reconciliation=reconciliation_df,
            exceptions=exceptions_df,
            db_path=db_path,
        )
        if not persisted:
            existing_run = get_pipeline_run(run_id, db_path)
            if existing_run is None:
                raise RuntimeError("Run persistence lost an idempotency race")
            run_status = str(existing_run["status"])
            break_count = int(existing_run["break_count"])

        summary = {
            "run_id": run_id,
            "status": run_status,
            "break_count": break_count,
            "symbols": list(selected_symbols),
            "control_date": effective_control_date.isoformat(),
            "already_completed": not persisted,
        }

        print(
            f"Run ID: {run_id} | "
            f"Symbols: {','.join(selected_symbols)} | "
            f"Yahoo rows: {len(yahoo_bars)} | "
            f"Alpaca rows: {len(alpaca_bars)} | "
            f"MATCH: {match_count} | "
            f"WARN: {warn_count} | "
            f"BREAK: {break_count} | "
            f"Largest difference: {largest_difference}"
        )
        print("Yahoo dates:")
        print(yahoo_bars["trade_date"].tolist())
        print("Alpaca dates:")
        print(alpaca_bars["trade_date"].tolist())
        print(reconciliation_df)
    except Exception:
        failed_summary = {
            "run_id": run_id,
            "status": "FAILED",
            "break_count": break_count,
            "symbols": list(selected_symbols),
            "control_date": effective_control_date.isoformat(),
        }
        send_run_notification(failed_summary)
        raise

    send_run_notification(summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the live equity control or replay archived provider data."
    )
    parser.add_argument("--db", default="reconciliation.duckdb")
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        help=(
            "Equity symbol to reconcile; repeat the option or use comma-separated "
            "values. Overrides RECONCILIATION_SYMBOLS."
        ),
    )
    parser.add_argument(
        "--replay-run-id",
        help="Recompute a report from normalized Parquet files without API calls",
    )
    parser.add_argument(
        "--control-date",
        help=(
            "Logical market date in YYYY-MM-DD form. Retries with the same date "
            "and symbol universe reuse the same run ID."
        ),
    )
    arguments = parser.parse_args()

    if arguments.replay_run_id:
        replay = replay_archived_run(arguments.replay_run_id, arguments.data_root)
        print(replay)
        print(
            f"Replayed run {arguments.replay_run_id} | "
            f"MATCH: {(replay['status'] == 'MATCH').sum()} | "
            f"BREAK: {(replay['status'] == 'BREAK').sum()}"
        )
    else:
        main(
            arguments.db,
            arguments.data_root,
            arguments.symbols,
            arguments.control_date,
        )
