"""Persisted Yahoo/Alpaca option-chain reconciliation workflow."""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, uuid5

from reconciliation_engine.ingestion.alpaca_options import fetch_alpaca_option_chain
from reconciliation_engine.ingestion.yahoo_options import fetch_option_chain
from reconciliation_engine.reconciliation.options import reconcile_option_sources
from reconciliation_engine.storage.duckdb import initialize_database, persist_option_run


def build_option_run_id(underlying: str, expiry: str, control_date: date | str) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            f"option-chain-v1|{underlying.upper()}|{expiry}|{control_date}",
        )
    )


def main(
    underlying: str,
    expiry: str,
    *,
    control_date: date | str | None = None,
    db_path: str = "reconciliation.duckdb",
) -> dict[str, object]:
    effective_date = control_date or datetime.now(timezone.utc).date()
    run_id = build_option_run_id(underlying, expiry, effective_date)
    initialize_database(db_path)
    started_at = datetime.now(timezone.utc).isoformat()
    yahoo = fetch_option_chain(underlying.upper(), expiry)
    alpaca = fetch_alpaca_option_chain(underlying.upper(), expiry)
    reconciliation = reconcile_option_sources({"YAHOO": yahoo, "ALPACA": alpaca})
    breaks = int((reconciliation["status"] == "BREAK").sum())
    status = "SUCCESS" if breaks == 0 else "SUCCESS_WITH_BREAKS"
    persisted = persist_option_run(
        run_id=run_id,
        underlying=underlying,
        expiry=expiry,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc).isoformat(),
        status=status,
        yahoo_options=yahoo,
        alpaca_options=alpaca,
        reconciliation=reconciliation,
        db_path=db_path,
    )
    return {
        "run_id": run_id,
        "status": status,
        "break_count": breaks,
        "already_completed": not persisted,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("underlying")
    parser.add_argument("expiry", help="Expiration in YYYY-MM-DD form")
    parser.add_argument("--control-date")
    parser.add_argument("--db", default="reconciliation.duckdb")
    args = parser.parse_args()
    print(
        main(
            args.underlying,
            args.expiry,
            control_date=args.control_date,
            db_path=args.db,
        )
    )
