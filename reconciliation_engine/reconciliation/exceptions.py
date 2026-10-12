"""Create actionable reconciliation exception records."""

from datetime import datetime, timedelta, timezone
import uuid
import pandas as pd


EXCEPTION_COLUMNS = [
    "exception_id",
    "run_id",
    "instrument_type",
    "symbol",
    "trade_date",
    "source_a",
    "source_b",
    "source_a_close",
    "source_b_close",
    "close_difference",
    "severity",
    "priority",
    "status",
    "owner",
    "reason_code",
    "due_at",
    "resolved_at",
    "created_at",
]


def create_equity_exceptions(
    reconciliation_df: pd.DataFrame,
    run_id: str,
) -> pd.DataFrame:
    breaks = reconciliation_df[reconciliation_df["status"] == "BREAK"]

    if breaks.empty:
        return pd.DataFrame(columns=EXCEPTION_COLUMNS)

    exception_records = []
    for break_row in breaks.itertuples(index=False):
        pairwise = "source_a" in breaks.columns
        source_a = break_row.source_a if pairwise else "YAHOO"
        source_b = break_row.source_b if pairwise else "ALPACA"
        source_a_close = (
            break_row.source_a_close if pairwise else break_row.yahoo_close
        )
        source_b_close = (
            break_row.source_b_close if pairwise else break_row.alpaca_close
        )
        created_at = datetime.now(timezone.utc)
        reason_code = getattr(break_row, "break_reason", None) or "PRICE_DIFFERENCE"
        exception_records.append({
            "exception_id": str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{run_id}|EQUITY|{break_row.symbol}|{break_row.trade_date}|"
                    f"{source_a}|{source_b}",
                )
            ),
            "run_id": run_id,
            "instrument_type": "EQUITY",
            "symbol": break_row.symbol,
            "trade_date": break_row.trade_date,
            "source_a": source_a,
            "source_b": source_b,
            "source_a_close": source_a_close,
            "source_b_close": source_b_close,
            "close_difference": break_row.close_difference,
            "severity": "BREAK",
            "priority": "HIGH",
            "status": "OPEN",
            "owner": "UNASSIGNED",
            "reason_code": reason_code,
            "due_at": (created_at + timedelta(hours=24)).isoformat(),
            "resolved_at": None,
            "created_at": created_at.isoformat(),
        })

    return pd.DataFrame(exception_records, columns=EXCEPTION_COLUMNS)
