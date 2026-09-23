from datetime import datetime, timezone
import uuid
import pandas as pd


EXCEPTION_COLUMNS = [
    "exception_id",
    "run_id",
    "instrument_type",
    "symbol",
    "trade_date",
    "yahoo_close",
    "alpaca_close",
    "close_difference",
    "severity",
    "status",
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
        exception_records.append({
            "exception_id": str(uuid.uuid4()),
            "run_id": run_id,
            "instrument_type": "EQUITY",
            "symbol": break_row.symbol,
            "trade_date": break_row.trade_date,
            "yahoo_close": break_row.yahoo_close,
            "alpaca_close": break_row.alpaca_close,
            "close_difference": break_row.close_difference,
            "severity": "BREAK",
            "status": "OPEN",
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    return pd.DataFrame(exception_records, columns=EXCEPTION_COLUMNS)