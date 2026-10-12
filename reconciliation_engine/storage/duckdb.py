import duckdb
import pandas as pd

from reconciliation_engine.security import restrict_file_permissions


def _migrate_equity_staging_primary_key(conn: duckdb.DuckDBPyConnection) -> None:
    """Add symbol to the business key used by databases from older releases."""
    constraint = conn.execute(
        """
        SELECT constraint_text
        FROM duckdb_constraints()
        WHERE table_name = 'equity_bars_staging'
          AND constraint_type = 'PRIMARY KEY'
        """
    ).fetchone()
    if constraint is None or "symbol" in constraint[0].lower():
        return

    conn.execute(
        """
        CREATE TABLE equity_bars_staging_v2 (
            symbol VARCHAR,
            run_id VARCHAR,
            source VARCHAR,
            timestamp TIMESTAMP,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            PRIMARY KEY (run_id, source, symbol, timestamp),
            FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
        )
        """
    )
    conn.execute("INSERT INTO equity_bars_staging_v2 SELECT * FROM equity_bars_staging")
    conn.execute("DROP TABLE equity_bars_staging")
    conn.execute("ALTER TABLE equity_bars_staging_v2 RENAME TO equity_bars_staging")

def initialize_database(db_path: str = "reconciliation.duckdb") -> None:
    with duckdb.connect(db_path) as conn:

        #pipeline runs table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR PRIMARY KEY,
                created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                status VARCHAR,
                yahoo_row_count BIGINT,
                alpaca_row_count BIGINT,
                match_count BIGINT,
                break_count BIGINT               
            )
            """
        )

        #equity bars staging table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS equity_bars_staging (
                symbol VARCHAR,
                run_id VARCHAR,
                source VARCHAR,
                timestamp TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                PRIMARY KEY (run_id, source, symbol, timestamp),
                FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
            )
            """
        )
        _migrate_equity_staging_primary_key(conn)

        #equity reconciliation results table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS equity_reconciliation_results (
                run_id VARCHAR,
                symbol VARCHAR,
                timestamp TIMESTAMP,
                metric_name VARCHAR,
                source_a_value DOUBLE,
                source_b_value DOUBLE,
                difference DOUBLE,
                is_discrepancy BOOLEAN,
                PRIMARY KEY (run_id, symbol, timestamp, metric_name)
            )
        """
        )

        #create exceptions table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS equity_exceptions (
                exception_id VARCHAR PRIMARY KEY,
                run_id VARCHAR,
                instrument_type VARCHAR,
                symbol VARCHAR,
                trade_date DATE,
                yahoo_close DOUBLE,
                alpaca_close DOUBLE,
                close_difference DOUBLE,
                severity VARCHAR,
                status VARCHAR,
                created_at TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
            )
            """
        )
    restrict_file_permissions(db_path)

def save_pipeline_run(
    run_id: str,
    started_at: str,
    finished_at: str,
    status: str,
    yahoo_row_count: int,
    alpaca_row_count: int,
    match_count: int,
    break_count: int,
    db_path: str = "reconciliation.duckdb",
) -> None:
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO pipeline_runs (
                run_id,
                started_at,
                finished_at,
                status,
                yahoo_row_count,
                alpaca_row_count,
                match_count,
                break_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                started_at,
                finished_at,
                status,
                yahoo_row_count,
                alpaca_row_count,
                match_count,
                break_count,
            ),
        )

def save_equity_bars(
    df: pd.DataFrame,
    source: str,
    run_id: str,
    db_path: str = "reconciliation.duckdb"
) -> None:
    rows = _equity_bar_rows(df, source, run_id)
    with duckdb.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO equity_bars_staging
            (symbol, run_id, source, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def _equity_bar_rows(df: pd.DataFrame, source: str, run_id: str) -> list[tuple]:
    columns = [
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    selected_df = df[columns].copy()

    selected_df["run_id"] = run_id
    selected_df["source"] = source

    selected_df.rename(
        columns={"trade_date": "timestamp"},
        inplace=True
    )

    selected_df = selected_df[
        [
            "symbol",
            "run_id",
            "source",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    ]

    return [tuple(row) for row in selected_df.itertuples(index=False, name=None)]

    
def save_reconciliation_results(
    df: pd.DataFrame,
    run_id: str,
    db_path: str = "reconciliation.duckdb"
) -> None:
    results = _reconciliation_rows(df, run_id)
    with duckdb.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO equity_reconciliation_results
            (
                run_id,
                symbol,
                timestamp,
                metric_name,
                source_a_value,
                source_b_value,
                difference,
                is_discrepancy
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            results,
        )


def _reconciliation_rows(df: pd.DataFrame, run_id: str) -> list[tuple]:
    results: list[tuple] = []
    for _, row in df.iterrows():
        results.extend(
            [
            (
                run_id,
                row["symbol"],
                row["trade_date"],
                "close",
                row["yahoo_close"],
                row["alpaca_close"],
                row["close_difference"],
                row["status"] == "BREAK",
            ),
            (
                run_id,
                row["symbol"],
                row["trade_date"],
                "volume",
                row["yahoo_volume"],
                row["alpaca_volume"],
                row["volume_difference"],
                row["status"] == "BREAK",
            ),
            ]
        )
    return results

def save_exceptions(df: pd.DataFrame, db_path: str = "reconciliation.duckdb") -> None:
    rows = _exception_rows(df)
    if not rows:
        return
    with duckdb.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO equity_exceptions
            (
                exception_id,
                run_id,
                instrument_type,
                symbol,
                trade_date,
                yahoo_close,
                alpaca_close,
                close_difference,
                severity,
                status,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def _exception_rows(df: pd.DataFrame) -> list[tuple]:
    return [tuple(row) for row in df.itertuples(index=False, name=None)]


def get_pipeline_run(
    run_id: str,
    db_path: str = "reconciliation.duckdb",
) -> dict[str, object] | None:
    """Return a stored run summary used to short-circuit safe retries."""
    with duckdb.connect(db_path, read_only=True) as conn:
        row = conn.execute(
            """
            SELECT run_id, status, yahoo_row_count, alpaca_row_count,
                   match_count, break_count
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()
    if row is None:
        return None
    columns = (
        "run_id",
        "status",
        "yahoo_row_count",
        "alpaca_row_count",
        "match_count",
        "break_count",
    )
    return dict(zip(columns, row))


def persist_equity_run(
    *,
    run_id: str,
    started_at: str,
    finished_at: str,
    status: str,
    yahoo_bars: pd.DataFrame,
    alpaca_bars: pd.DataFrame,
    reconciliation: pd.DataFrame,
    exceptions: pd.DataFrame,
    db_path: str = "reconciliation.duckdb",
) -> bool:
    """Atomically persist a complete equity run; return False if already stored."""
    match_count = int((reconciliation["status"] == "MATCH").sum())
    break_count = int((reconciliation["status"] == "BREAK").sum())
    bar_rows = [
        *_equity_bar_rows(yahoo_bars, "YAHOO", run_id),
        *_equity_bar_rows(alpaca_bars, "ALPACA", run_id),
    ]
    result_rows = _reconciliation_rows(reconciliation, run_id)
    exception_rows = _exception_rows(exceptions)

    with duckdb.connect(db_path) as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            exists = conn.execute(
                "SELECT 1 FROM pipeline_runs WHERE run_id = ?", [run_id]
            ).fetchone()
            if exists:
                conn.execute("ROLLBACK")
                return False

            conn.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, started_at, finished_at, status,
                    yahoo_row_count, alpaca_row_count, match_count, break_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    started_at,
                    finished_at,
                    status,
                    len(yahoo_bars),
                    len(alpaca_bars),
                    match_count,
                    break_count,
                ),
            )
            if bar_rows:
                conn.executemany(
                    """
                    INSERT INTO equity_bars_staging
                    (symbol, run_id, source, timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    bar_rows,
                )
            if result_rows:
                conn.executemany(
                    """
                    INSERT INTO equity_reconciliation_results
                    (run_id, symbol, timestamp, metric_name, source_a_value,
                     source_b_value, difference, is_discrepancy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    result_rows,
                )
            if exception_rows:
                conn.executemany(
                    """
                    INSERT INTO equity_exceptions
                    (exception_id, run_id, instrument_type, symbol, trade_date,
                     yahoo_close, alpaca_close, close_difference, severity,
                     status, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    exception_rows,
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return True
