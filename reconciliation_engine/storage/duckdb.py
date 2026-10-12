import duckdb
import pandas as pd
from datetime import datetime, timezone

from reconciliation_engine.scaling.partitions import WorkPartition

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
            volume DOUBLE,
            PRIMARY KEY (run_id, source, symbol, timestamp),
            FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
        )
        """
    )
    conn.execute("INSERT INTO equity_bars_staging_v2 SELECT * FROM equity_bars_staging")
    conn.execute("DROP TABLE equity_bars_staging")
    conn.execute("ALTER TABLE equity_bars_staging_v2 RENAME TO equity_bars_staging")


def _migrate_reconciliation_results(conn: duckdb.DuckDBPyConnection) -> None:
    """Add provider-pair lineage to result rows created by older releases."""
    columns = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info('equity_reconciliation_results')"
        ).fetchall()
    }
    if "source_a" in columns:
        return
    conn.execute(
        """
        CREATE TABLE equity_reconciliation_results_v2 (
            run_id VARCHAR,
            symbol VARCHAR,
            timestamp TIMESTAMP,
            metric_name VARCHAR,
            source_a VARCHAR,
            source_b VARCHAR,
            source_a_value DOUBLE,
            source_b_value DOUBLE,
            difference DOUBLE,
            is_discrepancy BOOLEAN,
            PRIMARY KEY (
                run_id, symbol, timestamp, metric_name, source_a, source_b
            )
        )
        """
    )
    conn.execute(
        """
        INSERT INTO equity_reconciliation_results_v2
        SELECT run_id, symbol, timestamp, metric_name, 'YAHOO', 'ALPACA',
               source_a_value, source_b_value, difference, is_discrepancy
        FROM equity_reconciliation_results
        """
    )
    conn.execute("DROP TABLE equity_reconciliation_results")
    conn.execute(
        "ALTER TABLE equity_reconciliation_results_v2 "
        "RENAME TO equity_reconciliation_results"
    )

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
                massive_row_count BIGINT DEFAULT 0,
                match_count BIGINT,
                break_count BIGINT               
            )
            """
        )
        conn.execute(
            "ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS "
            "massive_row_count BIGINT DEFAULT 0"
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
                volume DOUBLE,
                PRIMARY KEY (run_id, source, symbol, timestamp),
                FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
            )
            """
        )
        _migrate_equity_staging_primary_key(conn)
        conn.execute(
            "ALTER TABLE equity_bars_staging ALTER COLUMN volume SET DATA TYPE DOUBLE"
        )

        #equity reconciliation results table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS equity_reconciliation_results (
                run_id VARCHAR,
                symbol VARCHAR,
                timestamp TIMESTAMP,
                metric_name VARCHAR,
                source_a VARCHAR,
                source_b VARCHAR,
                source_a_value DOUBLE,
                source_b_value DOUBLE,
                difference DOUBLE,
                is_discrepancy BOOLEAN,
                PRIMARY KEY (
                    run_id, symbol, timestamp, metric_name, source_a, source_b
                )
            )
        """
        )
        _migrate_reconciliation_results(conn)

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
                source_a VARCHAR,
                source_b VARCHAR,
                source_a_close DOUBLE,
                source_b_close DOUBLE,
                severity VARCHAR,
                priority VARCHAR,
                status VARCHAR,
                owner VARCHAR,
                reason_code VARCHAR,
                due_at TIMESTAMP,
                resolved_at TIMESTAMP,
                created_at TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
            )
            """
        )
        for definition in (
            "source_a VARCHAR",
            "source_b VARCHAR",
            "source_a_close DOUBLE",
            "source_b_close DOUBLE",
            "priority VARCHAR",
            "owner VARCHAR",
            "reason_code VARCHAR",
            "due_at TIMESTAMP",
            "resolved_at TIMESTAMP",
        ):
            conn.execute(
                f"ALTER TABLE equity_exceptions ADD COLUMN IF NOT EXISTS {definition}"
            )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS work_partitions (
                partition_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                control_date DATE NOT NULL,
                instrument_type VARCHAR NOT NULL,
                symbol_bucket INTEGER NOT NULL,
                provider VARCHAR NOT NULL,
                symbols_json VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                error_type VARCHAR,
                error_message VARCHAR,
                row_count BIGINT,
                UNIQUE (
                    run_id, provider, instrument_type, symbol_bucket
                )
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS provider_request_metrics (
                partition_id VARCHAR,
                run_id VARCHAR,
                provider VARCHAR,
                instrument_type VARCHAR,
                symbol_bucket INTEGER,
                requested_symbols INTEGER,
                returned_rows BIGINT,
                latency_ms DOUBLE,
                reused_archive BOOLEAN,
                outcome_status VARCHAR,
                error_type VARCHAR,
                recorded_at TIMESTAMP
            )
            """
        )
        conn.execute(
            "ALTER TABLE provider_request_metrics ADD COLUMN IF NOT EXISTS "
            "outcome_status VARCHAR"
        )
        conn.execute(
            "ALTER TABLE provider_request_metrics ADD COLUMN IF NOT EXISTS "
            "error_type VARCHAR"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quarantine_records (
                quarantine_id VARCHAR PRIMARY KEY,
                partition_id VARCHAR,
                run_id VARCHAR,
                provider VARCHAR,
                instrument_type VARCHAR,
                symbol_bucket INTEGER,
                reason VARCHAR,
                raw_archive_path VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
    restrict_file_permissions(db_path)


def register_work_partitions(
    partitions: tuple[WorkPartition, ...],
    db_path: str = "reconciliation.duckdb",
) -> None:
    """Create pending manifest rows without resetting previous attempts."""
    if not partitions:
        return
    rows = [
        (
            item.partition_id,
            item.run_id,
            item.control_date,
            item.instrument_type,
            item.symbol_bucket,
            item.provider,
            item.symbols_json,
            "PENDING",
        )
        for item in partitions
    ]
    with duckdb.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO work_partitions (
                partition_id, run_id, control_date, instrument_type,
                symbol_bucket, provider, symbols_json, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (partition_id) DO NOTHING
            """,
            rows,
        )


def mark_partition_running(
    partition_id: str,
    db_path: str = "reconciliation.duckdb",
) -> None:
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE work_partitions
            SET status = 'RUNNING', attempts = attempts + 1,
                started_at = ?, finished_at = NULL,
                error_type = NULL, error_message = NULL
            WHERE partition_id = ? AND status <> 'SUCCESS'
            """,
            [datetime.now(timezone.utc), partition_id],
        )


def finish_work_partition(
    partition_id: str,
    *,
    status: str,
    row_count: int = 0,
    error: Exception | None = None,
    db_path: str = "reconciliation.duckdb",
) -> None:
    if status not in {"SUCCESS", "FAILED"}:
        raise ValueError("Partition status must be SUCCESS or FAILED")
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE work_partitions
            SET status = ?, finished_at = ?, row_count = ?,
                error_type = ?, error_message = ?
            WHERE partition_id = ?
            """,
            [
                status,
                datetime.now(timezone.utc),
                row_count,
                type(error).__name__ if error else None,
                str(error)[:1000] if error else None,
                partition_id,
            ],
        )


def record_provider_metric(
    partition: WorkPartition,
    *,
    returned_rows: int,
    latency_ms: float,
    reused_archive: bool,
    outcome_status: str = "SUCCESS",
    error_type: str | None = None,
    db_path: str = "reconciliation.duckdb",
) -> None:
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO provider_request_metrics (
                partition_id, run_id, provider, instrument_type, symbol_bucket,
                requested_symbols, returned_rows, latency_ms, reused_archive,
                outcome_status, error_type, recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                partition.partition_id,
                partition.run_id,
                partition.provider,
                partition.instrument_type,
                partition.symbol_bucket,
                len(partition.symbols),
                returned_rows,
                latency_ms,
                reused_archive,
                outcome_status,
                error_type,
                datetime.now(timezone.utc),
            ],
        )


def record_quarantine(
    partition: WorkPartition,
    *,
    reason: str,
    raw_archive_path: str | None,
    db_path: str = "reconciliation.duckdb",
) -> None:
    from uuid import NAMESPACE_URL, uuid5

    quarantine_id = str(
        uuid5(NAMESPACE_URL, f"{partition.partition_id}|{reason[:500]}")
    )
    with duckdb.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO quarantine_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (quarantine_id) DO NOTHING
            """,
            [
                quarantine_id,
                partition.partition_id,
                partition.run_id,
                partition.provider,
                partition.instrument_type,
                partition.symbol_bucket,
                reason[:1000],
                raw_archive_path,
                datetime.now(timezone.utc),
            ],
        )

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
                source_a,
                source_b,
                source_a_value,
                source_b_value,
                difference,
                is_discrepancy
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            results,
        )


def _reconciliation_rows(df: pd.DataFrame, run_id: str) -> list[tuple]:
    results: list[tuple] = []
    for _, row in df.iterrows():
        is_pairwise = "source_a" in df.columns
        source_a = row["source_a"] if is_pairwise else "YAHOO"
        source_b = row["source_b"] if is_pairwise else "ALPACA"
        close_a = row["source_a_close"] if is_pairwise else row["yahoo_close"]
        close_b = row["source_b_close"] if is_pairwise else row["alpaca_close"]
        volume_a = row["source_a_volume"] if is_pairwise else row["yahoo_volume"]
        volume_b = row["source_b_volume"] if is_pairwise else row["alpaca_volume"]
        results.extend(
            [
            (
                run_id,
                row["symbol"],
                row["trade_date"],
                "close",
                source_a,
                source_b,
                close_a,
                close_b,
                row["close_difference"],
                row["status"] == "BREAK",
            ),
            (
                run_id,
                row["symbol"],
                row["trade_date"],
                "volume",
                source_a,
                source_b,
                volume_a,
                volume_b,
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
                source_a,
                source_b,
                source_a_close,
                source_b_close,
                close_difference,
                severity,
                priority,
                status,
                owner,
                reason_code,
                due_at,
                resolved_at,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                   massive_row_count,
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
        "massive_row_count",
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
    massive_bars: pd.DataFrame | None = None,
    db_path: str = "reconciliation.duckdb",
) -> bool:
    """Atomically persist a complete equity run; return False if already stored."""
    match_count = int((reconciliation["status"] == "MATCH").sum())
    break_count = int((reconciliation["status"] == "BREAK").sum())
    bar_rows = [
        *_equity_bar_rows(yahoo_bars, "YAHOO", run_id),
        *_equity_bar_rows(alpaca_bars, "ALPACA", run_id),
    ]
    if massive_bars is not None:
        bar_rows.extend(_equity_bar_rows(massive_bars, "MASSIVE", run_id))
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
                    yahoo_row_count, alpaca_row_count, massive_row_count,
                    match_count, break_count
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    started_at,
                    finished_at,
                    status,
                    len(yahoo_bars),
                    len(alpaca_bars),
                    len(massive_bars) if massive_bars is not None else 0,
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
                    (run_id, symbol, timestamp, metric_name, source_a, source_b,
                     source_a_value, source_b_value, difference, is_discrepancy)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    result_rows,
                )
            if exception_rows:
                conn.executemany(
                    """
                    INSERT INTO equity_exceptions
                    (exception_id, run_id, instrument_type, symbol, trade_date,
                     source_a, source_b, source_a_close, source_b_close,
                     close_difference, severity, priority, status, owner,
                     reason_code, due_at, resolved_at, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    exception_rows,
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return True
