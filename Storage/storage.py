import pandas as pd
import duckdb

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
                PRIMARY KEY (run_id, source, timestamp),
                FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
            )
            """
        )

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

    with duckdb.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO equity_bars_staging
            (symbol, run_id, source, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            selected_df.values.tolist()
        )

    
def save_reconciliation_results(
    df: pd.DataFrame,
    run_id: str,
    db_path: str = "reconciliation.duckdb"
) -> None:

    results = []

    for _, row in df.iterrows():
        results.extend([
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
        ])

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
            results
        )
