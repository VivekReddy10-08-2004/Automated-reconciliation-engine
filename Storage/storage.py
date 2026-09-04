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
            )
            """
        )

        #equity bars table
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS equity_bars (
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
def save_equity_bars(df: pd.DataFrame, source: str, run_id: str, db_path: str = "reconciliation.duckdb") -> None:
    
    
def save_reconciliation_results(df: pd.DataFrame, run_id: str, db_path: str = "reconciliation.duckdb") -> None: