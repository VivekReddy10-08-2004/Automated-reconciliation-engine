"""Create a deterministic database for the API load test."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from reconciliation_engine.storage.duckdb import initialize_database


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=10_000)
    args = parser.parse_args()
    initialize_database(str(args.db))
    with duckdb.connect(str(args.db)) as connection:
        connection.execute(
            "INSERT INTO pipeline_runs (run_id, status, match_count, break_count) VALUES ('load-test', 'SUCCESS', ?, 0)",
            [args.rows],
        )
        connection.execute(
            """
            INSERT INTO equity_reconciliation_results
            SELECT 'load-test', 'S' || range::VARCHAR,
                   DATE '2000-01-01' + range::INTEGER, metric, 'YAHOO', 'ALPACA',
                   100.0, 100.0, 0.0, false, 'MATCH', NULL
            FROM range(?)
            CROSS JOIN (VALUES ('close'), ('volume')) AS metrics(metric)
            """,
            [args.rows],
        )
    print(args.db)


if __name__ == "__main__":
    main()
