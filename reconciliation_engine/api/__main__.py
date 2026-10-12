"""Run the authenticated read-only reconciliation API."""

from __future__ import annotations

import argparse

import uvicorn

from reconciliation_engine.api.service import create_app
from reconciliation_engine.security import load_export_api_key


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="reconciliation.duckdb")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--max-concurrent-queries", type=int, default=4)
    parser.add_argument("--export-max-rows", type=int, default=50_000)
    arguments = parser.parse_args()
    app = create_app(
        db_path=arguments.db,
        api_key=load_export_api_key(),
        max_concurrent_queries=arguments.max_concurrent_queries,
        export_max_rows=arguments.export_max_rows,
    )
    uvicorn.run(app, host=arguments.host, port=arguments.port)


if __name__ == "__main__":
    main()
