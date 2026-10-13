"""Atomically publish a completed DuckDB copy for concurrent API readers."""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

import duckdb

from reconciliation_engine.security import restrict_file_permissions


def publish_snapshot(source: str | Path, destination: str | Path) -> Path:
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if source == destination:
        raise ValueError("snapshot destination must differ from source database")
    if not source.is_file():
        raise FileNotFoundError(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    temporary.unlink()
    try:
        with duckdb.connect() as target:
            escaped_source = str(source).replace("'", "''")
            escaped_temporary = str(temporary).replace("'", "''")
            target.execute(f"ATTACH '{escaped_source}' AS source_db (READ_ONLY)")
            target.execute(f"ATTACH '{escaped_temporary}' AS snapshot_db")
            target.execute("COPY FROM DATABASE source_db TO snapshot_db")
            target.execute("DETACH source_db")
            target.execute("CHECKPOINT snapshot_db")
            target.execute("DETACH snapshot_db")
        restrict_file_permissions(temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    print(publish_snapshot(args.source, args.destination))


if __name__ == "__main__":
    main()
