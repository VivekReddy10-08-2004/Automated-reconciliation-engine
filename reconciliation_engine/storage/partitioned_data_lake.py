"""Immutable partition archives for independently retryable work units."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from reconciliation_engine.scaling.partitions import WorkPartition
from reconciliation_engine.security import restrict_file_permissions


def partition_archive_path(
    partition: WorkPartition,
    *,
    layer: str,
    data_root: str | Path = "data",
) -> Path:
    if layer not in {"raw", "normalized"}:
        raise ValueError("layer must be raw or normalized")
    filename = "payload.parquet" if layer == "raw" else "data.parquet"
    return (
        Path(data_root)
        / "partitions"
        / layer
        / f"control_date={partition.control_date.isoformat()}"
        / f"instrument_type={partition.instrument_type}"
        / f"symbol_bucket={partition.symbol_bucket:05d}"
        / f"provider={partition.provider}"
        / f"partition_id={partition.partition_id}"
        / filename
    )


def write_partition_once(
    frame: pd.DataFrame,
    partition: WorkPartition,
    *,
    layer: str,
    data_root: str | Path = "data",
) -> Path:
    path = partition_archive_path(partition, layer=layer, data_root=data_root)
    if path.exists():
        raise FileExistsError(f"Partition archive already exists: {path}")
    archived = frame.copy()
    archived["run_id"] = partition.run_id
    archived["partition_id"] = partition.partition_id
    archived["provider"] = partition.provider
    archived["symbol_bucket"] = partition.symbol_bucket
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as connection:
        connection.from_df(archived).write_parquet(str(path))
    restrict_file_permissions(path)
    return path


def read_partition(
    partition: WorkPartition,
    *,
    layer: str,
    data_root: str | Path = "data",
) -> pd.DataFrame | None:
    path = partition_archive_path(partition, layer=layer, data_root=data_root)
    if not path.exists():
        return None
    with duckdb.connect() as connection:
        return connection.read_parquet(str(path)).df()
