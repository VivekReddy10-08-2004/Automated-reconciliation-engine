"""Local, immutable Parquet archives for reconciliation inputs and results."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final, Iterable

import duckdb
import pandas as pd

from security import restrict_file_permissions


DEFAULT_DATA_ROOT: Final = Path("data")
EQUITY_INSTRUMENT: Final = "equities"
VALID_LAYERS: Final = {"raw", "normalized"}
VALID_SOURCES: Final = {"YAHOO", "ALPACA"}

EQUITY_COLUMNS: Final = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
]
EQUITY_ARCHIVE_COLUMNS: Final = [*EQUITY_COLUMNS, "run_id"]
RECONCILIATION_COLUMNS: Final = [
    "symbol",
    "trade_date",
    "yahoo_close",
    "alpaca_close",
    "close_difference",
    "yahoo_volume",
    "alpaca_volume",
    "volume_difference",
    "status",
]
RECONCILIATION_ARCHIVE_COLUMNS: Final = [*RECONCILIATION_COLUMNS, "run_id"]

_SAFE_PARTITION_VALUE = re.compile(r"^[A-Za-z0-9._-]+$")


def _validate_partition_value(name: str, value: str) -> str:
    value = str(value)
    if not value or not _SAFE_PARTITION_VALUE.fullmatch(value):
        raise ValueError(
            f"{name} must contain only letters, numbers, dots, underscores, or hyphens"
        )
    return value


def _archive_path(
    layer: str,
    source: str,
    instrument_type: str,
    run_id: str,
    data_root: str | Path,
) -> Path:
    layer = layer.lower()
    source = source.upper()
    instrument_type = instrument_type.lower()

    if layer not in VALID_LAYERS:
        raise ValueError(f"layer must be one of {sorted(VALID_LAYERS)}")
    if source not in VALID_SOURCES:
        raise ValueError(f"source must be one of {sorted(VALID_SOURCES)}")

    _validate_partition_value("instrument_type", instrument_type)
    _validate_partition_value("run_id", run_id)
    filename = "payload.parquet" if layer == "raw" else "data.parquet"
    return (
        Path(data_root)
        / layer
        / source
        / instrument_type
        / f"run_id={run_id}"
        / filename
    )


def _with_run_id(frame: pd.DataFrame, run_id: str) -> pd.DataFrame:
    archived = frame.copy()
    if "run_id" in archived.columns:
        existing_ids = set(archived["run_id"].dropna().astype(str).unique())
        if existing_ids and existing_ids != {run_id}:
            raise ValueError("DataFrame run_id values do not match the archive path")
    archived["run_id"] = run_id
    return archived


def _write_parquet_once(frame: pd.DataFrame, path: Path) -> Path:
    if path.exists():
        raise FileExistsError(f"Archive already exists and will not be overwritten: {path}")

    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as connection:
        connection.from_df(frame).write_parquet(str(path))
    restrict_file_permissions(path)
    return path


def write_dataframe(
    frame: pd.DataFrame,
    source: str,
    instrument_type: str,
    run_id: str,
    *,
    layer: str,
    data_root: str | Path = DEFAULT_DATA_ROOT,
) -> Path:
    """Write one immutable provider DataFrame to its partitioned Parquet path."""
    source = source.upper()
    instrument_type = instrument_type.lower()
    run_id = _validate_partition_value("run_id", run_id)
    path = _archive_path(layer, source, instrument_type, run_id, data_root)
    return _write_parquet_once(_with_run_id(frame, run_id), path)


def _default_columns(layer: str, instrument_type: str) -> list[str]:
    if layer.lower() in VALID_LAYERS and instrument_type.lower() == EQUITY_INSTRUMENT:
        return EQUITY_ARCHIVE_COLUMNS.copy()
    return ["run_id"]


def read_dataframe(
    source: str,
    instrument_type: str,
    run_id: str,
    *,
    layer: str,
    data_root: str | Path = DEFAULT_DATA_ROOT,
    expected_columns: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Read an archived provider DataFrame, or return an empty expected schema."""
    path = _archive_path(layer, source, instrument_type, run_id, data_root)
    columns = list(expected_columns or _default_columns(layer, instrument_type))
    if not path.exists():
        return pd.DataFrame(columns=columns)

    with duckdb.connect() as connection:
        restored = connection.read_parquet(str(path)).df()
    if (
        layer.lower() == "normalized"
        and instrument_type.lower() == EQUITY_INSTRUMENT
        and "trade_date" in restored.columns
    ):
        restored["trade_date"] = pd.to_datetime(restored["trade_date"]).dt.date
    return restored


def normalize_equity_dataframe(frame: pd.DataFrame) -> pd.DataFrame:
    """Return the canonical equity schema used by reconciliation."""
    missing = [column for column in EQUITY_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Equity data is missing required columns: {missing}")

    normalized = frame[EQUITY_COLUMNS].copy()
    normalized["symbol"] = normalized["symbol"].astype("string")
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"]).dt.date
    for column in ["open", "high", "low", "close", "volume"]:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    return normalized


def archive_equity_source(
    frame: pd.DataFrame,
    source: str,
    run_id: str,
    *,
    data_root: str | Path = DEFAULT_DATA_ROOT,
) -> pd.DataFrame:
    """Archive raw and canonical snapshots, then return canonical equity data."""
    write_dataframe(
        frame,
        source,
        EQUITY_INSTRUMENT,
        run_id,
        layer="raw",
        data_root=data_root,
    )
    normalized = normalize_equity_dataframe(frame)
    write_dataframe(
        normalized,
        source,
        EQUITY_INSTRUMENT,
        run_id,
        layer="normalized",
        data_root=data_root,
    )
    return normalized


def _reconciliation_path(run_id: str, data_root: str | Path) -> Path:
    run_id = _validate_partition_value("run_id", run_id)
    return (
        Path(data_root)
        / "reconciliation"
        / EQUITY_INSTRUMENT
        / f"run_id={run_id}"
        / "results.parquet"
    )


def archive_reconciliation_results(
    frame: pd.DataFrame,
    run_id: str,
    *,
    data_root: str | Path = DEFAULT_DATA_ROOT,
) -> Path:
    """Archive the derived equity reconciliation result for later audit."""
    missing = [column for column in RECONCILIATION_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"Reconciliation data is missing required columns: {missing}")
    archived = _with_run_id(frame[RECONCILIATION_COLUMNS], run_id)
    return _write_parquet_once(archived, _reconciliation_path(run_id, data_root))


def read_reconciliation_results(
    run_id: str,
    *,
    data_root: str | Path = DEFAULT_DATA_ROOT,
) -> pd.DataFrame:
    """Read archived reconciliation results without contacting either provider."""
    path = _reconciliation_path(run_id, data_root)
    if not path.exists():
        return pd.DataFrame(columns=RECONCILIATION_ARCHIVE_COLUMNS)
    with duckdb.connect() as connection:
        restored = connection.read_parquet(str(path)).df()
    restored["trade_date"] = pd.to_datetime(restored["trade_date"]).dt.date
    return restored
