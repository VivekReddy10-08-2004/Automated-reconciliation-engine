"""Bounded, checkpointed execution of canonical equity-provider partitions."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
import time

import pandas as pd

from reconciliation_engine.ingestion.providers import EquityProviderAdapter
from reconciliation_engine.observability import log_event
from reconciliation_engine.reconciliation.equities import validate_equity_bars
from reconciliation_engine.scaling.partitions import WorkPartition, build_work_partitions
from reconciliation_engine.storage.data_lake import EQUITY_COLUMNS, normalize_equity_dataframe
from reconciliation_engine.storage.duckdb import (
    finish_work_partition,
    mark_partition_running,
    record_provider_metric,
    record_quarantine,
    register_work_partitions,
)
from reconciliation_engine.storage.partitioned_data_lake import (
    partition_archive_path,
    read_partition,
    write_partition_once,
)


@dataclass(frozen=True)
class PartitionOutcome:
    partition: WorkPartition
    frame: pd.DataFrame | None
    latency_ms: float
    reused_archive: bool
    error: Exception | None = None
    raw_archive_path: str | None = None


class PartitionExecutionError(RuntimeError):
    pass


def _canonical_from_archive(frame: pd.DataFrame) -> pd.DataFrame:
    return frame[EQUITY_COLUMNS].copy()


def _execute_partition(
    partition: WorkPartition,
    adapter: EquityProviderAdapter,
    start: datetime,
    end: datetime,
    data_root: str | Path,
) -> PartitionOutcome:
    normalized = read_partition(partition, layer="normalized", data_root=data_root)
    if normalized is not None:
        return PartitionOutcome(
            partition, _canonical_from_archive(normalized), 0.0, True
        )

    raw_path = partition_archive_path(partition, layer="raw", data_root=data_root)
    latency_ms = 0.0
    try:
        raw = read_partition(partition, layer="raw", data_root=data_root)
        reused = raw is not None
        if raw is None:
            started = time.monotonic()
            try:
                raw = adapter.fetch(partition.symbols, start, end)
            finally:
                latency_ms = (time.monotonic() - started) * 1000
            write_partition_once(raw, partition, layer="raw", data_root=data_root)

        canonical = normalize_equity_dataframe(raw)
        validate_equity_bars(canonical)
        write_partition_once(
            canonical, partition, layer="normalized", data_root=data_root
        )
        return PartitionOutcome(
            partition,
            canonical,
            latency_ms,
            reused,
            raw_archive_path=str(raw_path),
        )
    except Exception as error:
        return PartitionOutcome(
            partition,
            None,
            latency_ms,
            raw_path.exists(),
            error,
            str(raw_path) if raw_path.exists() else None,
        )


def ingest_equity_partitions(
    *,
    run_id: str,
    control_date: date,
    symbols: tuple[str, ...],
    start: datetime,
    end: datetime,
    adapters: dict[str, EquityProviderAdapter],
    db_path: str,
    data_root: str | Path,
    bucket_count: int = 64,
    max_workers: int = 8,
) -> dict[str, pd.DataFrame]:
    """Execute provider/bucket work with bounded concurrency and checkpoints."""
    if not 1 <= max_workers <= 64:
        raise ValueError("max_workers must be between 1 and 64")
    partitions = build_work_partitions(
        run_id=run_id,
        control_date=control_date,
        instrument_type="EQUITY",
        symbols=symbols,
        providers=tuple(adapters),
        bucket_count=bucket_count,
    )
    register_work_partitions(partitions, db_path)
    for partition in partitions:
        mark_partition_running(partition.partition_id, db_path)

    outcomes: list[PartitionOutcome] = []
    with ThreadPoolExecutor(
        max_workers=min(max_workers, len(partitions)),
        thread_name_prefix="equity-ingestion",
    ) as executor:
        futures = {
            executor.submit(
                _execute_partition,
                partition,
                adapters[partition.provider],
                start,
                end,
                data_root,
            ): partition
            for partition in partitions
        }
        for future in as_completed(futures):
            outcomes.append(future.result())

    failures: list[PartitionOutcome] = []
    provider_frames: dict[str, list[pd.DataFrame]] = {
        provider: [] for provider in adapters
    }
    for outcome in outcomes:
        if outcome.error is not None or outcome.frame is None:
            failures.append(outcome)
            finish_work_partition(
                outcome.partition.partition_id,
                status="FAILED",
                error=outcome.error,
                db_path=db_path,
            )
            record_provider_metric(
                outcome.partition,
                returned_rows=0,
                latency_ms=outcome.latency_ms,
                reused_archive=outcome.reused_archive,
                outcome_status="FAILED",
                error_type=type(outcome.error).__name__,
                db_path=db_path,
            )
            record_quarantine(
                outcome.partition,
                reason=str(outcome.error),
                raw_archive_path=outcome.raw_archive_path,
                db_path=db_path,
            )
            log_event(
                "partition_failed",
                partition_id=outcome.partition.partition_id,
                run_id=outcome.partition.run_id,
                provider=outcome.partition.provider,
                instrument_type=outcome.partition.instrument_type,
                symbol_bucket=outcome.partition.symbol_bucket,
                error_type=type(outcome.error).__name__,
            )
            continue
        provider_frames[outcome.partition.provider].append(outcome.frame)
        finish_work_partition(
            outcome.partition.partition_id,
            status="SUCCESS",
            row_count=len(outcome.frame),
            db_path=db_path,
        )
        record_provider_metric(
            outcome.partition,
            returned_rows=len(outcome.frame),
            latency_ms=outcome.latency_ms,
            reused_archive=outcome.reused_archive,
            db_path=db_path,
        )
        log_event(
            "partition_completed",
            partition_id=outcome.partition.partition_id,
            run_id=outcome.partition.run_id,
            provider=outcome.partition.provider,
            instrument_type=outcome.partition.instrument_type,
            symbol_bucket=outcome.partition.symbol_bucket,
            returned_rows=len(outcome.frame),
            latency_ms=round(outcome.latency_ms, 3),
            reused_archive=outcome.reused_archive,
        )

    if failures:
        failed_ids = ", ".join(item.partition.partition_id for item in failures)
        raise PartitionExecutionError(f"Failed ingestion partitions: {failed_ids}")

    return {
        provider: pd.concat(frames, ignore_index=True)
        if frames
        else pd.DataFrame(columns=EQUITY_COLUMNS)
        for provider, frames in provider_frames.items()
    }
