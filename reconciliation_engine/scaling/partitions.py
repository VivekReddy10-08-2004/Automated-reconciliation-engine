"""Stable work partitions for independently retryable ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
from uuid import NAMESPACE_URL, uuid5


DEFAULT_SYMBOL_BUCKETS = 64


@dataclass(frozen=True)
class WorkPartition:
    partition_id: str
    run_id: str
    control_date: date
    instrument_type: str
    symbol_bucket: int
    provider: str
    symbols: tuple[str, ...]

    @property
    def symbols_json(self) -> str:
        return json.dumps(self.symbols, separators=(",", ":"))


def symbol_bucket(symbol: str, bucket_count: int = DEFAULT_SYMBOL_BUCKETS) -> int:
    """Map a symbol to a stable bucket without Python's randomized hash()."""
    if bucket_count <= 0:
        raise ValueError("bucket_count must be positive")
    digest = hashlib.sha256(symbol.strip().upper().encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % bucket_count


def build_work_partitions(
    *,
    run_id: str,
    control_date: date,
    instrument_type: str,
    symbols: tuple[str, ...],
    providers: tuple[str, ...],
    bucket_count: int = DEFAULT_SYMBOL_BUCKETS,
) -> tuple[WorkPartition, ...]:
    """Build deterministic non-empty bucket/provider partitions."""
    normalized_symbols = tuple(sorted({symbol.strip().upper() for symbol in symbols}))
    normalized_providers = tuple(dict.fromkeys(p.strip().upper() for p in providers))
    if not normalized_symbols:
        raise ValueError("At least one symbol is required")
    if not normalized_providers:
        raise ValueError("At least one provider is required")

    buckets: dict[int, list[str]] = {}
    for symbol in normalized_symbols:
        buckets.setdefault(symbol_bucket(symbol, bucket_count), []).append(symbol)

    partitions: list[WorkPartition] = []
    instrument = instrument_type.strip().upper()
    for bucket in sorted(buckets):
        bucket_symbols = tuple(buckets[bucket])
        for provider in normalized_providers:
            logical_key = (
                f"{run_id}|{control_date.isoformat()}|{instrument}|"
                f"{bucket:05d}|{provider}"
            )
            partitions.append(
                WorkPartition(
                    partition_id=str(uuid5(NAMESPACE_URL, logical_key)),
                    run_id=run_id,
                    control_date=control_date,
                    instrument_type=instrument,
                    symbol_bucket=bucket,
                    provider=provider,
                    symbols=bucket_symbols,
                )
            )
    return tuple(partitions)
