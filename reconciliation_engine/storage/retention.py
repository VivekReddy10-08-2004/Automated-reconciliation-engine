"""Conservative retention planning for date-partitioned provider archives."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import shutil


@dataclass(frozen=True)
class RetentionCandidate:
    control_date: date
    path: Path
    bytes_on_disk: int


def plan_partition_retention(
    data_root: str | Path,
    *,
    before: date,
) -> tuple[RetentionCandidate, ...]:
    """List whole control-date partitions older than ``before`` without deleting."""
    partitions_root = Path(data_root).resolve() / "partitions"
    if not partitions_root.is_dir():
        return ()

    candidates: list[RetentionCandidate] = []
    seen: set[Path] = set()
    for layer in ("raw", "normalized"):
        layer_root = partitions_root / layer
        if not layer_root.is_dir():
            continue
        for path in layer_root.glob("control_date=*"):
            if not path.is_dir():
                continue
            try:
                control_date = date.fromisoformat(path.name.removeprefix("control_date="))
            except ValueError:
                continue
            resolved = path.resolve()
            if control_date >= before or resolved in seen:
                continue
            seen.add(resolved)
            size = sum(item.stat().st_size for item in resolved.rglob("*") if item.is_file())
            candidates.append(RetentionCandidate(control_date, resolved, size))
    return tuple(sorted(candidates, key=lambda item: (item.control_date, str(item.path))))


def apply_partition_retention(
    candidates: tuple[RetentionCandidate, ...],
    *,
    data_root: str | Path,
    dry_run: bool = True,
) -> tuple[Path, ...]:
    """Delete only validated date directories; dry-run is the safe default."""
    allowed_root = (Path(data_root).resolve() / "partitions")
    selected: list[Path] = []
    for candidate in candidates:
        target = candidate.path.resolve()
        if allowed_root not in target.parents:
            raise ValueError(f"Retention candidate escapes partition root: {target}")
        if not target.name.startswith("control_date="):
            raise ValueError(f"Retention candidate is not a control-date directory: {target}")
        selected.append(target)
    if not dry_run:
        for target in selected:
            if target.is_dir():
                shutil.rmtree(target)
    return tuple(selected)
