"""Render reconciliation breaks and operational analytics from DuckDB."""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table


@dataclass(frozen=True)
class DashboardData:
    latest_run: pd.DataFrame
    run_trend: pd.DataFrame
    top_breaks: pd.DataFrame
    repeat_offenders: pd.DataFrame
    exception_status: pd.DataFrame


def _display(value: object) -> str:
    if pd.isna(value):
        return "MISSING"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def load_dashboard_data(db_path: str | Path, limit: int = 10) -> DashboardData:
    """Load curated control metrics through a read-only database connection."""
    with duckdb.connect(str(db_path), read_only=True) as conn:
        latest_run = conn.execute(
            """
            SELECT run_id, started_at, finished_at, status,
                   yahoo_row_count, alpaca_row_count, match_count, break_count
            FROM pipeline_runs
            ORDER BY started_at DESC NULLS LAST
            LIMIT 1
            """
        ).fetchdf()
        run_trend = conn.execute(
            """
            SELECT run_id, started_at, status, match_count, break_count,
                   CASE WHEN match_count + break_count = 0 THEN 0
                        ELSE 100.0 * break_count / (match_count + break_count)
                   END AS break_rate_pct
            FROM pipeline_runs
            ORDER BY started_at DESC NULLS LAST
            LIMIT ?
            """,
            [limit],
        ).fetchdf()
        top_breaks = conn.execute(
            """
            WITH latest AS (
                SELECT run_id FROM pipeline_runs
                ORDER BY started_at DESC NULLS LAST LIMIT 1
            )
            SELECT symbol, timestamp, metric_name, source_a_value,
                   source_b_value, difference
            FROM equity_reconciliation_results
            WHERE run_id = (SELECT run_id FROM latest) AND is_discrepancy
            ORDER BY CASE WHEN isfinite(difference) THEN 1 ELSE 0 END,
                     abs(difference) DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()
        repeat_offenders = conn.execute(
            """
            SELECT symbol, metric_name, count(DISTINCT run_id) AS affected_runs,
                   max(CASE WHEN isfinite(difference) THEN abs(difference) END)
                       AS max_abs_difference
            FROM equity_reconciliation_results
            WHERE is_discrepancy
            GROUP BY symbol, metric_name
            ORDER BY affected_runs DESC, max_abs_difference DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()
        exception_status = conn.execute(
            """
            SELECT status, count(*) AS exception_count
            FROM equity_exceptions
            GROUP BY status
            ORDER BY exception_count DESC
            """
        ).fetchdf()

    return DashboardData(
        latest_run=latest_run,
        run_trend=run_trend,
        top_breaks=top_breaks,
        repeat_offenders=repeat_offenders,
        exception_status=exception_status,
    )


def _table(title: str, frame: pd.DataFrame, columns: list[tuple[str, str]]) -> Table:
    table = Table(title=title, expand=True)
    for _, label in columns:
        table.add_column(label)
    if frame.empty:
        table.add_row(*(["No data"] + [""] * (len(columns) - 1)))
        return table
    for row in frame.itertuples(index=False):
        values = row._asdict()
        table.add_row(*(_display(values[key]) for key, _ in columns))
    return table


def render_dashboard(data: DashboardData) -> Group:
    latest = _table(
        "Latest run",
        data.latest_run,
        [
            ("run_id", "Run ID"), ("started_at", "Started"), ("status", "Status"),
            ("match_count", "Matches"), ("break_count", "Breaks"),
        ],
    )
    trend = _table(
        "Break-rate trend",
        data.run_trend,
        [
            ("started_at", "Started"), ("status", "Status"),
            ("match_count", "Matches"), ("break_count", "Breaks"),
            ("break_rate_pct", "Break rate %"),
        ],
    )
    breaks = _table(
        "Largest mismatches in latest run",
        data.top_breaks,
        [
            ("symbol", "Instrument"), ("timestamp", "Date"),
            ("metric_name", "Metric"), ("source_a_value", "Source A"),
            ("source_b_value", "Source B"), ("difference", "Difference"),
        ],
    )
    offenders = _table(
        "Repeat offenders",
        data.repeat_offenders,
        [
            ("symbol", "Instrument"), ("metric_name", "Metric"),
            ("affected_runs", "Affected runs"),
            ("max_abs_difference", "Max abs difference"),
        ],
    )
    exceptions = _table(
        "Exception queue",
        data.exception_status,
        [("status", "Status"), ("exception_count", "Count")],
    )
    return Group(Panel(latest, title="Reconciliation control room"), breaks, trend, offenders, exceptions)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="reconciliation.duckdb", help="DuckDB database path")
    parser.add_argument("--limit", type=int, default=10, help="Rows per analytics panel")
    parser.add_argument(
        "--refresh", type=float, default=0, metavar="SECONDS",
        help="Refresh continuously; omit for a one-time snapshot",
    )
    args = parser.parse_args()

    if args.refresh <= 0:
        from rich.console import Console

        Console().print(render_dashboard(load_dashboard_data(args.db, args.limit)))
        return

    with Live(refresh_per_second=2, screen=True) as live:
        while True:
            live.update(render_dashboard(load_dashboard_data(args.db, args.limit)))
            time.sleep(max(args.refresh, 0.5))


if __name__ == "__main__":
    main()
