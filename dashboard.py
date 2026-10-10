"""Show the latest reconciliation run in a clear, static terminal dashboard."""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
from rich.columns import Columns
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table


DEFAULT_DATABASE = Path("reconciliation.duckdb")
MAX_DIFFERENCES = 5


@dataclass(frozen=True)
class DashboardData:
    """Data needed for one dashboard screen."""

    latest_run: dict[str, Any]
    largest_differences: list[dict[str, Any]]
    open_exceptions: list[dict[str, Any]]


def _fetch_rows(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[Any] | None = None,
) -> list[dict[str, Any]]:
    """Run a SELECT and return rows labelled with their column names."""
    result = connection.execute(query, parameters or [])
    column_names = [column[0] for column in result.description]
    return [dict(zip(column_names, row)) for row in result.fetchall()]


def load_dashboard_data(db_path: str | Path = DEFAULT_DATABASE) -> DashboardData | None:
    """Read the latest run and its operator-facing details from DuckDB."""
    with duckdb.connect(str(db_path), read_only=True) as connection:
        latest_runs = _fetch_rows(
            connection,
            """
            SELECT
                run_id,
                started_at,
                status,
                yahoo_row_count,
                alpaca_row_count,
                match_count,
                break_count
            FROM pipeline_runs
            ORDER BY started_at DESC NULLS LAST
            LIMIT 1
            """,
        )

        if not latest_runs:
            return None

        latest_run = latest_runs[0]
        run_id = latest_run["run_id"]

        largest_differences = _fetch_rows(
            connection,
            """
            SELECT
                symbol,
                CAST(timestamp AS DATE) AS trade_date,
                source_a_value AS yahoo_close,
                source_b_value AS alpaca_close,
                difference AS close_difference
            FROM equity_reconciliation_results
            WHERE run_id = ?
              AND metric_name = 'close'
              AND is_discrepancy = TRUE
              AND isfinite(difference)
            ORDER BY abs(difference) DESC NULLS LAST
            LIMIT ?
            """,
            [run_id, MAX_DIFFERENCES],
        )

        open_exceptions = _fetch_rows(
            connection,
            """
            SELECT
                symbol,
                trade_date,
                yahoo_close,
                alpaca_close,
                close_difference,
                severity
            FROM equity_exceptions
            WHERE run_id = ?
              AND upper(status) = 'OPEN'
            ORDER BY abs(close_difference) DESC NULLS LAST
            """,
            [run_id],
        )

    return DashboardData(latest_run, largest_differences, open_exceptions)


def _format_number(value: Any) -> str:
    """Format database values consistently for terminal display."""
    if value is None:
        return "MISSING"
    if isinstance(value, float) and math.isnan(value):
        return "MISSING"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return f"{value:,.4f}"
    return str(value)


def _metric_panel(title: str, metrics: list[tuple[str, Any, str]]) -> Panel:
    grid = Table.grid(expand=True, padding=(0, 2))
    grid.add_column(style="dim")
    grid.add_column(justify="right")

    for label, value, style in metrics:
        grid.add_row(label, f"[{style}]{_format_number(value)}[/{style}]")

    return Panel(grid, title=title, border_style="bright_blue", expand=True)


def _largest_differences_table(rows: list[dict[str, Any]]) -> Table:
    table = Table(
        title=f"Top {MAX_DIFFERENCES} Close Breaks",
        title_style="bold",
        header_style="bold bright_cyan",
        border_style="cyan",
        expand=True,
    )
    table.add_column("Symbol", style="bold")
    table.add_column("Date")
    table.add_column("Yahoo", justify="right")
    table.add_column("Alpaca", justify="right")
    table.add_column("Difference", justify="right", style="bold red")

    if not rows:
        table.add_row("No close breaks", "", "", "", "")
        return table

    for row in rows:
        table.add_row(
            _format_number(row["symbol"]),
            _format_number(row["trade_date"]),
            _format_number(row["yahoo_close"]),
            _format_number(row["alpaca_close"]),
            _format_number(row["close_difference"]),
        )
    return table


def _open_exceptions_table(rows: list[dict[str, Any]]) -> Table:
    table = Table(
        title="Open Exceptions from Latest Run",
        title_style="bold",
        header_style="bold bright_magenta",
        border_style="magenta",
        expand=True,
    )
    table.add_column("Symbol", style="bold")
    table.add_column("Trade date")
    table.add_column("Yahoo", justify="right")
    table.add_column("Alpaca", justify="right")
    table.add_column("Difference", justify="right")
    table.add_column("Severity", style="bold yellow")

    if not rows:
        table.add_row("No open exceptions", "", "", "", "", "")
        return table

    for row in rows:
        table.add_row(
            _format_number(row["symbol"]),
            _format_number(row["trade_date"]),
            _format_number(row["yahoo_close"]),
            _format_number(row["alpaca_close"]),
            _format_number(row["close_difference"]),
            _format_number(row["severity"]),
        )
    return table


def render_dashboard(data: DashboardData) -> Group:
    """Build a readable, one-screen Rich dashboard."""
    run = data.latest_run
    status_style = "green" if str(run["status"]).startswith("SUCCESS") else "red"

    run_header = Panel(
        f"[bold]Run ID:[/bold] {run['run_id']}\n"
        f"[bold]Started:[/bold] {_format_number(run['started_at'])}\n"
        f"[bold]Status:[/bold] [{status_style}]{run['status']}[/{status_style}]",
        title="Latest Reconciliation Run",
        border_style=status_style,
        padding=(1, 2),
    )

    source_counts = _metric_panel(
        "Source Rows",
        [
            ("Yahoo", run["yahoo_row_count"], "bright_yellow"),
            ("Alpaca", run["alpaca_row_count"], "bright_blue"),
        ],
    )
    result_counts = _metric_panel(
        "Reconciliation Results",
        [
            ("MATCH", run["match_count"], "bold green"),
            ("BREAK", run["break_count"], "bold red"),
        ],
    )

    return Group(
        run_header,
        Columns([source_counts, result_counts], equal=True, expand=True),
        _largest_differences_table(data.largest_differences),
        _open_exceptions_table(data.open_exceptions),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DATABASE,
        help="DuckDB file to read (default: reconciliation.duckdb)",
    )
    args = parser.parse_args()

    dashboard_data = load_dashboard_data(args.db)
    if dashboard_data is None:
        Console().print("[yellow]No reconciliation runs are stored yet.[/yellow]")
        return

    Console().print(render_dashboard(dashboard_data))


if __name__ == "__main__":
    main()
