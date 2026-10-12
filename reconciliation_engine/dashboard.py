"""Render a read-only control room for batch and real-time reconciliation."""

from __future__ import annotations

import argparse
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
import math
from pathlib import Path
import time

import duckdb
import pandas as pd
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
import zmq

from reconciliation_engine.realtime import get_scored_endpoint, validate_local_endpoint
from reconciliation_engine.realtime.anomaly_scorer import validate_annotated_tick


MAX_REFRESHES_PER_SECOND = 2
MIN_REFRESH_SECONDS = 1 / MAX_REFRESHES_PER_SECOND
DEFAULT_FEED_STALE_SECONDS = 5.0
DEFAULT_ANOMALY_WINDOW_SECONDS = 60.0


@dataclass(frozen=True)
class DashboardData:
    latest_run: pd.DataFrame
    run_trend: pd.DataFrame
    top_breaks: pd.DataFrame
    repeat_offenders: pd.DataFrame
    exception_status: pd.DataFrame


@dataclass(frozen=True)
class BatchDashboardState:
    data: DashboardData | None
    warning: str | None = None


@dataclass(frozen=True)
class RealtimeDashboardData:
    status: str
    latest_tick: dict[str, object] | None
    ticks_received: int
    recent_anomaly_count: int
    latest_anomaly_reason: str | None
    warning: str | None = None


def _display(value: object) -> str:
    if pd.isna(value):
        return "MISSING"
    if isinstance(value, float):
        return f"{value:.4f}"
    return str(value)


def load_dashboard_data(db_path: str | Path, limit: int = 10) -> DashboardData:
    """Load curated control metrics through a read-only database connection."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    with duckdb.connect(str(db_path), read_only=True) as conn:
        latest_run = conn.execute(
            """
            SELECT run_id, started_at, finished_at, status,
                   yahoo_row_count, alpaca_row_count, massive_row_count,
                   match_count, break_count
            FROM pipeline_runs
            ORDER BY started_at DESC NULLS LAST
            LIMIT 1
            """
        ).fetchdf()
        latest_run["warn_count"] = 0

        if not latest_run.empty:
            has_dbt_fact = conn.execute(
                """
                SELECT count(*)
                FROM information_schema.tables
                WHERE table_schema = 'main'
                  AND table_name = 'fct_equity_reconciliation'
                """
            ).fetchone()[0]
            if has_dbt_fact:
                status_counts = conn.execute(
                    """
                    SELECT count(*) FILTER (WHERE status = 'MATCH'),
                           count(*) FILTER (WHERE status = 'WARN'),
                           count(*) FILTER (WHERE status = 'BREAK')
                    FROM main.fct_equity_reconciliation
                    WHERE run_id = ?
                    """,
                    [latest_run.iloc[0]["run_id"]],
                ).fetchone()
                if sum(status_counts) > 0:
                    latest_run.loc[latest_run.index[0], "match_count"] = status_counts[0]
                    latest_run.loc[latest_run.index[0], "warn_count"] = status_counts[1]
                    latest_run.loc[latest_run.index[0], "break_count"] = status_counts[2]
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
            SELECT symbol, timestamp, metric_name, source_a, source_b,
                   source_a_value,
                   source_b_value, difference
            FROM equity_reconciliation_results
            WHERE run_id = (SELECT run_id FROM latest) AND is_discrepancy
            ORDER BY CASE WHEN isfinite(difference) THEN 1 ELSE 0 END,
                     abs(difference) DESC
            LIMIT ?
            """,
            [min(limit, 5)],
        ).fetchdf()
        repeat_offenders = conn.execute(
            """
            SELECT symbol, metric_name, source_a, source_b,
                   count(DISTINCT run_id) AS affected_runs,
                   max(CASE WHEN isfinite(difference) THEN abs(difference) END)
                       AS max_abs_difference
            FROM equity_reconciliation_results
            WHERE is_discrepancy
            GROUP BY symbol, metric_name, source_a, source_b
            ORDER BY affected_runs DESC, max_abs_difference DESC
            LIMIT ?
            """,
            [limit],
        ).fetchdf()
        exception_status = conn.execute(
            """
            SELECT status,
                   count(*) AS exception_count,
                   count(*) FILTER (WHERE owner = 'UNASSIGNED') AS unassigned_count,
                   count(*) FILTER (
                       WHERE status = 'OPEN' AND due_at < current_timestamp
                   ) AS overdue_count,
                   max(
                       CASE WHEN status = 'OPEN'
                            THEN date_diff('hour', created_at, current_timestamp)
                       END
                   ) AS oldest_open_hours
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


def load_batch_dashboard_state(
    db_path: str | Path,
    limit: int = 10,
) -> BatchDashboardState:
    """Convert database failures into a panel warning for fault isolation."""
    try:
        data = load_dashboard_data(db_path, limit)
    except (duckdb.Error, OSError) as error:
        return BatchDashboardState(
            data=None,
            warning=f"Batch data unavailable ({type(error).__name__})",
        )
    if data.latest_run.empty:
        return BatchDashboardState(data=data, warning="Batch data unavailable: no runs")
    return BatchDashboardState(data=data)


class RealtimeStateTracker:
    """Accumulate live feed state without owning or writing to any transport."""

    def __init__(
        self,
        *,
        stale_after_seconds: float = DEFAULT_FEED_STALE_SECONDS,
        anomaly_window_seconds: float = DEFAULT_ANOMALY_WINDOW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if stale_after_seconds <= 0 or not math.isfinite(stale_after_seconds):
            raise ValueError("stale_after_seconds must be a positive finite number")
        if anomaly_window_seconds <= 0 or not math.isfinite(anomaly_window_seconds):
            raise ValueError("anomaly_window_seconds must be a positive finite number")
        self.stale_after_seconds = stale_after_seconds
        self.anomaly_window_seconds = anomaly_window_seconds
        self._clock = clock
        self._started_at = clock()
        self._last_tick_at: float | None = None
        self._latest_tick: dict[str, object] | None = None
        self._ticks_received = 0
        self._anomaly_times: deque[float] = deque()
        self._latest_anomaly_reason: str | None = None
        self._warning: str | None = None

    def observe(self, message: object) -> None:
        tick = validate_annotated_tick(message)
        now = self._clock()
        self._latest_tick = dict(tick)
        self._last_tick_at = now
        self._ticks_received += 1
        self._warning = None
        if tick["is_anomaly"]:
            self._anomaly_times.append(now)
            self._latest_anomaly_reason = str(tick["reason"])

    def record_warning(self, warning: str) -> None:
        self._warning = warning

    def snapshot(self) -> RealtimeDashboardData:
        now = self._clock()
        cutoff = now - self.anomaly_window_seconds
        while self._anomaly_times and self._anomaly_times[0] < cutoff:
            self._anomaly_times.popleft()

        if self._last_tick_at is None:
            elapsed = now - self._started_at
            if elapsed >= self.stale_after_seconds:
                status = "UNAVAILABLE"
                warning = "Real-time feed unavailable: no annotated ticks received"
            elif self._warning:
                status = "DEGRADED"
                warning = self._warning
            else:
                status = "WAITING"
                warning = "Waiting for the first annotated tick"
        elif now - self._last_tick_at >= self.stale_after_seconds:
            status = "STALE"
            warning = "Real-time feed unavailable: latest tick is stale"
        elif self._warning:
            status = "DEGRADED"
            warning = self._warning
        else:
            status = "LIVE"
            warning = None

        return RealtimeDashboardData(
            status=status,
            latest_tick=self._latest_tick,
            ticks_received=self._ticks_received,
            recent_anomaly_count=len(self._anomaly_times),
            latest_anomaly_reason=self._latest_anomaly_reason,
            warning=warning,
        )


class RealtimeFeedSubscriber:
    """Read annotated ticks from a ZeroMQ SUB socket without publishing."""

    def __init__(
        self,
        endpoint: str,
        *,
        stale_after_seconds: float = DEFAULT_FEED_STALE_SECONDS,
        anomaly_window_seconds: float = DEFAULT_ANOMALY_WINDOW_SECONDS,
        context: zmq.Context | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        validate_local_endpoint(endpoint)
        self._tracker = RealtimeStateTracker(
            stale_after_seconds=stale_after_seconds,
            anomaly_window_seconds=anomaly_window_seconds,
            clock=clock,
        )
        self._owns_context = context is None
        self._context = context or zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.LINGER, 0)
        self._socket.setsockopt(zmq.SUBSCRIBE, b"")
        self._socket.connect(endpoint)

    def poll(self, max_messages: int = 1000) -> RealtimeDashboardData:
        """Drain currently available messages and return a bounded snapshot."""
        if max_messages <= 0:
            raise ValueError("max_messages must be positive")
        for _ in range(max_messages):
            try:
                message = self._socket.recv_json(flags=zmq.NOBLOCK)
            except zmq.Again:
                break
            except zmq.ZMQError:
                self._tracker.record_warning("Real-time feed unavailable (ZeroMQ error)")
                break
            try:
                self._tracker.observe(message)
            except ValueError:
                self._tracker.record_warning("Real-time feed degraded: invalid tick ignored")
        return self._tracker.snapshot()

    def close(self) -> None:
        self._socket.close(linger=0)
        if self._owns_context:
            self._context.term()

    def __enter__(self) -> RealtimeFeedSubscriber:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def unavailable_realtime_data(reason: str) -> RealtimeDashboardData:
    return RealtimeDashboardData(
        status="UNAVAILABLE",
        latest_tick=None,
        ticks_received=0,
        recent_anomaly_count=0,
        latest_anomaly_reason=None,
        warning=reason,
    )


def bounded_refresh_interval(seconds: float) -> float:
    """Enforce the task 20 limit of at most two dashboard refreshes per second."""
    if not math.isfinite(seconds):
        raise ValueError("refresh interval must be finite")
    return max(seconds, MIN_REFRESH_SECONDS)


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


def _render_batch_panel(state: BatchDashboardState) -> Panel:
    if state.data is None:
        content: object = Text(state.warning or "Batch data unavailable", style="yellow")
    else:
        data = state.data
        latest = _table(
            "Latest batch run",
            data.latest_run,
            [
                ("run_id", "Run ID"),
                ("status", "Status"),
                ("yahoo_row_count", "Yahoo"),
                ("alpaca_row_count", "Alpaca"),
                ("massive_row_count", "Massive"),
                ("match_count", "MATCH"),
                ("warn_count", "WARN"),
                ("break_count", "BREAK"),
            ],
        )
        breaks = _table(
            "Top five breaks",
            data.top_breaks.head(5),
            [
                ("symbol", "Instrument"),
                ("timestamp", "Date"),
                ("metric_name", "Metric"),
                ("source_a", "Provider A"),
                ("source_a_value", "Value A"),
                ("source_b", "Provider B"),
                ("source_b_value", "Value B"),
                ("difference", "Difference"),
            ],
        )
        exceptions = _table(
            "Exception operations",
            data.exception_status,
            [
                ("status", "Status"),
                ("exception_count", "Count"),
                ("unassigned_count", "Unassigned"),
                ("overdue_count", "Overdue"),
                ("oldest_open_hours", "Oldest (h)"),
            ],
        )
        parts: list[object] = []
        if state.warning:
            parts.append(Text(state.warning, style="yellow"))
        parts.extend((latest, breaks, exceptions))
        content = Group(*parts)
    return Panel(content, title="Batch reconciliation", border_style="blue")


def _render_realtime_panel(data: RealtimeDashboardData) -> Panel:
    table = Table.grid(expand=True)
    table.add_column(style="bold", ratio=1)
    table.add_column(ratio=3)
    table.add_row("Feed status", data.status)
    if data.latest_tick is None:
        table.add_row("Most recent tick", "No tick received")
        table.add_row("Latency", "MISSING")
    else:
        tick = data.latest_tick
        table.add_row(
            "Most recent tick",
            f"{tick['symbol']} @ {_display(tick['price'])} ({tick['exchange_timestamp']})",
        )
        table.add_row("Latency", f"{_display(tick['latency_ms'])} ms")
        table.add_row("Price change", f"{_display(tick['pct_change'])}%")
    table.add_row("Ticks received", str(data.ticks_received))
    table.add_row("Recent anomalies", str(data.recent_anomaly_count))
    table.add_row("Latest anomaly", data.latest_anomaly_reason or "None observed")
    if data.warning:
        table.add_row("Warning", Text(data.warning, style="yellow"))
    return Panel(table, title="Real-time anomaly feed", border_style="magenta")


def render_control_room(
    batch: BatchDashboardState,
    realtime: RealtimeDashboardData,
) -> Panel:
    """Render two independent panels on one read-only control-room screen."""
    columns = Table.grid(expand=True)
    columns.add_column(ratio=1)
    columns.add_column(ratio=1)
    columns.add_row(_render_batch_panel(batch), _render_realtime_panel(realtime))
    return Panel(columns, title="Automated reconciliation control room")


def render_dashboard(data: DashboardData) -> Panel:
    """Backward-compatible static renderer used by existing callers."""
    return render_control_room(
        BatchDashboardState(data=data),
        unavailable_realtime_data("Real-time feed not connected"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="reconciliation.duckdb", help="DuckDB database path")
    parser.add_argument("--limit", type=int, default=10, help="Rows per analytics query")
    parser.add_argument(
        "--refresh",
        type=float,
        default=0,
        metavar="SECONDS",
        help="Refresh continuously; values below 0.5 seconds are bounded to 0.5",
    )
    parser.add_argument(
        "--realtime-endpoint",
        default=None,
        help="Annotated ZeroMQ endpoint; defaults to REALTIME_SCORED_ZMQ_ENDPOINT",
    )
    parser.add_argument(
        "--feed-stale-seconds",
        type=float,
        default=DEFAULT_FEED_STALE_SECONDS,
    )
    parser.add_argument(
        "--anomaly-window-seconds",
        type=float,
        default=DEFAULT_ANOMALY_WINDOW_SECONDS,
    )
    args = parser.parse_args()

    feed: RealtimeFeedSubscriber | None = None
    feed_error: RealtimeDashboardData | None = None
    try:
        endpoint = args.realtime_endpoint or get_scored_endpoint()
        feed = RealtimeFeedSubscriber(
            endpoint,
            stale_after_seconds=args.feed_stale_seconds,
            anomaly_window_seconds=args.anomaly_window_seconds,
        )
    except (ValueError, zmq.ZMQError) as error:
        feed_error = unavailable_realtime_data(
            f"Real-time feed unavailable ({type(error).__name__})"
        )

    def snapshot() -> Panel:
        batch = load_batch_dashboard_state(args.db, args.limit)
        realtime = feed.poll() if feed else feed_error
        assert realtime is not None
        return render_control_room(batch, realtime)

    try:
        if args.refresh <= 0:
            Console().print(snapshot())
            return

        interval = bounded_refresh_interval(args.refresh)
        with Live(
            refresh_per_second=MAX_REFRESHES_PER_SECOND,
            screen=True,
        ) as live:
            while True:
                live.update(snapshot())
                time.sleep(interval)
    except KeyboardInterrupt:
        return
    finally:
        if feed:
            feed.close()


if __name__ == "__main__":
    main()
