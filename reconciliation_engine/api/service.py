"""Read-only FastAPI service for runs, reconciliations, exceptions, and exports."""

from __future__ import annotations

from contextlib import contextmanager
import csv
from datetime import date, datetime, timezone
import io
import json
import os
from pathlib import Path
import secrets
import tempfile
import threading
from typing import Literal

import duckdb
from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import pandas as pd

from reconciliation_engine.observability import log_event


DEFAULT_QUERY_CONCURRENCY = 4
DEFAULT_EXPORT_MAX_ROWS = 50_000
MAX_PAGE_SIZE = 500


class DatabaseBusyError(RuntimeError):
    pass


class ReadOnlyDuckDB:
    """Short-lived read-only connections protected by a process-local bound."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        max_concurrent_queries: int = DEFAULT_QUERY_CONCURRENCY,
        acquire_timeout_seconds: float = 2.0,
    ) -> None:
        if not 1 <= max_concurrent_queries <= 32:
            raise ValueError("max_concurrent_queries must be between 1 and 32")
        if acquire_timeout_seconds <= 0:
            raise ValueError("acquire_timeout_seconds must be positive")
        self.db_path = Path(db_path)
        self.max_concurrent_queries = max_concurrent_queries
        self.acquire_timeout_seconds = acquire_timeout_seconds
        self._slots = threading.BoundedSemaphore(max_concurrent_queries)

    @contextmanager
    def connection(self):
        acquired = self._slots.acquire(timeout=self.acquire_timeout_seconds)
        if not acquired:
            raise DatabaseBusyError("Read query capacity is currently exhausted")
        try:
            with duckdb.connect(str(self.db_path), read_only=True) as connection:
                yield connection
        finally:
            self._slots.release()

    def rows(self, sql: str, parameters: list[object] | None = None) -> list[dict]:
        with self.connection() as connection:
            cursor = connection.execute(sql, parameters or [])
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def scalar(self, sql: str, parameters: list[object] | None = None) -> object:
        with self.connection() as connection:
            row = connection.execute(sql, parameters or []).fetchone()
            return row[0] if row else None


RECONCILIATION_CTE = """
WITH reconciliations AS (
    SELECT
        run_id,
        symbol,
        CAST(timestamp AS DATE) AS trade_date,
        source_a,
        source_b,
        max(source_a_value) FILTER (WHERE metric_name = 'close') AS source_a_close,
        max(source_b_value) FILTER (WHERE metric_name = 'close') AS source_b_close,
        max(difference) FILTER (WHERE metric_name = 'close') AS close_difference,
        max(source_a_value) FILTER (WHERE metric_name = 'volume') AS source_a_volume,
        max(source_b_value) FILTER (WHERE metric_name = 'volume') AS source_b_volume,
        max(difference) FILTER (WHERE metric_name = 'volume') AS volume_difference,
        coalesce(
            max(status) FILTER (WHERE metric_name = 'close'),
            CASE WHEN bool_or(is_discrepancy) THEN 'BREAK' ELSE 'MATCH' END
        ) AS status,
        max(break_reason) FILTER (WHERE metric_name = 'close') AS break_reason
    FROM equity_reconciliation_results
    GROUP BY run_id, symbol, CAST(timestamp AS DATE), source_a, source_b
)
"""


def _pagination(limit: int, offset: int, total: int) -> dict[str, int | None]:
    next_offset = offset + limit if offset + limit < total else None
    return {
        "limit": limit,
        "offset": offset,
        "total": total,
        "next_offset": next_offset,
    }


def _reconciliation_filters(
    *,
    symbol: str | None,
    trade_date: date | None,
    start_date: date | None,
    end_date: date | None,
    reconciliation_status: str | None,
    run_id: str | None,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    parameters: list[object] = []
    for clause, value in (
        ("symbol = ?", symbol.upper() if symbol else None),
        ("trade_date = ?", trade_date),
        ("trade_date >= ?", start_date),
        ("trade_date <= ?", end_date),
        ("status = ?", reconciliation_status),
        ("run_id = ?", run_id),
    ):
        if value is not None:
            clauses.append(clause)
            parameters.append(value)
    return (" WHERE " + " AND ".join(clauses) if clauses else "", parameters)


def _json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _export_payload(rows: list[dict], output_format: str) -> tuple[bytes, str, str]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    columns = list(rows[0]) if rows else [
        "run_id", "symbol", "trade_date", "source_a", "source_b",
        "source_a_close", "source_b_close", "close_difference",
        "source_a_volume", "source_b_volume", "volume_difference",
        "status", "break_reason",
    ]
    if output_format == "json":
        return (
            json.dumps(rows, default=_json_default, separators=(",", ":")).encode(),
            "application/json",
            f"reconciliations-{timestamp}.json",
        )
    if output_format == "csv":
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
        return stream.getvalue().encode(), "text/csv", f"reconciliations-{timestamp}.csv"

    frame = pd.DataFrame(rows, columns=columns)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temporary:
        path = Path(temporary.name)
    try:
        with duckdb.connect() as connection:
            connection.from_df(frame).write_parquet(str(path))
        return path.read_bytes(), "application/vnd.apache.parquet", f"reconciliations-{timestamp}.parquet"
    finally:
        path.unlink(missing_ok=True)


def create_app(
    *,
    db_path: str | Path = "reconciliation.duckdb",
    api_key: str | None = None,
    max_concurrent_queries: int = DEFAULT_QUERY_CONCURRENCY,
    export_max_rows: int = DEFAULT_EXPORT_MAX_ROWS,
) -> FastAPI:
    if export_max_rows <= 0:
        raise ValueError("export_max_rows must be positive")
    store = ReadOnlyDuckDB(
        db_path,
        max_concurrent_queries=max_concurrent_queries,
    )
    bearer = HTTPBearer(auto_error=False)
    app = FastAPI(
        title="Reconciliation Export API",
        version="1.0.0",
        description="Authenticated read-only access to curated reconciliation data.",
    )
    app.state.store = store

    def authenticate(
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    ) -> None:
        expected = api_key or os.getenv("RECONCILIATION_EXPORT_API_KEY")
        if not expected:
            raise HTTPException(status_code=503, detail="API authentication is not configured")
        if (
            credentials is None
            or credentials.scheme.lower() != "bearer"
            or not secrets.compare_digest(credentials.credentials, expected)
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or missing bearer token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    def query_rows(sql: str, parameters: list[object] | None = None) -> list[dict]:
        try:
            return store.rows(sql, parameters)
        except DatabaseBusyError as error:
            raise HTTPException(status_code=503, detail=str(error)) from None
        except (duckdb.Error, OSError):
            raise HTTPException(status_code=503, detail="Reconciliation database is unavailable") from None

    def query_scalar(sql: str, parameters: list[object] | None = None) -> object:
        try:
            return store.scalar(sql, parameters)
        except DatabaseBusyError as error:
            raise HTTPException(status_code=503, detail=str(error)) from None
        except (duckdb.Error, OSError):
            raise HTTPException(status_code=503, detail="Reconciliation database is unavailable") from None

    @app.get("/health", tags=["operations"])
    def health(response: Response) -> dict:
        try:
            query_scalar("SELECT 1")
        except HTTPException:
            response.status_code = 503
            return {"status": "unhealthy", "database": "unavailable"}
        return {
            "status": "healthy",
            "database": "read-only",
            "max_concurrent_queries": store.max_concurrent_queries,
        }

    @app.get("/api/v1/runs", dependencies=[Depends(authenticate)], tags=["runs"])
    def list_runs(
        limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ) -> dict:
        total = int(query_scalar("SELECT count(*) FROM pipeline_runs") or 0)
        items = query_rows(
            """
            SELECT run_id, created_at, started_at, finished_at, status,
                   yahoo_row_count, alpaca_row_count, massive_row_count,
                   match_count, break_count
            FROM pipeline_runs
            ORDER BY started_at DESC NULLS LAST, run_id
            LIMIT ? OFFSET ?
            """,
            [limit, offset],
        )
        return {"items": items, "page": _pagination(limit, offset, total)}

    @app.get("/api/v1/runs/{run_id}", dependencies=[Depends(authenticate)], tags=["runs"])
    def get_run(run_id: str) -> dict:
        items = query_rows(
            """
            SELECT run_id, created_at, started_at, finished_at, status,
                   yahoo_row_count, alpaca_row_count, massive_row_count,
                   match_count, break_count
            FROM pipeline_runs WHERE run_id = ?
            """,
            [run_id],
        )
        if not items:
            raise HTTPException(status_code=404, detail="Run not found")
        partition_summary = query_rows(
            """
            SELECT provider, status, count(*) AS partition_count,
                   coalesce(sum(row_count), 0) AS row_count,
                   coalesce(sum(attempts), 0) AS attempts
            FROM work_partitions WHERE run_id = ?
            GROUP BY provider, status ORDER BY provider, status
            """,
            [run_id],
        )
        return {**items[0], "partitions": partition_summary}

    @app.get(
        "/api/v1/reconciliations",
        dependencies=[Depends(authenticate)],
        tags=["reconciliation"],
    )
    def list_reconciliations(
        symbol: str | None = Query(None, pattern=r"^[A-Za-z][A-Za-z0-9.-]{0,14}$"),
        trade_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        reconciliation_status: Literal["MATCH", "WARN", "BREAK"] | None = Query(None, alias="status"),
        run_id: str | None = None,
        limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ) -> dict:
        if start_date and end_date and start_date > end_date:
            raise HTTPException(status_code=422, detail="start_date must not exceed end_date")
        where, parameters = _reconciliation_filters(
            symbol=symbol,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            reconciliation_status=reconciliation_status,
            run_id=run_id,
        )
        total = int(query_scalar(RECONCILIATION_CTE + "SELECT count(*) FROM reconciliations" + where, parameters) or 0)
        items = query_rows(
            RECONCILIATION_CTE
            + "SELECT * FROM reconciliations"
            + where
            + " ORDER BY trade_date DESC, symbol, source_a, source_b LIMIT ? OFFSET ?",
            [*parameters, limit, offset],
        )
        return {"items": items, "page": _pagination(limit, offset, total)}

    @app.get("/api/v1/exceptions", dependencies=[Depends(authenticate)], tags=["exceptions"])
    def list_exceptions(
        symbol: str | None = Query(None, pattern=r"^[A-Za-z][A-Za-z0-9.-]{0,14}$"),
        severity: str | None = None,
        exception_status: str | None = Query(None, alias="status"),
        run_id: str | None = None,
        limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ) -> dict:
        clauses: list[str] = []
        parameters: list[object] = []
        for clause, value in (
            ("upper(symbol) = ?", symbol.upper() if symbol else None),
            ("severity = ?", severity.upper() if severity else None),
            ("status = ?", exception_status.upper() if exception_status else None),
            ("run_id = ?", run_id),
        ):
            if value is not None:
                clauses.append(clause)
                parameters.append(value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        total = int(query_scalar("SELECT count(*) FROM equity_exceptions" + where, parameters) or 0)
        items = query_rows(
            """
            SELECT exception_id, run_id, instrument_type, symbol, trade_date,
                   source_a, source_b, source_a_close, source_b_close,
                   close_difference, severity, priority, status, owner,
                   reason_code, due_at, resolved_at, created_at
            FROM equity_exceptions
            """
            + where
            + " ORDER BY created_at DESC, exception_id LIMIT ? OFFSET ?",
            [*parameters, limit, offset],
        )
        return {"items": items, "page": _pagination(limit, offset, total)}

    @app.get("/api/v1/exports", dependencies=[Depends(authenticate)], tags=["exports"])
    def export_reconciliations(
        output_format: Literal["csv", "json", "parquet"] = Query("csv", alias="format"),
        symbol: str | None = Query(None, pattern=r"^[A-Za-z][A-Za-z0-9.-]{0,14}$"),
        trade_date: date | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        reconciliation_status: Literal["MATCH", "WARN", "BREAK"] | None = Query(None, alias="status"),
        run_id: str | None = None,
    ) -> StreamingResponse:
        if start_date and end_date and start_date > end_date:
            raise HTTPException(status_code=422, detail="start_date must not exceed end_date")
        where, parameters = _reconciliation_filters(
            symbol=symbol,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            reconciliation_status=reconciliation_status,
            run_id=run_id,
        )
        rows = query_rows(
            RECONCILIATION_CTE
            + "SELECT * FROM reconciliations"
            + where
            + " ORDER BY trade_date DESC, symbol, source_a, source_b LIMIT ?",
            [*parameters, export_max_rows + 1],
        )
        if len(rows) > export_max_rows:
            raise HTTPException(
                status_code=413,
                detail=f"Export exceeds the configured {export_max_rows}-row limit; add filters",
            )
        payload, media_type, filename = _export_payload(rows, output_format)
        log_event(
            "reconciliation_export_completed",
            format=output_format,
            row_count=len(rows),
            filtered=bool(where),
        )
        return StreamingResponse(
            iter([payload]),
            media_type=media_type,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "X-Export-Row-Count": str(len(rows)),
            },
        )

    return app
