from pathlib import Path
import tempfile
import unittest

import duckdb
from fastapi.testclient import TestClient

from reconciliation_engine.api.service import DatabaseBusyError, ReadOnlyDuckDB, create_app
from reconciliation_engine.storage.duckdb import initialize_database


TOKEN = "test-export-token"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def _seed_database(path: str) -> None:
    initialize_database(path)
    with duckdb.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, started_at, finished_at, status,
                yahoo_row_count, alpaca_row_count, massive_row_count,
                match_count, break_count
            ) VALUES
                ('run-new', '2026-10-10 20:00:00', '2026-10-10 20:01:00',
                 'SUCCESS_WITH_BREAKS', 1, 1, 1, 1, 1),
                ('run-old', '2026-10-09 20:00:00', '2026-10-09 20:01:00',
                 'SUCCESS', 1, 1, 1, 3, 0)
            """
        )
        result_rows = [
            ("run-new", "AAPL", "2026-10-10", "close", "YAHOO", "ALPACA", 100.0, 101.0, -1.0, True, "BREAK", "PRICE_DIFFERENCE"),
            ("run-new", "AAPL", "2026-10-10", "volume", "YAHOO", "ALPACA", 1000.0, 1100.0, -100.0, True, "BREAK", "PRICE_DIFFERENCE"),
            ("run-old", "MSFT", "2026-10-09", "close", "YAHOO", "MASSIVE", 200.0, 200.02, -0.02, False, "WARN", None),
            ("run-old", "MSFT", "2026-10-09", "volume", "YAHOO", "MASSIVE", 2000.0, 2001.0, -1.0, False, "WARN", None),
        ]
        connection.executemany(
            """
            INSERT INTO equity_reconciliation_results VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            result_rows,
        )
        connection.execute(
            """
            INSERT INTO equity_exceptions (
                exception_id, run_id, instrument_type, symbol, trade_date,
                source_a, source_b, source_a_close, source_b_close,
                close_difference, severity, priority, status, owner,
                reason_code, due_at, resolved_at, created_at
            ) VALUES (
                'exception-1', 'run-new', 'EQUITY', 'AAPL', '2026-10-10',
                'YAHOO', 'ALPACA', 100, 101, -1, 'BREAK', 'HIGH', 'OPEN',
                'UNASSIGNED', 'PRICE_DIFFERENCE', '2026-10-11', NULL,
                '2026-10-10 20:01:00'
            )
            """
        )


class ExportApiTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.directory.name) / "reconciliation.duckdb")
        _seed_database(self.db_path)
        self.client = TestClient(create_app(db_path=self.db_path, api_key=TOKEN))

    def tearDown(self):
        self.client.close()
        self.directory.cleanup()

    def test_health_is_public_but_data_requires_bearer_authentication(self):
        health = self.client.get("/health")
        unauthorized = self.client.get("/api/v1/runs")
        authorized = self.client.get("/api/v1/runs", headers=HEADERS)

        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["database"], "read-only")
        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(authorized.status_code, 200)

    def test_runs_are_paginated_and_one_run_has_partition_metadata(self):
        page = self.client.get(
            "/api/v1/runs?limit=1&offset=0", headers=HEADERS
        ).json()
        detail = self.client.get(
            "/api/v1/runs/run-new", headers=HEADERS
        )

        self.assertEqual(page["page"]["total"], 2)
        self.assertEqual(page["page"]["next_offset"], 1)
        self.assertEqual(page["items"][0]["run_id"], "run-new")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["status"], "SUCCESS_WITH_BREAKS")
        self.assertEqual(detail.json()["partitions"], [])
        self.assertEqual(
            self.client.get("/api/v1/runs/missing", headers=HEADERS).status_code,
            404,
        )

    def test_reconciliations_filter_by_symbol_date_status_and_run(self):
        response = self.client.get(
            "/api/v1/reconciliations",
            params={
                "symbol": "aapl",
                "trade_date": "2026-10-10",
                "status": "BREAK",
                "run_id": "run-new",
            },
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["page"]["total"], 1)
        self.assertEqual(payload["items"][0]["symbol"], "AAPL")
        self.assertEqual(payload["items"][0]["status"], "BREAK")
        self.assertEqual(payload["items"][0]["source_a_volume"], 1000.0)

    def test_exceptions_include_operational_fields_and_filters(self):
        response = self.client.get(
            "/api/v1/exceptions?severity=break&status=open&symbol=AAPL",
            headers=HEADERS,
        )

        self.assertEqual(response.status_code, 200)
        item = response.json()["items"][0]
        self.assertEqual(item["priority"], "HIGH")
        self.assertEqual(item["owner"], "UNASSIGNED")
        self.assertEqual(item["reason_code"], "PRICE_DIFFERENCE")

    def test_exports_csv_json_and_parquet_with_filtering(self):
        for output_format, content_type in (
            ("csv", "text/csv"),
            ("json", "application/json"),
            ("parquet", "application/vnd.apache.parquet"),
        ):
            with self.subTest(output_format=output_format):
                response = self.client.get(
                    "/api/v1/exports",
                    params={"format": output_format, "status": "WARN"},
                    headers=HEADERS,
                )
                self.assertEqual(response.status_code, 200)
                self.assertTrue(response.headers["content-type"].startswith(content_type))
                self.assertEqual(response.headers["x-export-row-count"], "1")
                self.assertIn("attachment", response.headers["content-disposition"])
                if output_format == "csv":
                    self.assertIn("MSFT", response.text)
                elif output_format == "json":
                    self.assertEqual(response.json()[0]["status"], "WARN")
                else:
                    self.assertTrue(response.content.startswith(b"PAR1"))

    def test_export_limit_requires_narrower_filters(self):
        client = TestClient(
            create_app(db_path=self.db_path, api_key=TOKEN, export_max_rows=1)
        )
        try:
            response = client.get("/api/v1/exports", headers=HEADERS)
        finally:
            client.close()
        self.assertEqual(response.status_code, 413)

    def test_empty_parquet_export_still_returns_a_valid_file(self):
        response = self.client.get(
            "/api/v1/exports?format=parquet&symbol=NVDA", headers=HEADERS
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-export-row-count"], "0")
        self.assertTrue(response.content.startswith(b"PAR1"))


class DuckDBConcurrencyTests(unittest.TestCase):
    def test_store_is_read_only_and_rejects_excess_concurrency(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "database.duckdb")
            with duckdb.connect(path) as connection:
                connection.execute("CREATE TABLE probe (id INTEGER)")
            store = ReadOnlyDuckDB(
                path, max_concurrent_queries=1, acquire_timeout_seconds=0.01
            )
            with store.connection() as connection:
                with self.assertRaises(duckdb.Error):
                    connection.execute("CREATE TABLE forbidden (id INTEGER)")
                with self.assertRaises(DatabaseBusyError):
                    store.rows("SELECT * FROM probe")


if __name__ == "__main__":
    unittest.main()
