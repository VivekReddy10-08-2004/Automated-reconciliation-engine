"""Offline end-to-end demo: ingest, break, reconcile, recover, export, trace."""

from __future__ import annotations

import csv
import io
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from benchmarks.generate_data import synthetic_equity_bars
from reconciliation_engine.observability import (
    TelemetryConfig,
    build_telemetry_runtime,
    set_telemetry_for_testing,
)
from reconciliation_engine.reconciliation.equities import reconcile_equity_sources
from reconciliation_engine.storage.data_lake import (
    archive_equity_source,
    archive_reconciliation_results,
    read_reconciliation_results,
)
from reconciliation_engine.storage.duckdb import initialize_database, persist_equity_run


def main() -> None:
    output = Path("benchmarks/results/end_to_end_demo.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        database, lake = root / "demo.duckdb", root / "data"
        exporter = InMemorySpanExporter()
        config = TelemetryConfig(
            True, "demo", "0.1.0", "demo", "unused", "unused", 60_000
        )
        runtime = build_telemetry_runtime(
            config,
            span_exporter=exporter,
            metric_reader=InMemoryMetricReader(),
        )
        previous = set_telemetry_for_testing(runtime)
        try:
            initialize_database(str(database))
            yahoo = synthetic_equity_bars(1000)
            alpaca = synthetic_equity_bars(1000)
            alpaca.loc[42, "close"] += 2.0  # deliberate, explainable data break
            alpaca.loc[42, "high"] = alpaca.loc[42, "close"] + 0.5
            with runtime.span("demo.end_to_end"):
                yahoo = archive_equity_source(
                    yahoo, "YAHOO", "demo-run", data_root=lake
                )
                alpaca = archive_equity_source(
                    alpaca, "ALPACA", "demo-run", data_root=lake
                )
                reconciliation = reconcile_equity_sources(
                    {"YAHOO": yahoo, "ALPACA": alpaca}
                )
                archive_reconciliation_results(
                    reconciliation, "demo-run", data_root=lake
                )
                persist_equity_run(
                    run_id="demo-run",
                    started_at=datetime.now(timezone.utc).isoformat(),
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    status="SUCCESS_WITH_BREAKS",
                    yahoo_bars=yahoo,
                    alpaca_bars=alpaca,
                    reconciliation=reconciliation,
                    exceptions=pd.DataFrame(),
                    db_path=str(database),
                )
                recovered = read_reconciliation_results("demo-run", data_root=lake)
                stream = io.StringIO(newline="")
                writer = csv.DictWriter(stream, fieldnames=list(recovered.columns))
                writer.writeheader()
                writer.writerows(recovered.to_dict("records"))
                exported = stream.getvalue().encode()
                filename = "reconciliations-demo-run.csv"
            runtime.force_flush()
            report = {
                "run_id": "demo-run",
                "ingested_rows": len(yahoo) + len(alpaca),
                "reconciliation_rows": len(reconciliation),
                "breaks": int((reconciliation["status"] == "BREAK").sum()),
                "recovered_rows": len(recovered),
                "export_filename": filename,
                "export_bytes": len(exported),
                "trace_spans": len(exporter.get_finished_spans()),
                "passed": len(recovered) == 1000
                and int((reconciliation["status"] == "BREAK").sum()) == 1,
            }
        finally:
            set_telemetry_for_testing(previous)
            runtime.shutdown()
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
