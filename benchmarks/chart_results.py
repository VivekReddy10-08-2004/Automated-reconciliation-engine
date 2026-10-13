"""Render labeled SVG charts from committed benchmark CSV files."""

from __future__ import annotations

import csv
from pathlib import Path

from benchmarks.chart_svg import bar_chart, latency_chart

ROOT = Path(__file__).resolve().parent


def main() -> None:
    benchmark = ROOT / "results" / "benchmark_results.csv"
    rows = list(csv.DictReader(benchmark.open(newline="", encoding="utf-8")))
    reconciliation = [row for row in rows if row["benchmark"] == "reconciliation"]
    categories = [f"{int(row['rows']):,} rows" for row in reconciliation]
    bar_chart(
        ROOT / "charts" / "throughput.svg",
        title="Reconciliation throughput",
        subtitle="Two deterministic source frames · full outer comparison · one worker",
        categories=categories,
        values=[float(row["rows_per_second"]) for row in reconciliation],
        y_label="Processed rows per second",
        x_label="Input rows per source",
        value_suffix=" rows/s",
        footer="Measured with time.perf_counter; includes validation, merge, and classification",
    )
    bar_chart(
        ROOT / "charts" / "scaling.svg",
        title="Peak memory by reconciliation workload",
        subtitle="Same correctness workload at 100K and 1M rows per provider source",
        categories=categories,
        values=[float(row["peak_rss_mb"]) for row in reconciliation],
        y_label="Peak resident memory (MB)",
        x_label="Input rows per source",
        value_suffix=" MB",
        footer="Peak RSS from resource.getrusage; current implementation uses in-memory Pandas",
    )
    load_path = ROOT / "results" / "api_load_results.csv"
    load_rows = (
        list(csv.DictReader(load_path.open(newline="", encoding="utf-8")))
        if load_path.exists()
        else []
    )
    latency_chart(ROOT / "charts" / "latency.svg", load_rows)


if __name__ == "__main__":
    main()
