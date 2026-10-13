"""Labeled SVG chart primitives for benchmark evidence."""

from __future__ import annotations

from html import escape
from pathlib import Path

WIDTH = 900
HEIGHT = 520
PLOT_LEFT = 100
PLOT_RIGHT = 865
PLOT_TOP = 105
PLOT_BOTTOM = 405
COLORS = ("#38bdf8", "#a78bfa", "#fb7185")


def _number(value: float, decimals: int = 0) -> str:
    return f"{value:,.{decimals}f}"


def _document(title: str, subtitle: str, body: list[str], footer: str) -> str:
    content = [
        (
            '<svg xmlns="http://www.w3.org/2000/svg" role="img" '
            f'aria-labelledby="title desc" viewBox="0 0 {WIDTH} {HEIGHT}">'
        ),
        f'<title id="title">{escape(title)}</title>',
        f'<desc id="desc">{escape(subtitle)} {escape(footer)}</desc>',
        '<rect width="900" height="520" rx="18" fill="#0f172a"/>',
        (
            "<style>text{font-family:Inter,ui-sans-serif,system-ui,sans-serif}"
            ".title{font-size:24px;font-weight:700;fill:#f8fafc}"
            ".subtitle{font-size:13px;fill:#94a3b8}"
            ".axis{font-size:12px;fill:#cbd5e1}"
            ".value{font-size:12px;font-weight:700;fill:#f8fafc}"
            ".label{font-size:13px;font-weight:600;fill:#e2e8f0}"
            ".grid{stroke:#334155;stroke-width:1}"
            ".baseline{stroke:#94a3b8;stroke-width:1.5}</style>"
        ),
        f'<text class="title" x="32" y="38">{escape(title)}</text>',
        f'<text class="subtitle" x="32" y="64">{escape(subtitle)}</text>',
        *body,
        f'<text class="subtitle" x="32" y="498">{escape(footer)}</text>',
        "</svg>",
    ]
    return "\n".join(content) + "\n"


def _axes(maximum: float, y_label: str, *, ticks: int = 4) -> list[str]:
    elements: list[str] = []
    plot_height = PLOT_BOTTOM - PLOT_TOP
    for index in range(ticks + 1):
        value = maximum * index / ticks
        y = PLOT_BOTTOM - plot_height * index / ticks
        elements.extend(
            [
                (
                    f'<line class="grid" x1="{PLOT_LEFT}" y1="{y:.1f}" '
                    f'x2="{PLOT_RIGHT}" y2="{y:.1f}"/>'
                ),
                (
                    f'<text class="axis" text-anchor="end" x="{PLOT_LEFT - 12}" '
                    f'y="{y + 4:.1f}">{escape(_number(value))}</text>'
                ),
            ]
        )
    elements.extend(
        [
            (
                f'<line class="baseline" x1="{PLOT_LEFT}" y1="{PLOT_BOTTOM}" '
                f'x2="{PLOT_RIGHT}" y2="{PLOT_BOTTOM}"/>'
            ),
            (
                f'<text class="label" text-anchor="middle" '
                f'transform="translate(24 {(PLOT_TOP + PLOT_BOTTOM) / 2}) rotate(-90)">'
                f"{escape(y_label)}</text>"
            ),
        ]
    )
    return elements


def bar_chart(
    path: Path,
    *,
    title: str,
    subtitle: str,
    categories: list[str],
    values: list[float],
    y_label: str,
    x_label: str,
    value_suffix: str,
    footer: str,
    decimals: int = 0,
) -> None:
    maximum = max(values) * 1.2 or 1.0
    elements = _axes(maximum, y_label)
    slot = (PLOT_RIGHT - PLOT_LEFT) / len(values)
    bar_width = min(150, slot * 0.52)
    plot_height = PLOT_BOTTOM - PLOT_TOP
    for index, (category, value) in enumerate(zip(categories, values)):
        x = PLOT_LEFT + slot * index + (slot - bar_width) / 2
        height = plot_height * value / maximum
        y = PLOT_BOTTOM - height
        elements.extend(
            [
                (
                    f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" '
                    f'height="{height:.1f}" rx="6" fill="{COLORS[0]}"/>'
                ),
                (
                    f'<text class="value" text-anchor="middle" '
                    f'x="{x + bar_width / 2:.1f}" y="{y - 9:.1f}">'
                    f"{escape(_number(value, decimals) + value_suffix)}</text>"
                ),
                (
                    f'<text class="axis" text-anchor="middle" '
                    f'x="{x + bar_width / 2:.1f}" y="{PLOT_BOTTOM + 24}">'
                    f"{escape(category)}</text>"
                ),
            ]
        )
    elements.append(
        f'<text class="label" text-anchor="middle" '
        f'x="{(PLOT_LEFT + PLOT_RIGHT) / 2}" y="{PLOT_BOTTOM + 54}">'
        f"{escape(x_label)}</text>"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_document(title, subtitle, elements, footer), encoding="utf-8")


def latency_chart(path: Path, rows: list[dict[str, str]]) -> None:
    series = (("p50", "p50_ms"), ("p95", "p95_ms"), ("p99", "p99_ms"))
    values = [[float(row[key]) for _, key in series] for row in rows]
    maximum = max(value for group in values for value in group) * 1.18
    elements = _axes(maximum, "Response latency (milliseconds)")
    slot = (PLOT_RIGHT - PLOT_LEFT) / len(rows)
    bar_width = min(36, slot * 0.2)
    plot_height = PLOT_BOTTOM - PLOT_TOP
    for group_index, (row, group) in enumerate(zip(rows, values)):
        group_width = bar_width * len(series)
        start_x = PLOT_LEFT + slot * group_index + (slot - group_width) / 2
        for series_index, value in enumerate(group):
            x = start_x + series_index * bar_width
            height = plot_height * value / maximum
            y = PLOT_BOTTOM - height
            elements.extend(
                [
                    (
                        f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width - 3:.1f}" '
                        f'height="{height:.1f}" rx="3" fill="{COLORS[series_index]}"/>'
                    ),
                    (
                        f'<text class="value" text-anchor="middle" '
                        f'transform="translate({x + (bar_width - 3) / 2:.1f} {y - 7:.1f}) '
                        f'rotate(-55)">{_number(value)}</text>'
                    ),
                ]
            )
        center = PLOT_LEFT + slot * group_index + slot / 2
        client_label = "client" if int(float(row["concurrency"])) == 1 else "clients"
        elements.append(
            f'<text class="axis" text-anchor="middle" x="{center:.1f}" '
            f'y="{PLOT_BOTTOM + 24}">{int(float(row["concurrency"]))} {client_label}</text>'
        )
    for index, (label, _) in enumerate(series):
        x = 600 + index * 82
        elements.extend(
            [
                (
                    f'<rect x="{x}" y="78" width="12" height="12" rx="2" '
                    f'fill="{COLORS[index]}"/>'
                ),
                f'<text class="axis" x="{x + 18}" y="89">{label}</text>',
            ]
        )
    elements.append(
        f'<text class="label" text-anchor="middle" '
        f'x="{(PLOT_LEFT + PLOT_RIGHT) / 2}" y="{PLOT_BOTTOM + 54}">'
        "Concurrent authenticated clients</text>"
    )
    path.write_text(
        _document(
            "Export API latency under concurrent load",
            "10,000-row DuckDB fixture · 20 authenticated requests at each concurrency level",
            elements,
            "Metric: end-to-end HTTP response time · Error rate: 0% for all four runs",
        ),
        encoding="utf-8",
    )
