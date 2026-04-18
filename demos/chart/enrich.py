"""Build the enriched demo JSON for delta-chart-gallery from source SQL."""
import json
import re

BASE = 'a:/delta-forge/delta-forge-demos/demos/delta/delta-chart-gallery'
OUT = 'a:/delta-forge/delta-forge-docs/enriched/demo/delta-chart-gallery.json'


def substitute_vars(sql: str) -> str:
    sql = sql.replace('{{zone_name}}', 'demo')
    sql = sql.replace('{{data_path}}', '~/delta-data')
    return sql


def strip_grants_and_detects(sql: str) -> str:
    out = []
    for line in sql.split('\n'):
        if line.strip().startswith('DETECT SCHEMA FOR TABLE'):
            continue
        if line.strip().startswith('GRANT ADMIN ON TABLE'):
            continue
        out.append(line)
    return '\n'.join(out)


def drop_orphan_trailing_banners(sql: str) -> str:
    """Drop comment-only trailing blocks left behind after removing DETECT/GRANT.

    Any region of the form ``-- --...\\n-- <title>\\n-- --...\\n`` (or its
    single-line-title variant) that is followed only by whitespace until EOF
    is dropped, because it annotates content that no longer exists.
    """
    lines = sql.split('\n')
    # Find the last non-blank, non-comment line
    last_sql_idx = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped and not stripped.startswith('--'):
            last_sql_idx = i
    if last_sql_idx < 0:
        return sql
    # Everything after last_sql_idx that is purely comments/blanks can be dropped
    return '\n'.join(lines[: last_sql_idx + 1])


def strip_asserts(sql: str) -> str:
    return '\n'.join(
        line for line in sql.split('\n')
        if not re.match(r'^\s*ASSERT\s', line)
    )


def collapse_blanks(sql: str) -> str:
    return re.sub(r'\n{3,}', '\n\n', sql)


def strip_leading_banner(sql: str) -> str:
    """Drop the first contiguous comment block (the source file's top banner).

    That banner duplicates the enricher's own ``-- DEMO:`` header and the
    ``-- SETUP``/``-- QUERIES``/``-- CLEANUP`` section labels, so it's noise
    in the runnable syntax doc.
    """
    lines = sql.split('\n')
    i = 0
    # Skip any leading blank lines
    while i < len(lines) and not lines[i].strip():
        i += 1
    # Skip leading comment/blank lines (the banner block)
    while i < len(lines) and (lines[i].strip().startswith('--') or not lines[i].strip()):
        i += 1
    return '\n'.join(lines[i:])


def strip_banner_sections(sql: str) -> str:
    """Collapse the ``-- === / -- Title / -- === / -- descriptions`` sub-banners
    that appear between queries down to a single ``-- Title`` comment.
    """
    out_lines = []
    lines = sql.split('\n')
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        # Detect a banner: a line of '-- ===...' followed by '-- <title>'
        # followed by another '-- ===...', then optional descriptive comments
        is_divider = stripped.startswith('-- =') and set(stripped.replace('-- ', '')) <= {'='}
        if is_divider and i + 2 < len(lines):
            title_line = lines[i + 1].strip()
            next_divider = lines[i + 2].strip()
            next_is_divider = next_divider.startswith('-- =') and set(next_divider.replace('-- ', '')) <= {'='}
            if title_line.startswith('--') and next_is_divider:
                # Emit just the title line
                out_lines.append(lines[i + 1])
                i += 3
                # Skip any immediately-following descriptive comment lines
                while i < len(lines) and lines[i].strip().startswith('--'):
                    i += 1
                continue
        out_lines.append(lines[i])
        i += 1
    return '\n'.join(out_lines)


with open(f'{BASE}/setup.sql', encoding='utf-8') as f:
    setup = f.read()
with open(f'{BASE}/queries.sql', encoding='utf-8') as f:
    queries = f.read()
with open(f'{BASE}/cleanup.sql', encoding='utf-8') as f:
    cleanup = f.read()

setup_p = collapse_blanks(
    drop_orphan_trailing_banners(
        strip_banner_sections(
            strip_leading_banner(
                strip_grants_and_detects(substitute_vars(setup))
            )
        )
    )
).strip()
queries_p = collapse_blanks(
    strip_banner_sections(
        strip_leading_banner(
            strip_asserts(substitute_vars(queries))
        )
    )
).strip()
cleanup_p = strip_leading_banner(substitute_vars(cleanup)).strip()

header = (
    "-- DEMO: Mira's Mercantile \u2014 Retail Analytics Chart Gallery\n"
    "-- Difficulty: intermediate | Time: ~5 min\n"
    "-- Self-contained"
)

SEP_SETUP = "-- ========================================================\n-- SETUP\n-- ========================================================"
SEP_QUERIES = "-- ========================================================\n-- QUERIES\n-- ========================================================"
SEP_CLEANUP = "-- ========================================================\n-- CLEANUP\n-- ========================================================"

syntax = "\n\n".join([header, SEP_SETUP, setup_p, SEP_QUERIES, queries_p, SEP_CLEANUP, cleanup_p])

description = (
    "## When to Use\n\n"
    "Use this demo as the canonical reference for the `CREATE CHART` command in the GUI's "
    "Query Explorer. It exercises every chart type the renderer supports, every option clause "
    "commonly used in practice (TITLE, SUBTITLE, XLABEL, YLABEL, VALUES, LEGEND, SMOOTH, STACKED, "
    "BINS), and both single-series and `GROUP BY`-pivoted multi-series patterns.\n\n"
    "## What You Will Learn\n\n"
    "1. The canonical `CREATE CHART <type> FROM (<query>) X <col> Y <col>, ...` syntax and when "
    "to specify X/Y explicitly vs. auto-detect from the source query shape\n"
    "2. How to pick a chart type for a given analytical question \u2014 BAR for category comparison, "
    "HBAR for ranked lists, LINE/AREA for trends, SCATTER for correlations, PIE for share-of-whole, "
    "HISTOGRAM for distributions, HEATMAP for 2D matrices, RADAR for multi-KPI entity comparison, "
    "CANDLESTICK for OHLC financial data\n"
    "3. How `GROUP BY <col>` inside the CHART clause pivots a single Y column into multiple series "
    "\u2014 used here for stacked AREA, HEATMAP rows, and RADAR axes\n"
    "4. How to combine validation (ASSERT-guarded SELECTs) with visualization (CREATE CHART) so "
    "every rendered SVG has pre-proven numeric correctness\n"
    "5. CANDLESTICK's exact-4-Y-column contract: (open, close, high, low) \u2014 any other count "
    "returns an error\n"
    "6. How option clauses stack: `SMOOTH`, `STACKED`, `VALUES ON`, `LEGEND RIGHT`, `BINS 8`, "
    "`TITLE '...'`, `SUBTITLE '...'`, `XLABEL '...'`, `YLABEL '...'` \u2014 all optional, all "
    "chainable after the FROM clause\n\n"
    "## Prerequisites\n\n"
    "Self-contained. Two Delta tables are created and populated from in-line INSERT VALUES \u2014 "
    "no external data files. Set `data_path` to any writable storage root and `zone_name` to your "
    "target zone."
)

pitfalls = [
    "CANDLESTICK requires EXACTLY 4 Y columns in this order: open, close, high, low. Supplying fewer than 4 \u2014 or reordering them \u2014 returns `CANDLESTICK chart requires exactly 4 Y columns`. The demo's stock_prices table stores them as (open_price, close_price, high_price, low_price) specifically so the Y clause can match.",
    "The executor caps every chart at 10,000 rows and 5 MB SVG; the source query is wrapped in `SELECT * FROM (...) LIMIT 10001`. For production-sized tables, add a server-side `LIMIT` in the inner query or the explicit `LIMIT <n>` option clause \u2014 otherwise rendering silently truncates and emits a WARNING in the result message.",
    "`GROUP BY <col>` inside the CHART clause only pivots when exactly one Y column is specified. If you list multiple Y columns AND a GROUP BY column, the GROUP BY is ignored and each Y column becomes its own series instead. For stacked AREA with multi-category data, use one Y column + GROUP BY.",
    "HISTOGRAM reads its X column as NUMERIC (not categorical), bins it via Sturges' rule by default, and labels each bin with its range \u2014 `BINS <n>` overrides the bin count. Passing a string column as X to HISTOGRAM will fail to extract values.",
    "PIE auto-buckets the tail of slices beyond the 12th into an `Other` slice and sorts slices by value descending. If your ordered categorical output matters for narrative, use BAR instead \u2014 PIE will reorder it.",
    "Chart rendering errors (e.g., empty source query, missing numeric Y column) surface with a `CREATE CHART:` prefix in the error message. The ASSERTs in the preceding SELECT catch underlying data issues earlier, which is why this demo pairs each CHART with a validation SELECT on the same aggregation."
]

doc = {
    "id": "demo_delta_chart_gallery",
    "name": "Demo: Mira's Mercantile \u2014 Retail Analytics Chart Gallery",
    "category": "Analytics",
    "difficulty": "intermediate",
    "summary": (
        "End-to-end showcase of every CREATE CHART visualization type \u2014 BAR, HBAR, LINE, AREA, "
        "SCATTER, PIE, HISTOGRAM, HEATMAP, RADAR, and CANDLESTICK \u2014 rendered from a unified "
        "retail dataset (80 daily sales rows across 4 stores x 4 categories x 5 weekdays) plus 10 "
        "weeks of parent-company stock OHLC. Every chart is preceded by a validated SELECT with "
        "ASSERTs proving the underlying aggregation before the SVG is rendered."
    ),
    "description": description,
    "syntax": syntax,
    "parameters": [
        {
            "name": "LOCATION path",
            "param_type": "string",
            "required": True,
            "default_value": "~/delta-data",
            "description": "Local filesystem path under which the two Delta tables (sales_daily, stock_prices) are stored. Change to any writable directory."
        }
    ],
    "examples": [],
    "pitfalls": pitfalls,
    "tags": [
        "delta-table", "intermediate", "visualization", "chart", "bar", "hbar", "line",
        "area", "scatter", "pie", "histogram", "heatmap", "radar", "candlestick",
        "gui", "query-explorer", "retail", "ohlc", "stacked", "group-by", "pivot"
    ],
    "see_also": [
        "CREATE_CHART", "SELECT", "GROUP_BY", "UNION_ALL", "CAST", "ROUND",
        "CREATE_DELTA_TABLE", "INSERT"
    ]
}

with open(OUT, 'w', encoding='utf-8') as f:
    json.dump(doc, f, indent=2, ensure_ascii=False)
    f.write('\n')

print(f'Wrote {OUT}')
print(f'  syntax length: {len(syntax)} chars')
print(f'  description length: {len(description)} chars')
print(f'  pitfalls: {len(pitfalls)}')
print(f'  tags: {len(doc["tags"])}')
