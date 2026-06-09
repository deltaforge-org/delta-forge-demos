-- ============================================================================
-- Demo: arXiv AI Research Feed, Queries
-- ============================================================================
-- This file exercises the API endpoint end to end. Registry inspection,
-- INVOKE, run-history audit, bronze table creation, and the bronze->silver
-- promotion all live here so the user sees in one place how an XML REST
-- endpoint is driven from SQL.
--
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used. Live
-- feeds change constantly so exact counts or values are never asserted.
-- The only meaningful check is that the API returned data at all.
-- ============================================================================

-- ============================================================================
-- API surface: inspect, invoke, audit
-- ============================================================================

-- Inspect the endpoint catalog row before invoking.
DESCRIBE API ENDPOINT {{zone_name}}.arxiv_api.cs_ai_latest;

-- Issue the actual HTTPS GET. One request writes one .xml file under a
-- per-run timestamped folder inside the connection storage path.
INVOKE API ENDPOINT {{zone_name}}.arxiv_api.cs_ai_latest;

-- Per-run audit: status, files_written, bytes_written, duration_ms.
SHOW API ENDPOINT RUNS {{zone_name}}.arxiv_api.cs_ai_latest LIMIT 5;

-- ============================================================================
-- Bronze external table over the landed XML
-- ============================================================================
-- Created here, after INVOKE, on purpose: CREATE EXTERNAL TABLE runs
-- schema discovery and validation against the landed .xml file, so the
-- landing folder must exist first. row_xpath = "//entry" pivots each Atom
-- <entry> into one row. namespaces declares the three URIs arXiv uses;
-- strip_namespace_prefixes means column names don't carry atom_ / arxiv_
-- prefixes. The repeating <author><name/></author> structure is flattened
-- via join_comma so a 5-author paper lands as "Alice Smith, Bob Jones,
-- Carol Lee, ..." in one row, the team's digest tool expects this shape.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.arxiv_api.arxiv_bronze
USING XML
LOCATION 'arxiv-ai-research-feed/arxiv_api/cs_ai_latest'
OPTIONS (
    recursive = 'true',
    xml_flatten_config = '{
        "row_xpath": "//entry",
        "include_paths": [
            "/feed/entry/id",
            "/feed/entry/title",
            "/feed/entry/published",
            "/feed/entry/updated",
            "/feed/entry/summary",
            "/feed/entry/author/name"
        ],
        "default_repeat_handling": "join_comma",
        "column_mappings": {
            "/feed/entry/id":         "paper_url",
            "/feed/entry/title":      "title",
            "/feed/entry/published":  "published_at",
            "/feed/entry/updated":    "updated_at",
            "/feed/entry/summary":    "summary",
            "/feed/entry/author/name": "author_names"
        },
        "strip_namespace_prefixes": true,
        "separator": "_",
        "max_depth": 10,
        "namespaces": {
            "atom":       "http://www.w3.org/2005/Atom",
            "arxiv":      "http://arxiv.org/schemas/atom",
            "opensearch": "http://a9.com/-/spec/opensearch/1.1/"
        }
    }'
);

-- Re-detect the bronze schema against the landed XML. CREATE EXTERNAL
-- TABLE already discovered it above, so this is idempotent; it stays in
-- the demo to show the DETECT SCHEMA command explicitly.
DETECT SCHEMA FOR TABLE {{zone_name}}.arxiv_api.arxiv_bronze;

-- ============================================================================
-- Query 1: Bronze raw feed
-- ============================================================================
-- Show the first 10 rows from the landed XML. Each row is one Atom entry
-- with the six mapped columns: paper_url, title, published_at, updated_at,
-- summary, author_names. ROW_COUNT > 0 confirms the API returned data.

ASSERT ROW_COUNT > 0
SELECT
    paper_url,
    title,
    published_at,
    author_names
FROM {{zone_name}}.arxiv_api.arxiv_bronze
LIMIT 10;

-- ============================================================================
-- Query 2: Bronze -> silver promotion
-- ============================================================================
-- Copy the bronze feed into the curated silver Delta table.
-- Silver is the layer downstream digest tools point at.

INSERT INTO {{zone_name}}.arxiv_api.arxiv_silver
SELECT
    paper_url,
    title,
    published_at,
    updated_at,
    summary,
    author_names
FROM {{zone_name}}.arxiv_api.arxiv_bronze;

-- ============================================================================
-- Query 3: Silver curated feed
-- ============================================================================
-- Confirm the promotion landed. Shows the most-recently-published papers.
-- ROW_COUNT > 0 confirms at least one row made it through the INSERT.

ASSERT ROW_COUNT > 0
SELECT
    paper_url,
    title,
    published_at,
    author_names
FROM {{zone_name}}.arxiv_api.arxiv_silver
ORDER BY published_at DESC
LIMIT 10;

-- ============================================================================
-- Query 4: Multi-author papers (join_comma repeat handling)
-- ============================================================================
-- Papers with more than one author have a comma in author_names, proving
-- the xml_flatten_config default_repeat_handling = "join_comma" fired.
-- (No row count assertion: single-author batches are valid.)

SELECT
    title,
    author_names
FROM {{zone_name}}.arxiv_api.arxiv_silver
WHERE author_names LIKE '%,%'
LIMIT 5;

-- ============================================================================
-- Query 5: Timestamp shape check
-- ============================================================================
-- Atom <published> serializes as YYYY-MM-DDTHH:MM:SSZ. Show a few rows
-- to confirm the shape is intact after the XML flatten.

ASSERT ROW_COUNT > 0
SELECT
    title,
    published_at,
    updated_at
FROM {{zone_name}}.arxiv_api.arxiv_silver
ORDER BY published_at DESC
LIMIT 5;

-- ============================================================================
-- Query 6: Silver Delta history
-- ============================================================================
-- The silver table should have at least two versions: v0 (schema creation)
-- and v1 (the INSERT above).

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.arxiv_api.arxiv_silver;
