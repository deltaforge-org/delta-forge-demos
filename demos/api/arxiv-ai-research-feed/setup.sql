-- ============================================================================
-- Demo: arXiv AI Research Feed — RESPONSE FORMAT XML
-- Feature: RESPONSE FORMAT XML on CREATE API ENDPOINT, xml_flatten_config
--          with row_xpath + namespaces map + strip_namespace_prefixes,
--          join_comma repeat handling for multi-author papers
-- ============================================================================
--
-- Real-world story: a corporate R&D library maintains a rolling feed of
-- the latest cs.AI paper abstracts from arXiv. Researchers get a daily
-- digest of what the open community has just posted. The feed is stable
-- shape (Atom 1.0 XML, arXiv has not broken this API in 15 years) and
-- the 50-row max_results cap makes it a perfect testbed for exercising
-- the XML response path end to end.
--
-- Pipeline:
--   1. Zone + schema  — bronze landing + research_intel schema
--   2. Connection     — arXiv public API, no auth
--                         (polite rate limit: 0.5 rps / 1 request per 2s)
--   3. API endpoint   — URL carries the search query inline:
--                         ?search_query=cat%3Acs.AI
--                         &max_results=50
--                         &sortBy=submittedDate&sortOrder=descending
--                       RESPONSE FORMAT XML is the only bit of syntax that
--                       differs from a JSON endpoint — the engine writes
--                       response bodies as `.xml` instead of `.json`.
--   4. INVOKE         — single page fetch; the max_results=50 cap means
--                         pagination is not needed.
--   5. External table — XML flatten with row_xpath = "//entry", namespaces
--                         declared explicitly, strip_namespace_prefixes
--                         for clean column names, and join_comma for
--                         multi-author <author> repeats.
--   6. Silver Delta   — typed promotion with TIMESTAMP columns for
--                         published_at / updated_at.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.research_intel
    COMMENT 'Research intelligence — arXiv cs.AI latest-papers feed';

-- --------------------------------------------------------------------------
-- 2. REST API connection
-- --------------------------------------------------------------------------
-- arXiv allows anonymous queries but asks for a polite rate (~ 1 req per
-- 3s). rate_limit_rps = '1' here caps at 1 per second; the endpoint
-- below tightens it further to 0.5 rps. timeout_secs = '60' accommodates
-- arXiv's occasional slow-but-OK responses during peak academic hours.

CREATE CONNECTION IF NOT EXISTS arxiv_api
    TYPE = rest_api
    OPTIONS (
        base_url       = 'http://export.arxiv.org',
        auth_mode      = 'none',
        storage_zone   = '{{zone_name}}',
        base_path      = 'arxiv_api',
        timeout_secs   = '60',
        rate_limit_rps = '1'
    );

-- --------------------------------------------------------------------------
-- 3. API endpoint — RESPONSE FORMAT XML
-- --------------------------------------------------------------------------
-- The key bit: RESPONSE FORMAT XML. For JSON this line reads
-- `RESPONSE FORMAT JSON`. The engine uses this to pick the file
-- extension it writes (.xml vs .json) — the external table below then
-- reads those `.xml` files. There is no runtime parsing at INVOKE time;
-- the engine hands the wire bytes to the downstream table unchanged.
--
-- URL carries the search query inline:
--   search_query = cat:cs.AI   (category filter, URL-encoded colon)
--   max_results  = 50          (arXiv's largest stable single-page cap)
--   sortBy       = submittedDate  (time-ordered)
--   sortOrder    = descending     (newest first)
--
-- rate_limit_rps = '0.5' overrides the connection default for this
-- fragile endpoint; retry_max_attempts = '4' gives arXiv one extra
-- chance on transient 503s.

CREATE API ENDPOINT {{zone_name}}.arxiv_api.cs_ai_latest
    URL '/api/query?search_query=cat%3Acs.AI&max_results=50&sortBy=submittedDate&sortOrder=descending'
    RESPONSE FORMAT XML
    OPTIONS (
        rate_limit_rps     = '0.5',
        retry_max_attempts = '4',
        timeout_secs       = '60'
    );

-- --------------------------------------------------------------------------
-- 4. INVOKE — single-page XML fetch
-- --------------------------------------------------------------------------
-- One GET, one .xml file written under the per-run timestamped folder.
-- No pagination because max_results = 50 caps the response in one call.

INVOKE API ENDPOINT {{zone_name}}.arxiv_api.cs_ai_latest;

-- --------------------------------------------------------------------------
-- 5. External table — XML flatten over the landed Atom feed
-- --------------------------------------------------------------------------
-- row_xpath = "//entry" pivots each Atom <entry> into one row.
-- namespaces declares the three URIs arXiv uses; strip_namespace_prefixes
-- means column names don't carry atom_ / arxiv_ prefixes. The repeating
-- <author><name/></author> structure is flattened via join_comma so a
-- 5-author paper lands as "Alice Smith, Bob Jones, Carol Lee, ..." in
-- one row — the team's digest tool expects this shape.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.research_intel.arxiv_bronze
USING XML
LOCATION 'arxiv_api/cs_ai_latest'
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

DETECT SCHEMA FOR TABLE {{zone_name}}.research_intel.arxiv_bronze;

-- --------------------------------------------------------------------------
-- 6. Silver Delta table — typed promotion
-- --------------------------------------------------------------------------
-- Silver is what the digest tool queries: TIMESTAMP columns let it do
-- `WHERE published_at >= NOW() - INTERVAL '1 day'` natively. Bronze
-- stays around for researchers who need to re-parse the raw XML.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.research_intel.arxiv_silver (
    paper_url     STRING,
    title         STRING,
    published_at  STRING,
    updated_at    STRING,
    summary       STRING,
    author_names  STRING
)
LOCATION 'silver/arxiv_latest';

INSERT INTO {{zone_name}}.research_intel.arxiv_silver
SELECT
    paper_url,
    title,
    published_at,
    updated_at,
    summary,
    author_names
FROM {{zone_name}}.research_intel.arxiv_bronze;

