-- ============================================================================
-- Demo: arXiv AI Research Feed, RESPONSE FORMAT XML
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
-- This file declares the data-independent catalog objects: zone, schema,
-- connection, API endpoint, and the silver Delta table. The bronze
-- external table is NOT created here. CREATE EXTERNAL TABLE runs schema
-- discovery and validation against the landed XML, so it can only be
-- created after INVOKE has fetched the feed and the landing folder
-- exists. That INVOKE, the bronze table creation, the SHOW API ENDPOINT
-- RUNS audit read, and the bronze->silver promotion all live in
-- queries.sql so the user can see in one place how an XML REST endpoint
-- is driven from SQL.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.arxiv_api
    COMMENT 'Research intelligence, arXiv cs.AI latest-papers feed';

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
        base_url       = 'https://export.arxiv.org',
        auth_mode      = 'none',
        storage_zone   = '{{zone_name}}',
        base_path      = 'arxiv-ai-research-feed/arxiv_api',
        timeout_secs   = '60',
        rate_limit_rps = '1'
    );

-- --------------------------------------------------------------------------
-- 3. API endpoint, RESPONSE FORMAT XML
-- --------------------------------------------------------------------------
-- The key bit: RESPONSE FORMAT XML. For JSON this line reads
-- `RESPONSE FORMAT JSON`. The engine uses this to pick the file
-- extension it writes (.xml vs .json), the external table below then
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
-- fragile endpoint; retry_max_attempts = '3' gives arXiv two extra
-- chances on transient 429/503s, with exponential backoff between them.

CREATE API ENDPOINT {{zone_name}}.arxiv_api.cs_ai_latest
    URL '/api/query?search_query=cat%3Acs.AI&max_results=50&sortBy=submittedDate&sortOrder=descending'
    RESPONSE FORMAT XML
    OPTIONS (
        rate_limit_rps     = '0.5',
        retry_max_attempts = '3',
        timeout_secs       = '60'
    );

-- --------------------------------------------------------------------------
-- 4. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------
-- Silver is what the digest tool queries: TIMESTAMP-shaped string columns
-- let it do `WHERE published_at >= ...` natively after a downstream
-- cast. Bronze stays around for researchers who need to re-parse the
-- raw XML. The bronze->silver INSERT lives in queries.sql.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.arxiv_api.arxiv_silver (
    paper_url     STRING,
    title         STRING,
    published_at  STRING,
    updated_at    STRING,
    summary       STRING,
    author_names  STRING
)
LOCATION 'arxiv-ai-research-feed/silver/arxiv_latest';
