-- ============================================================================
-- Demo: Blog Moderation Corpus — Page-Based Pagination Walkthrough
-- Feature: pagination = 'page' (page_param, page_start, max_pages),
--          query_param.* carrier, SHOW / DESCRIBE / SHOW RUNS metadata reads
-- ============================================================================
--
-- Real-world story: an NLP moderation team at a publisher uses the
-- JSONPlaceholder /posts mock as a fixed-shape staging corpus — 100 fake
-- blog posts split 20 per page, over 5 pages. The deterministic 100-row
-- shape makes it the ideal smoke test for their pagination engine: if
-- the ingest ever returns anything other than 100 rows, something in
-- the pagination loop drifted.
--
-- Pipeline:
--   1. Zone + schema       — bronze landing + content_moderation schema
--   2. REST connection     — JSONPlaceholder is public, auth_mode = 'none'
--   3. API endpoint        — URL '/posts' with paginated OPTIONS:
--                              pagination = 'page'
--                              page_param = '_page'
--                              page_start = '1'
--                              max_pages  = '5'
--                              query_param._limit = '20'   (per-page size)
--   4. SHOW API ENDPOINTS  — confirms the endpoint is registered
--   5. DESCRIBE API ENDPOINT — verbose config inspection before INVOKE
--   6. INVOKE              — engine walks /posts?_page=1.._page=5
--                              &_limit=20, writes 5 landing files
--   7. SHOW API ENDPOINT RUNS — confirms the run row landed in the
--                              api_endpoint_runs catalog table
--   8. External table      — bronze JSON flatten over the 5 landed pages
--   9. Silver Delta table  — typed promotion with computed char_len
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.content_moderation
    COMMENT 'Fake-blog moderation corpus sourced from JSONPlaceholder';

-- --------------------------------------------------------------------------
-- 2. REST API connection (public mock, no auth)
-- --------------------------------------------------------------------------
-- JSONPlaceholder is a zero-auth public testing service — one of the most
-- stable endpoints on the web for demoing an ingest pipeline. No
-- CREDENTIAL line is needed because auth_mode = 'none' skips credential
-- resolution entirely.

CREATE CONNECTION IF NOT EXISTS blog_moderation
    TYPE = rest_api
    OPTIONS (
        base_url     = 'https://jsonplaceholder.typicode.com',
        auth_mode    = 'none',
        storage_zone = '{{zone_name}}',
        base_path    = 'blog_moderation',
        timeout_secs = '30'
    );

-- --------------------------------------------------------------------------
-- 3. API endpoint — page pagination with explicit query-param carrier
-- --------------------------------------------------------------------------
-- `pagination = 'page'` drives an incrementing `page_param` through the
-- query string. `page_start = 1` pairs with JSONPlaceholder's 1-indexed
-- _page convention; `max_pages = 5` bounds the loop so a runaway feed
-- can never drain the whole catalog. `query_param._limit = '20'` is
-- merged into every page's request — this is the standard way to carry
-- a non-paginating query parameter the engine itself doesn't know about.
-- `rate_limit_rps = '5'` is a polite throttle — JSONPlaceholder doesn't
-- enforce one, but the ingest engine defaults to 10 rps and tightening
-- it for a page-by-page feed is the well-behaved citizen pattern.

CREATE API ENDPOINT {{zone_name}}.blog_moderation.blog_posts
    URL '/posts'
    RESPONSE FORMAT JSON
    OPTIONS (
        pagination         = 'page',
        page_param         = '_page',
        page_start         = '1',
        max_pages          = '5',
        query_param._limit = '20',
        rate_limit_rps     = '5'
    );

-- --------------------------------------------------------------------------
-- 4. SHOW API ENDPOINTS — registry smoke test
-- --------------------------------------------------------------------------
-- Lists every endpoint under the blog_moderation connection. Narrow
-- scoping via IN CONNECTION keeps the result small in shared envs.

SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.blog_moderation;

-- --------------------------------------------------------------------------
-- 5. DESCRIBE API ENDPOINT — full config dump
-- --------------------------------------------------------------------------
-- Prints url, http_method, response_format, option count, and the last
-- run status. Useful before an INVOKE to confirm the paginate knobs are
-- what you expect.

DESCRIBE API ENDPOINT {{zone_name}}.blog_moderation.blog_posts;

-- --------------------------------------------------------------------------
-- 6. INVOKE — actual HTTPS fetch across all 5 pages
-- --------------------------------------------------------------------------
-- Engine walks _page=1.._page=5 with &_limit=20 each, producing 5 JSON
-- files under the per-run timestamped folder. Each file is an array of
-- 20 post objects.

INVOKE API ENDPOINT {{zone_name}}.blog_moderation.blog_posts;

-- --------------------------------------------------------------------------
-- 7. SHOW API ENDPOINT RUNS — audit record produced by INVOKE
-- --------------------------------------------------------------------------
-- Each INVOKE writes one row into api_endpoint_runs with status,
-- pages_fetched, files_written, bytes_written, watermark deltas. LIMIT 5
-- caps history so repeat demo runs stay readable.

SHOW API ENDPOINT RUNS {{zone_name}}.blog_moderation.blog_posts LIMIT 5;

-- --------------------------------------------------------------------------
-- 8. External table over the 5 landed pages
-- --------------------------------------------------------------------------
-- `recursive = 'true'` walks the per-run timestamped subfolder. The
-- flatten picks out the 4 post fields JSONPlaceholder ships and maps
-- them to friendly column names.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.content_moderation.posts_bronze
USING JSON
LOCATION 'blog_moderation/blog_posts'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.userId",
            "$.id",
            "$.title",
            "$.body"
        ],
        "column_mappings": {
            "$.userId": "author_id",
            "$.id":     "post_id",
            "$.title":  "title",
            "$.body":   "body"
        },
        "max_depth": 2,
        "separator": "_",
        "infer_types": true
    }'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.content_moderation.posts_bronze;

-- --------------------------------------------------------------------------
-- 9. Silver Delta table — typed promotion with computed length
-- --------------------------------------------------------------------------
-- The moderation team's downstream models key off body length as a
-- quick signal, so silver pre-computes char_len once at promotion.
-- Delta gives ACID writes and time travel so a corrupted training
-- batch can be rolled back with VERSION AS OF.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.content_moderation.posts_silver (
    post_id   BIGINT,
    author_id BIGINT,
    title     STRING,
    body      STRING,
    char_len  BIGINT
)
LOCATION 'silver/posts_silver';

INSERT INTO {{zone_name}}.content_moderation.posts_silver
SELECT
    CAST(post_id AS BIGINT)      AS post_id,
    CAST(author_id AS BIGINT)    AS author_id,
    title,
    body,
    CAST(LENGTH(body) AS BIGINT) AS char_len
FROM {{zone_name}}.content_moderation.posts_bronze;

