-- ============================================================================
-- Demo: Blog Moderation Corpus, Page-Based Pagination Walkthrough
-- Feature: pagination = 'page' (page_param, page_start, max_pages),
--          query_param.* carrier on CREATE API ENDPOINT
-- ============================================================================
--
-- Real-world story: an NLP moderation team at a publisher uses the
-- JSONPlaceholder /posts mock as a fixed-shape staging corpus, 100 fake
-- blog posts split 20 per page, over 5 pages. The deterministic 100-row
-- shape makes it the ideal smoke test for their pagination engine: if
-- the ingest ever returns anything other than 100 rows, something in
-- the pagination loop drifted.
--
-- This file declares the catalog objects only. The actual API call
-- (INVOKE), the post-call audit (SHOW API ENDPOINT RUNS), the schema
-- detection, and the bronze->silver promotion all live in queries.sql
-- so users can see in one place how to actually drive a REST API
-- endpoint from SQL.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.blog_moderation
    COMMENT 'Fake-blog moderation corpus sourced from JSONPlaceholder';

-- --------------------------------------------------------------------------
-- 2. REST API connection (public mock, no auth)
-- --------------------------------------------------------------------------
-- JSONPlaceholder is a zero-auth public testing service, one of the most
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
-- 3. API endpoint, page pagination with explicit query-param carrier
-- --------------------------------------------------------------------------
-- `pagination = 'page'` drives an incrementing `page_param` through the
-- query string. `page_start = 1` pairs with JSONPlaceholder's 1-indexed
-- _page convention; `max_pages = 5` bounds the loop so a runaway feed
-- can never drain the whole catalog. `query_param._limit = '20'` is
-- merged into every page's request, the standard way to carry a
-- non-paginating query parameter the engine itself doesn't know about.
-- `rate_limit_rps = '5'` is a polite throttle, JSONPlaceholder doesn't
-- enforce one but the ingest engine defaults to 10 rps and tightening
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
-- 4. Bronze external table over the per-run landed pages
-- --------------------------------------------------------------------------
-- `recursive = 'true'` walks the per-run timestamped subfolder. The
-- flatten picks out the 4 post fields JSONPlaceholder ships and maps
-- them to friendly column names. The table can be created before any
-- INVOKE has run, queries against it just return zero rows until the
-- INVOKE in queries.sql lands data.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.blog_moderation.posts_bronze
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

-- --------------------------------------------------------------------------
-- 5. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------
-- The moderation team's downstream models key off body length as a
-- quick signal, so silver pre-computes char_len once at promotion.
-- Delta gives ACID writes and time travel so a corrupted training
-- batch can be rolled back with VERSION AS OF. The bronze->silver
-- INSERT lives in queries.sql, after the INVOKE has populated bronze.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.blog_moderation.posts_silver (
    post_id   BIGINT,
    author_id BIGINT,
    title     STRING,
    body      STRING,
    char_len  BIGINT
)
LOCATION 'silver/posts_silver';
