-- ============================================================================
-- Demo: Rust Release Catalog, Setup
-- Feature: REST API ingest catalog objects, connection, endpoint, bronze
--          external table with JSON flattening, silver Delta table
-- ============================================================================
--
-- Real-world story: a platform/DevRel team tracks the cadence of upstream
-- rust-lang/rust releases so they can plan toolchain rollouts, schedule
-- engineering windows for compiler upgrades, and feed a release-intel
-- dashboard. They sync the public GitHub releases API into a bronze
-- landing, flatten the nested release records into a queryable shape,
-- then promote the result into a typed silver Delta table that
-- dashboards point at.
--
-- This file declares the catalog objects only. The INVOKE that issues
-- the actual HTTPS GET, the run audit, the schema detection, and the
-- bronze->silver promotion all live in queries.sql so the user can
-- see the end-to-end ingest call from a single file.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------
-- Zone is the permission boundary. INVOKE writes downloaded files under
-- <zone-root>/<source>/<endpoint>/<run-ts>/page_NNNN.json, the zone
-- here is the destination + the right that gates who can run the ingest.

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.github_releases
    COMMENT 'Upstream OSS release intelligence, GitHub releases feed for the toolchains the platform team tracks';

-- --------------------------------------------------------------------------
-- 2. REST API connection (a `data_sources` row of source_type = rest_api)
-- --------------------------------------------------------------------------
-- Carries the host, auth mode, and storage destination. `auth_mode = 'none'`
-- here because GitHub's /repos/.../releases endpoint is public, it
-- validates every Authorization header it receives (even for public
-- resources) and rejects placeholder tokens with HTTP 401, so sending
-- none is the right move. For a protected API you would flip this to
-- `auth_mode = 'bearer'`, add `CREDENTIAL = <vault_entry>` below, and
-- keep everything else unchanged, the INVOKE path and the JSON flatten
-- are auth-agnostic.
--
-- The connection name is reused as the middle segment of the API endpoint's
-- qualified 3-part name (<zone>.<connection>.<endpoint>), so each connection
-- + endpoint pair stays traceable end-to-end.

CREATE CONNECTION IF NOT EXISTS github_releases
    TYPE = rest_api
    OPTIONS (
        base_url      = 'https://api.github.com',
        auth_mode     = 'none',
        storage_zone  = '{{zone_name}}',
        base_path     = 'github_releases',
        timeout_secs  = '30'
    );

-- --------------------------------------------------------------------------
-- 3. API endpoint definition (definition only, no HTTP yet)
-- --------------------------------------------------------------------------
-- Qualified name `<zone>.<source>.<name>` ties the endpoint to its
-- destination zone in one place. SHOW API ENDPOINTS lists this row;
-- DESCRIBE API ENDPOINT shows its full config (both demonstrated in
-- queries.sql).
--
-- `per_page=30` pins the response size to exactly 30 releases, which makes
-- every ROW_COUNT assertion in queries.sql stable across runs. Without
-- that, the default of 30 is what GitHub returns today, but any future
-- default change would silently shift the row count.

CREATE API ENDPOINT {{zone_name}}.github_releases.rust_releases
    URL '/repos/rust-lang/rust/releases?per_page=30'
    RESPONSE FORMAT JSON;

-- --------------------------------------------------------------------------
-- 4. Bronze external table over the eventual landing path
-- --------------------------------------------------------------------------
-- LOCATION is relative to the zone's storage_root, so it resolves to the
-- same path the ingest engine writes to. `recursive` walks the
-- timestamped per-run subfolders so adding more INVOKE runs over time
-- expands the row set without editing the table definition.
--
-- json_flatten_config picks specific fields out of each release object in
-- the response array and maps them to friendly flat column names, the
-- queryable shape the platform + release-engineering teams want.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.github_releases.rust_releases_bronze
USING JSON
LOCATION 'github_releases/rust_releases'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.id",
            "$.tag_name",
            "$.name",
            "$.draft",
            "$.prerelease",
            "$.created_at",
            "$.published_at",
            "$.html_url",
            "$.author.login"
        ],
        "column_mappings": {
            "$.id":            "release_id",
            "$.tag_name":      "tag_name",
            "$.name":          "release_name",
            "$.draft":         "is_draft",
            "$.prerelease":    "is_prerelease",
            "$.created_at":    "created_at",
            "$.published_at":  "published_at",
            "$.html_url":      "html_url",
            "$.author.login":  "author_login"
        },
        "max_depth": 3,
        "separator": "_",
        "infer_types": true
    }'
);

-- --------------------------------------------------------------------------
-- 5. Silver Delta table, schema-only declaration
-- --------------------------------------------------------------------------
-- Bronze (the external table above) is the raw source-of-truth: every
-- INVOKE adds another timestamped page under the landing folder, and the
-- external table reads them all on every query. That's the right shape
-- for ingest auditing and reprocessing, but it isn't what you point a
-- dashboard at, every query re-parses every page.
--
-- The silver Delta table is the curated layer downstream consumers
-- query. It carries the same flat-column shape as bronze but lives in
-- Delta format, which gives:
--   - ACID multi-row writes (the INSERT in queries.sql lands atomically)
--   - Time travel via VERSION AS OF / TIMESTAMP AS OF
--   - Schema evolution + ALTER TABLE
--   - OPTIMIZE / VACUUM lifecycle for storage efficiency
--   - Cross-engine portability (Spark, DuckDB, Polars all read it)
--
-- Re-running INVOKE writes new bronze pages, but silver only updates
-- when the INSERT (or a downstream MERGE in a real pipeline) runs.
-- That separation is what makes the medallion model auditable.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.github_releases.rust_releases_silver (
    release_id     BIGINT,
    tag_name       STRING,
    release_name   STRING,
    is_draft       BOOLEAN,
    is_prerelease  BOOLEAN,
    created_at     STRING,
    published_at   STRING,
    html_url       STRING,
    author_login   STRING
)
LOCATION 'silver/rust_releases';
