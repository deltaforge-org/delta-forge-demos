-- ============================================================================
-- Demo: Rust Release Catalog, Queries
-- ============================================================================
-- API demo assertion policy: only ASSERT ROW_COUNT > 0 is used.
--
-- Bronze external table creation happens HERE, not in setup.sql. CREATE
-- EXTERNAL TABLE validates and detects schema against the landed files, so
-- the bronze table is created in Block 4, after the INVOKE has written its
-- pages.
--
-- Block ordering note: INVOKE is isolated in its own block. The planner
-- pre-registers external tables across the whole script and JSON
-- registration fails on empty directories, so the bronze CREATE EXTERNAL
-- TABLE and any block referencing rust_releases_bronze must run after the
-- INVOKE has written files. The bronze CREATE therefore lives in Block 4,
-- never in the INVOKE block itself.
-- ============================================================================

-- ============================================================================
-- Block 1: describe the endpoint
-- ============================================================================

DESCRIBE API ENDPOINT {{zone_name}}.github_releases.rust_releases;

-- ============================================================================
-- Block 2: INVOKE the endpoint (isolated)
-- ============================================================================
-- Single-page response (per_page=30) writes one page_0001.json under
-- a timestamped per-run folder.

INVOKE API ENDPOINT {{zone_name}}.github_releases.rust_releases;

-- ============================================================================
-- Block 3: per-run audit
-- ============================================================================

SHOW API ENDPOINT RUNS {{zone_name}}.github_releases.rust_releases LIMIT 5;

-- ============================================================================
-- Block 4: detect bronze schema
-- ============================================================================
-- The bronze external table is created HERE, after the INVOKE, not in
-- setup.sql. CREATE EXTERNAL TABLE validates and detects schema against the
-- landed files, so the JSON landing folder must already exist. LOCATION is
-- relative to the zone's storage_root, so it resolves to the same path the
-- ingest engine writes to. `recursive` walks the timestamped per-run
-- subfolders so adding more INVOKE runs over time expands the row set
-- without editing the table definition. json_flatten_config picks specific
-- fields out of each release object in the response array and maps them to
-- friendly flat column names, the queryable shape the platform +
-- release-engineering teams want.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.github_releases.rust_releases_bronze
USING JSON
LOCATION 'rust-release-catalog/github_releases/rust_releases'
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

DETECT SCHEMA FOR TABLE {{zone_name}}.github_releases.rust_releases_bronze;

-- ============================================================================
-- Block 5: bronze feed landed
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    tag_name,
    release_name,
    author_login,
    published_at
FROM {{zone_name}}.github_releases.rust_releases_bronze
LIMIT 10;

-- ============================================================================
-- Block 6: bronze -> silver promotion
-- ============================================================================

INSERT INTO {{zone_name}}.github_releases.rust_releases_silver
SELECT
    release_id,
    tag_name,
    release_name,
    is_draft,
    is_prerelease,
    created_at,
    published_at,
    html_url,
    author_login
FROM {{zone_name}}.github_releases.rust_releases_bronze;

-- ============================================================================
-- Block 7: silver typed releases
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT
    tag_name,
    release_name,
    is_draft,
    is_prerelease,
    published_at
FROM {{zone_name}}.github_releases.rust_releases_silver
WHERE is_draft = false
  AND is_prerelease = false
ORDER BY published_at DESC
LIMIT 10;

-- ============================================================================
-- Block 8: release overview
-- ============================================================================

SELECT
    COUNT(*)         AS total_releases,
    MIN(published_at) AS oldest_release,
    MAX(published_at) AS newest_release
FROM {{zone_name}}.github_releases.rust_releases_silver;

-- ============================================================================
-- Block 9: silver Delta history
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.github_releases.rust_releases_silver;
