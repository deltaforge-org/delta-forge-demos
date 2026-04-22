-- ============================================================================
-- Demo: Rust Release Catalog
-- Feature: End-to-end REST API ingest — keychain credential, connection,
--          endpoint, INVOKE, external table with JSON flattening
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
-- Pipeline:
--   1. Vault entry             — placeholder API token in the OS keychain
--                                (the always-on default credential
--                                storage). GitHub's releases endpoint is
--                                public and works without auth, but we
--                                exercise the full credential path so
--                                production APIs that DO require auth use
--                                the same pattern verbatim.
--   2. Zone + schema           — bronze landing + release_intel for the
--                                queryable external table
--   3. REST API connection     — base URL + auth_mode + storage_zone +
--                                base_path on the data source
--   4. API ingest endpoint     — qualified name + endpoint path +
--                                response format
--   5. INVOKE                  — actual HTTPS GET, writes raw JSON to
--                                bronze under a timestamped per-run folder
--   6. External table (bronze) — JSON over the bronze landing with
--                                json_flatten_config to project nested
--                                fields into flat columns
--   7. Delta table   (silver)  — curated copy of the bronze data into a
--                                Delta table, demonstrating the typical
--                                bronze→silver promotion. The Delta layer
--                                gives ACID writes, time travel,
--                                schema evolution, and OPTIMIZE/VACUUM —
--                                queries from BI tools point here, not at
--                                the raw JSON landing.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Vault entry (the API token itself)
-- --------------------------------------------------------------------------
-- The OS keychain backend (Windows Credential Manager / macOS Keychain /
-- Linux Secret Service) is the always-on default storage — no explicit
-- registration needed. CREATE CREDENTIAL writes to it directly.
--
-- The literal SECRET below is a placeholder — GitHub's /repos/.../releases
-- endpoint is public and ignores the Authorization header. The same syntax
-- + flow applies unchanged for bearer-protected APIs (private repos, the
-- full GitHub Apps flow, Stripe, etc.); only the literal value changes.

CREATE CREDENTIAL IF NOT EXISTS github_api_token
    TYPE = CREDENTIAL
    SECRET 'demo-placeholder-token-public-releases-endpoint'
    DESCRIPTION 'Bearer token for the GitHub release-catalog sync';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------
-- Zone is the permission boundary. INVOKE writes downloaded files under
-- <zone-root>/<source>/<endpoint>/<run-ts>/page_NNNN.json — the zone
-- here is the destination + the right that gates who can run the ingest.

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.release_intel
    COMMENT 'Upstream OSS release intelligence — GitHub releases feed for the toolchains the platform team tracks';

-- --------------------------------------------------------------------------
-- 3. REST API connection (a `data_sources` row of source_type = rest_api)
-- --------------------------------------------------------------------------
-- Carries the host, auth mode, and storage destination. CREDENTIAL = …
-- references the vault entry above by name; the executor resolves the
-- secret material at INVOKE time without it ever crossing back into SQL.
--
-- The connection name is reused as the middle segment of the API endpoint's
-- qualified 3-part name (<zone>.<connection>.<endpoint>), so each connection
-- + endpoint pair stays traceable end-to-end.

CREATE CONNECTION IF NOT EXISTS github_releases
    TYPE = rest_api
    OPTIONS (
        base_url      = 'https://api.github.com',
        auth_mode     = 'bearer',
        storage_zone  = '{{zone_name}}',
        base_path     = 'github_releases',
        timeout_secs  = '30'
    )
    CREDENTIAL = github_api_token;

-- --------------------------------------------------------------------------
-- 4. API endpoint definition (definition only — no HTTP yet)
-- --------------------------------------------------------------------------
-- Qualified name `<zone>.<source>.<name>` ties the endpoint to its
-- destination zone in one place. SHOW API ENDPOINTS lists this row;
-- DESCRIBE API ENDPOINT shows its full config.
--
-- `per_page=30` pins the response size to exactly 30 releases, which makes
-- every ROW_COUNT assertion below stable across runs. Without that, the
-- default of 30 is what GitHub returns today, but any future default
-- change would silently shift the row count.

CREATE API ENDPOINT {{zone_name}}.github_releases.rust_releases
    URL '/repos/rust-lang/rust/releases?per_page=30'
    RESPONSE FORMAT JSON;

-- --------------------------------------------------------------------------
-- 5. INVOKE — actual HTTPS fetch, lands raw JSON under bronze
-- --------------------------------------------------------------------------
-- Single-page response (`per_page=30` gives us the full window in one call),
-- so pagination isn't needed. The engine writes one `page_0001.json` under
-- a timestamped per-run folder.

INVOKE API ENDPOINT {{zone_name}}.github_releases.rust_releases;

-- --------------------------------------------------------------------------
-- 6. External table over the landed JSON
-- --------------------------------------------------------------------------
-- LOCATION is relative to the zone's storage_root, so it resolves to the
-- same path the ingest engine wrote to. `recursive` walks the
-- timestamped per-run subfolders so adding more INVOKE runs over time
-- expands the row set without editing the table definition.
--
-- json_flatten_config picks specific fields out of each release object in
-- the response array and maps them to friendly flat column names — the
-- queryable shape the platform + release-engineering teams want.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.release_intel.rust_releases
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
-- Schema detection + permissions (bronze)
-- --------------------------------------------------------------------------

DETECT SCHEMA FOR TABLE {{zone_name}}.release_intel.rust_releases;
GRANT ADMIN ON TABLE {{zone_name}}.release_intel.rust_releases TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 7. Silver layer — curated Delta table promoted from the bronze landing
-- --------------------------------------------------------------------------
-- Bronze (the external table above) is the raw source-of-truth: every
-- INVOKE adds another timestamped page under the landing folder, and the
-- external table reads them all on every query. That's the right shape
-- for ingest auditing and reprocessing, but it isn't what you point a
-- dashboard at — every query re-parses every page.
--
-- The silver Delta table is the curated layer downstream consumers
-- query. It carries the same flat-column shape as bronze but lives in
-- Delta format, which gives:
--   • ACID multi-row writes (this INSERT lands atomically)
--   • Time travel via VERSION AS OF / TIMESTAMP AS OF
--   • Schema evolution + ALTER TABLE
--   • OPTIMIZE / VACUUM lifecycle for storage efficiency
--   • Cross-engine portability (Spark, DuckDB, Polars all read it)
--
-- Re-running INVOKE writes new bronze pages, but silver only updates
-- when this INSERT (or a downstream MERGE in a real pipeline) runs.
-- That separation is what makes the medallion model auditable.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.release_intel.rust_releases_silver (
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

INSERT INTO {{zone_name}}.release_intel.rust_releases_silver
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
FROM {{zone_name}}.release_intel.rust_releases;

GRANT ADMIN ON TABLE {{zone_name}}.release_intel.rust_releases_silver TO USER {{current_user}};
