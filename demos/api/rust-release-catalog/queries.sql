-- ============================================================================
-- Demo: Rust Release Catalog — Queries
-- ============================================================================
-- Validates the end-to-end REST API ingest flow:
--   • The bronze landing has an actual JSON file written by INVOKE.
--   • The flattened external table exposes the release shape we asked for.
--   • The bronze→silver promotion gives BI-quality typed columns.
--   • Stable upstream invariants hold (every release authored by rustbot,
--     names all begin with "Rust ", nothing is draft/prerelease in the
--     public list, tags are numeric "1.xx.y" strings).
--
-- Query targeting convention:
--   • Bronze (external JSON table): structural smoke checks, exact-string
--     lookups. The flatten produces Utf8 columns for the string fields;
--     native typed aggregation on booleans/ints is what silver is for.
--   • Silver (Delta table): all typed filters and bronze↔silver parity.
--     Silver has BIGINT/BOOLEAN columns by declaration, so `WHERE
--     is_draft = false` works natively — that's the headline value of
--     the bronze→silver promotion every dashboard query benefits from.
--
-- The `per_page=30` pin on the endpoint URL makes ROW_COUNT exactly 30
-- stable across runs. Other assertions use invariants that hold for the
-- entire history of the rust-lang/rust releases feed (author = rustbot,
-- "Rust " prefix, tag starts with a digit) — these will not flake even
-- when a new release is cut and the oldest entry rolls out of the window.
-- ============================================================================

-- ============================================================================
-- Query 1: Catalog Smoke Check — exactly the window we requested
-- ============================================================================
-- `per_page=30` on the endpoint URL pins the response count. If this row
-- count moves, either the flatten dropped rows or the per_page pin is
-- being stripped somewhere in the path.

ASSERT ROW_COUNT = 1
ASSERT VALUE release_count = 30
SELECT COUNT(*) AS release_count
FROM {{zone_name}}.release_intel.rust_releases;

-- ============================================================================
-- Query 2: Author Invariant — every release is authored by rustbot
-- ============================================================================
-- rust-lang/rust uses the `rustbot` service account to cut every release,
-- and has done so for the entire history visible in the API's default
-- window. Any row with a different author means either the flatten
-- mis-mapped the nested `$.author.login` path or the upstream release
-- process changed (newsworthy either way).

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_authors = 1
ASSERT VALUE author_value = 'rustbot'
SELECT COUNT(DISTINCT author_login) AS distinct_authors,
       MAX(author_login)            AS author_value
FROM {{zone_name}}.release_intel.rust_releases;

-- ============================================================================
-- Query 3: Release-Name Prefix — every entry is a "Rust x.y.z" line
-- ============================================================================
-- Every official release name in this repo follows the `Rust <version>`
-- convention. Asserting every name passes the LIKE catches both flatten
-- regressions (column missing) and upstream naming drift.

ASSERT ROW_COUNT = 1
ASSERT VALUE non_rust_named = 0
SELECT COUNT(*) AS non_rust_named
FROM {{zone_name}}.release_intel.rust_releases
WHERE release_name NOT LIKE 'Rust %';

-- ============================================================================
-- Query 4: Tag Format — every tag_name starts with a digit
-- ============================================================================
-- rust-lang/rust tags are plain SemVer strings (e.g., `1.95.0`). The
-- default `releases` feed excludes the nightly channel, so every row's
-- tag_name starts with a digit. If any tag comes through starting with
-- `v`, the upstream team switched tagging conventions.

ASSERT ROW_COUNT = 1
ASSERT VALUE non_numeric_tags = 0
SELECT COUNT(*) AS non_numeric_tags
FROM {{zone_name}}.release_intel.rust_releases
WHERE tag_name NOT LIKE '0%'
  AND tag_name NOT LIKE '1%'
  AND tag_name NOT LIKE '2%'
  AND tag_name NOT LIKE '3%'
  AND tag_name NOT LIKE '4%'
  AND tag_name NOT LIKE '5%'
  AND tag_name NOT LIKE '6%'
  AND tag_name NOT LIKE '7%'
  AND tag_name NOT LIKE '8%'
  AND tag_name NOT LIKE '9%';

-- ============================================================================
-- Query 5: No Drafts, No Prereleases — public feed is stable-channel only
-- ============================================================================
-- The GitHub `/releases` endpoint lists published, non-draft items only.
-- Silver is where we assert this because the typed BOOLEAN filter works
-- natively — bronze has these as Utf8 "true"/"false" strings after the
-- JSON flatten.

ASSERT ROW_COUNT = 1
ASSERT VALUE draft_count = 0
ASSERT VALUE prerelease_count = 0
SELECT
    SUM(CASE WHEN is_draft       = true THEN 1 ELSE 0 END) AS draft_count,
    SUM(CASE WHEN is_prerelease  = true THEN 1 ELSE 0 END) AS prerelease_count
FROM {{zone_name}}.release_intel.rust_releases_silver;

-- ============================================================================
-- Query 6: Tag Distinctness — the feed has no duplicates
-- ============================================================================
-- Every release tag is unique. Asserting distinct count equals total
-- count catches both flatten duplication and upstream republishing
-- bugs.

ASSERT ROW_COUNT = 1
ASSERT VALUE duplicate_tags = 0
SELECT COUNT(*) - COUNT(DISTINCT tag_name) AS duplicate_tags
FROM {{zone_name}}.release_intel.rust_releases;

-- ============================================================================
-- Query 7: Bronze ↔ Silver Parity — promotion preserved every row
-- ============================================================================
-- INSERT INTO ... SELECT FROM in setup.sql copied bronze (the external
-- JSON-flattened table) into the silver Delta table. After that single
-- promotion, row count and distinct tag coverage must match exactly.
-- Any drift means the promotion lost or duplicated rows.
--
-- ASSERT VALUE compares against literal scalars, not column references,
-- so we collapse the bronze↔silver comparison into pre-computed deltas
-- and assert each equals zero — same invariant, expressed in the shape
-- ASSERT accepts.

ASSERT ROW_COUNT = 1
ASSERT VALUE row_count_delta = 0
ASSERT VALUE distinct_tag_delta = 0
ASSERT VALUE silver_release_count = 30
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.release_intel.rust_releases)
        - (SELECT COUNT(*) FROM {{zone_name}}.release_intel.rust_releases_silver)
                                                                                 AS row_count_delta,
    (SELECT COUNT(DISTINCT tag_name) FROM {{zone_name}}.release_intel.rust_releases)
        - (SELECT COUNT(DISTINCT tag_name) FROM {{zone_name}}.release_intel.rust_releases_silver)
                                                                                 AS distinct_tag_delta,
    (SELECT COUNT(*) FROM {{zone_name}}.release_intel.rust_releases_silver)      AS silver_release_count;

-- ============================================================================
-- Query 8: Silver Delta Time-Travel — DESCRIBE HISTORY shows v0 + v1
-- ============================================================================
-- The Delta table got two writes during setup: the CREATE (v0, schema only)
-- and the INSERT (v1, the bronze→silver promotion). DESCRIBE HISTORY
-- exposes the transaction log and proves the table is queryable with
-- VERSION AS OF semantics — the headline Delta capability you don't get
-- from a bare external JSON table.

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.release_intel.rust_releases_silver;

-- ============================================================================
-- Query 9: Silver Boolean Filter — typed columns enable native predicates
-- ============================================================================
-- Silver's `is_draft` is a real BOOLEAN, so a `WHERE is_draft = false`
-- filter works without casting — try the same on bronze and you get a
-- type mismatch (Utf8 vs Bool). This query is the on-demo proof that
-- the silver layer is the one downstream consumers want to query. All
-- 30 rows come through the `/releases` endpoint, which is the
-- stable-channel feed by design, so every row satisfies the predicate.

ASSERT ROW_COUNT = 1
ASSERT VALUE published_releases = 30
SELECT COUNT(*) AS published_releases
FROM {{zone_name}}.release_intel.rust_releases_silver
WHERE is_draft = false
  AND is_prerelease = false;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting query exercising the whole pipeline: row count is
-- exactly 30, every author is rustbot, every name begins with "Rust ",
-- no drafts/prereleases came through, AND the silver Delta table is in
-- sync with bronze. If this passes, the credential resolved, the HTTPS
-- fetch succeeded, the bronze write landed, the JSON flatten produced
-- the expected shape, AND the bronze→silver promotion preserved every
-- row. Aggregates run on silver because that's where the typed columns
-- live.

ASSERT ROW_COUNT = 1
ASSERT VALUE release_count = 30
ASSERT VALUE rustbot_only = 1
ASSERT VALUE rust_prefix_only = 1
ASSERT VALUE stable_channel_only = 1
ASSERT VALUE silver_matches_bronze = 1
SELECT
    COUNT(*)                                                                                      AS release_count,
    CASE WHEN COUNT(DISTINCT author_login) = 1 AND MAX(author_login) = 'rustbot'
         THEN 1 ELSE 0 END                                                                        AS rustbot_only,
    CASE WHEN SUM(CASE WHEN release_name NOT LIKE 'Rust %' THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                                        AS rust_prefix_only,
    CASE WHEN SUM(CASE WHEN is_draft = true OR is_prerelease = true THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                                        AS stable_channel_only,
    CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM {{zone_name}}.release_intel.rust_releases)
         THEN 1 ELSE 0 END                                                                        AS silver_matches_bronze
FROM {{zone_name}}.release_intel.rust_releases_silver;
