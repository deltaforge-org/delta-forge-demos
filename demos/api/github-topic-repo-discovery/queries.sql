-- ============================================================================
-- Demo: GitHub Topic Repo Discovery — Queries
-- ============================================================================
-- Validates link-header pagination + FULL REFRESH + nested-flatten on
-- GitHub search responses:
--   • Exactly 90 rows (30 per page × 3 pages, bounded by max_pages).
--   • Every row has a distinct repo_id and full_name (the search API
--     never returns duplicates across pages of a single sort order).
--   • Every html_url is a github.com URL (SSRF guard proof).
--   • Every full_name is `owner/repo` shaped (has a slash) — the
--     flatten preserved GitHub's canonical name format.
--   • Star counts are all non-negative; max > 0 (topic has >= 1 popular
--     repo — Delta Lake's home).
--   • Most rows are original repos, not forks.
--
-- Stability note: the GitHub search API surface and shape are stable,
-- but star counts drift daily. Assertions that would flake (exact star
-- counts, named top repo) are avoided. The 90-row exact count is
-- stable because the topic has > 90 repos and max_pages caps the crawl.
-- ============================================================================

-- ============================================================================
-- Query 1: Page Budget — 30 per page × 3 pages = 90 rows
-- ============================================================================
-- max_pages = 3 is the hard cap. The `topic:delta-lake` search returns
-- far more than 90 matches, so the crawl is guaranteed bounded. Any
-- count other than 90 means: link-header following broke (too few), the
-- per_page pin was stripped (variable), or a page response double-wrote.

ASSERT ROW_COUNT = 1
ASSERT VALUE repo_count = 90
SELECT COUNT(*) AS repo_count
FROM {{zone_name}}.oss_intel.delta_lake_repos_bronze;

-- ============================================================================
-- Query 2: Search-API Distinctness — no duplicates across pages
-- ============================================================================
-- GitHub's search API guarantees a single consistent ordering for a
-- fixed query+sort; a repo never appears on two pages. COUNT(DISTINCT)
-- equaling total COUNT(*) proves link-header pagination didn't
-- misinterpret `rel="next"` (a common bug: jumping back to page 1).

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_repos = 90
ASSERT VALUE distinct_full_names = 90
SELECT
    COUNT(DISTINCT repo_id)   AS distinct_repos,
    COUNT(DISTINCT full_name) AS distinct_full_names
FROM {{zone_name}}.oss_intel.delta_lake_repos_bronze;

-- ============================================================================
-- Query 3: URL + Identifier Invariants
-- ============================================================================
-- Every row's html_url is on github.com (anti-SSRF proof), full_name
-- is non-null, owner_login is non-null. A flatten that dropped one of
-- these would surface immediately as a count mismatch.

ASSERT ROW_COUNT = 1
ASSERT VALUE github_urls = 90
ASSERT VALUE non_null_full_names = 90
ASSERT VALUE non_null_owners = 90
SELECT
    SUM(CASE WHEN html_url LIKE 'https://github.com/%' THEN 1 ELSE 0 END) AS github_urls,
    SUM(CASE WHEN full_name IS NOT NULL                THEN 1 ELSE 0 END) AS non_null_full_names,
    SUM(CASE WHEN owner_login IS NOT NULL              THEN 1 ELSE 0 END) AS non_null_owners
FROM {{zone_name}}.oss_intel.delta_lake_repos_silver;

-- ============================================================================
-- Query 4: Star-Count Sanity — BIGINT typed column, non-negative
-- ============================================================================
-- Silver's BIGINT stars column lets the report do `ORDER BY stars DESC`
-- natively. MIN >= 0 proves no negative leaked through the CAST (which
-- would have been a flatten regression). MAX > 0 proves at least one
-- repo has stars — trivially true for a search sorted by stars desc.

ASSERT ROW_COUNT = 1
ASSERT VALUE min_stars_non_negative = 1
ASSERT VALUE max_stars_positive = 1
SELECT
    CASE WHEN MIN(stars) >= 0 THEN 1 ELSE 0 END AS min_stars_non_negative,
    CASE WHEN MAX(stars) > 0  THEN 1 ELSE 0 END AS max_stars_positive
FROM {{zone_name}}.oss_intel.delta_lake_repos_silver;

-- ============================================================================
-- Query 5: Fork Majority — the topic is dominated by originals
-- ============================================================================
-- Sorting by stars desc pushes the topic's canonical repos to the top;
-- forks don't accumulate stars independently. The top 90 is
-- non-fork-dominated. If is_fork is true for most rows, the flatten
-- mis-cast the boolean or the sort is broken.

ASSERT ROW_COUNT = 1
ASSERT VALUE non_fork_majority = 1
SELECT
    CASE WHEN SUM(CASE WHEN is_fork = false THEN 1 ELSE 0 END)
              > SUM(CASE WHEN is_fork = true THEN 1 ELSE 0 END)
         THEN 1 ELSE 0 END AS non_fork_majority
FROM {{zone_name}}.oss_intel.delta_lake_repos_silver;

-- ============================================================================
-- Query 6: full_name Shape Invariant — every name is `owner/repo`
-- ============================================================================
-- GitHub full names are always `<owner>/<repo>`. Asserting every row
-- matches `%/%` proves the flatten preserved the string intact — no
-- trimming, no splitting.

ASSERT ROW_COUNT = 1
ASSERT VALUE full_name_has_slash = 90
SELECT
    SUM(CASE WHEN full_name LIKE '%/%' THEN 1 ELSE 0 END) AS full_name_has_slash
FROM {{zone_name}}.oss_intel.delta_lake_repos_silver;

-- ============================================================================
-- Query 7: Silver Delta History — v0 schema + v1 INSERT
-- ============================================================================
-- CREATE DELTA TABLE (v0) + INSERT FROM SELECT (v1). Subsequent
-- refreshes would add another version each time.

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.oss_intel.delta_lake_repos_silver;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting query covering every invariant: row count, distinct
-- repos, github.com-only URLs, owner/repo shape, non-negative stars,
-- and bronze↔silver row parity.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_repos = 90
ASSERT VALUE distinct_repos = 90
ASSERT VALUE all_github_urls = 1
ASSERT VALUE all_have_slash = 1
ASSERT VALUE stars_monotonic_plausible = 1
ASSERT VALUE bronze_silver_parity = 1
SELECT
    COUNT(*)                                                                       AS total_repos,
    COUNT(DISTINCT repo_id)                                                        AS distinct_repos,
    CASE WHEN SUM(CASE WHEN html_url NOT LIKE 'https://github.com/%' THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                         AS all_github_urls,
    CASE WHEN SUM(CASE WHEN full_name NOT LIKE '%/%' THEN 1 ELSE 0 END) = 0
         THEN 1 ELSE 0 END                                                         AS all_have_slash,
    CASE WHEN MAX(stars) >= MIN(stars) AND MIN(stars) >= 0
         THEN 1 ELSE 0 END                                                         AS stars_monotonic_plausible,
    CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM {{zone_name}}.oss_intel.delta_lake_repos_bronze)
         THEN 1 ELSE 0 END                                                         AS bronze_silver_parity
FROM {{zone_name}}.oss_intel.delta_lake_repos_silver;
