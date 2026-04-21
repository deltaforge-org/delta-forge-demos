-- ============================================================================
-- Demo: European Travel Reference Catalog — Queries
-- ============================================================================
-- Validates the end-to-end REST API ingest flow:
--   • The bronze landing has an actual JSON file written by INVOKE.
--   • The flattened external table exposes the country shape we asked for.
--   • The bronze→silver promotion gives BI-quality typed columns.
--   • Stable European-geography invariants hold (Norway exists, total
--     population is in the right ballpark, every row's region is "Europe").
--
-- Query targeting convention:
--   • Bronze (external JSON table): structural smoke checks, exact-string
--     lookups by ISO code. Numeric columns there are still Utf8 because
--     `json_flatten_config` is a string-projection operation; aggregations
--     against them require CAST.
--   • Silver (Delta table): all aggregations and cross-table parity.
--     Silver has BIGINT/DOUBLE/BOOLEAN columns by declaration, so SUM
--     and arithmetic work natively — that's the headline value of the
--     bronze→silver promotion every dashboard query benefits from.
--
-- Assertions are deliberately tolerant of small REST Countries data drift
-- (a country joining/leaving the EU, a population revision) by using
-- BETWEEN ranges for aggregates and exact match for cca2/cca3 ISO codes
-- (those don't change without an actual geopolitical event).
-- ============================================================================

-- ============================================================================
-- Query 1: Catalog Smoke Check — every European country has a row
-- ============================================================================
-- REST Countries v3.1 lists ~50 European entries. The exact count varies
-- slightly with data revisions but never drops below 40 or climbs above 60.
-- Assertion is on the COUNT VALUE, not ROW_COUNT (which is always 1 here).

ASSERT ROW_COUNT = 1
ASSERT VALUE country_count BETWEEN 40 AND 60
SELECT COUNT(*) AS country_count
FROM {{zone_name}}.travel_geo.european_countries;

-- ============================================================================
-- Query 2: Norway Reference Lookup — proves the JSON flatten worked
-- ============================================================================
-- Pick a stable, isolated country whose key fields don't change.
-- Norway: cca2 = NO, cca3 = NOR, capital Oslo. The capital assertion
-- exercises array-index extraction in json_flatten_config — the
-- `$.capital[0]` path picks the first element of the capitals array
-- as a scalar string (not as an array). If you see `["Oslo"]` instead
-- of `Oslo` here, the [N] index isn't being honoured at flatten time.

ASSERT ROW_COUNT = 1
ASSERT VALUE name_common = 'Norway' WHERE cca2 = 'NO'
ASSERT VALUE cca3 = 'NOR' WHERE cca2 = 'NO'
ASSERT VALUE capital = 'Oslo' WHERE cca2 = 'NO'
ASSERT VALUE region = 'Europe' WHERE cca2 = 'NO'
SELECT name_common, cca2, cca3, capital, region, subregion
FROM {{zone_name}}.travel_geo.european_countries
WHERE cca2 = 'NO';

-- ============================================================================
-- Query 3: Region Invariant — every row's region is "Europe"
-- ============================================================================
-- We asked for /v3.1/region/europe, so the response contract guarantees
-- region = 'Europe' on every row. If the flatten dropped or mis-mapped
-- the region column, this assertion fails loud.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_regions = 1
ASSERT VALUE region_value = 'Europe'
SELECT COUNT(DISTINCT region) AS distinct_regions,
       MAX(region)            AS region_value
FROM {{zone_name}}.travel_geo.european_countries;

-- ============================================================================
-- Query 4: Population Aggregate — Europe total in the expected range (silver)
-- ============================================================================
-- Total European population (REST Countries v3.1 dataset) sits at
-- ~745M. BETWEEN 700M and 800M absorbs revisions without flaking.
-- Querying SILVER because population is BIGINT there; bronze still has
-- it as Utf8 (the JSON flatten produces strings — promotion is what
-- gives downstream consumers the typed shape they expect).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_population BETWEEN 700000000 AND 800000000
SELECT SUM(population) AS total_population
FROM {{zone_name}}.travel_geo.european_countries_silver;

-- ============================================================================
-- Query 5: ISO-Code Coverage — three small sovereign states all present
-- ============================================================================
-- Picks three small but stable European entities by ISO 3166-1 alpha-2
-- code. None of these have changed in living memory; if any drops out
-- of the response, the flatten or the upstream API has regressed.

ASSERT ROW_COUNT = 3
ASSERT VALUE name_common = 'Iceland' WHERE cca2 = 'IS'
ASSERT VALUE name_common = 'Malta'   WHERE cca2 = 'MT'
ASSERT VALUE name_common = 'Monaco'  WHERE cca2 = 'MC'
SELECT cca2, cca3, name_common
FROM {{zone_name}}.travel_geo.european_countries
WHERE cca2 IN ('IS', 'MT', 'MC')
ORDER BY cca2;

-- ============================================================================
-- Query 6: Subregion Distribution — canonical European subregions
-- ============================================================================
-- The REST Countries dataset places every European country into one of
-- a small set of subregions (Northern, Western, Southern, Eastern,
-- Southeast). Asserting the distinct count is in a narrow band catches
-- both flatten regressions (column missing) and upstream breaking
-- changes (a wholly new subregion category appearing).

ASSERT ROW_COUNT = 1
ASSERT VALUE subregion_count BETWEEN 4 AND 6
SELECT COUNT(DISTINCT subregion) AS subregion_count
FROM {{zone_name}}.travel_geo.european_countries
WHERE subregion IS NOT NULL;

-- ============================================================================
-- Query 7: Bronze ↔ Silver Parity — promotion preserved every row
-- ============================================================================
-- INSERT INTO ... SELECT FROM in setup.sql copied bronze (the external
-- JSON-flattened table) into the silver Delta table. After that single
-- promotion, row count and distinct ISO-code coverage must match
-- exactly. Any drift means the promotion lost or duplicated rows.
-- We compare population SUM only on the silver side (bronze population
-- is Utf8 — the typed silver column is what dashboards aggregate over).
--
-- ASSERT VALUE compares against literal scalars, not column references,
-- so we collapse the bronze↔silver comparison into a pre-computed delta
-- and assert it equals zero — same invariant, but in a shape ASSERT
-- can express.

ASSERT ROW_COUNT = 1
ASSERT VALUE row_count_delta = 0
ASSERT VALUE distinct_cca2_delta = 0
ASSERT VALUE silver_population BETWEEN 700000000 AND 800000000
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.travel_geo.european_countries)
        - (SELECT COUNT(*) FROM {{zone_name}}.travel_geo.european_countries_silver)
                                                                                      AS row_count_delta,
    (SELECT COUNT(DISTINCT cca2) FROM {{zone_name}}.travel_geo.european_countries)
        - (SELECT COUNT(DISTINCT cca2) FROM {{zone_name}}.travel_geo.european_countries_silver)
                                                                                      AS distinct_cca2_delta,
    (SELECT SUM(population) FROM {{zone_name}}.travel_geo.european_countries_silver)  AS silver_population;

-- ============================================================================
-- Query 8: Silver Delta Time-Travel — DESCRIBE HISTORY shows v0 + v1
-- ============================================================================
-- The Delta table got two writes during setup: the CREATE (v0, schema only)
-- and the INSERT (v1, the bronze→silver promotion). DESCRIBE HISTORY
-- exposes the transaction log and proves the table is queryable with
-- VERSION AS OF semantics — the headline Delta capability you don't get
-- from a bare external JSON table.

ASSERT ROW_COUNT >= 2
DESCRIBE HISTORY {{zone_name}}.travel_geo.european_countries_silver;

-- ============================================================================
-- Query 9: Silver Boolean Filter — typed columns enable native predicates
-- ============================================================================
-- Silver's `is_un_member` is a real BOOLEAN, so a `WHERE is_un_member = true`
-- filter works without casting — try the same on bronze and you get a
-- type mismatch (Utf8 vs Bool). This query is the on-demo proof that the
-- silver layer is the one downstream consumers want to query.
-- ~44 of the ~50 European countries are UN members (the dependencies +
-- Vatican City + Kosovo are not), so the count sits in the 40-50 band.

ASSERT ROW_COUNT = 1
ASSERT VALUE un_member_count BETWEEN 40 AND 50
SELECT COUNT(*) AS un_member_count
FROM {{zone_name}}.travel_geo.european_countries_silver
WHERE is_un_member = true;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting query exercising the whole pipeline: row count is
-- in range, Norway is present, total population is in range, AND the
-- silver Delta table is in sync with bronze. If this passes, the
-- credential resolved, the HTTPS fetch succeeded, the bronze write
-- landed, the JSON flatten produced the expected shape, AND the
-- bronze→silver promotion preserved every row. Aggregates run on
-- silver because that's where the typed columns live.

ASSERT ROW_COUNT = 1
ASSERT VALUE country_count BETWEEN 40 AND 60
ASSERT VALUE has_norway = 1
ASSERT VALUE total_population BETWEEN 700000000 AND 800000000
ASSERT VALUE region_invariant_holds = 1
ASSERT VALUE silver_matches_bronze = 1
SELECT
    COUNT(*)                                                                                   AS country_count,
    SUM(CASE WHEN cca2 = 'NO' THEN 1 ELSE 0 END)                                               AS has_norway,
    SUM(population)                                                                            AS total_population,
    CASE WHEN COUNT(DISTINCT region) = 1 AND MAX(region) = 'Europe' THEN 1 ELSE 0 END          AS region_invariant_holds,
    CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM {{zone_name}}.travel_geo.european_countries)
         THEN 1 ELSE 0 END                                                                     AS silver_matches_bronze
FROM {{zone_name}}.travel_geo.european_countries_silver;
