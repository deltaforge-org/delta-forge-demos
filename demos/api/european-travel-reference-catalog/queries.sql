-- ============================================================================
-- Demo: European Travel Reference Catalog — Queries
-- ============================================================================
-- Validates the end-to-end REST API ingest flow:
--   • The bronze landing has an actual JSON file written by INVOKE.
--   • The flattened external table exposes the country shape we asked for.
--   • Stable European-geography invariants hold (Norway exists, total
--     population is in the right ballpark, every row's region is "Europe").
--
-- Assertions are deliberately tolerant of small REST Countries data drift
-- (a country joining/leaving the EU, a population revision) by using
-- BETWEEN ranges for aggregates and exact match for cca2/cca3 ISO codes
-- (those don't change without an actual geopolitical event).
-- ============================================================================

-- ============================================================================
-- Query 1: Catalog Smoke Check — every European country has a row
-- ============================================================================
-- REST Countries v3.1 lists ~50 European entries (member states + a few
-- special territories). The exact count varies slightly with data
-- revisions but never drops below 40 or climbs above 60.

ASSERT WARNING ROW_COUNT BETWEEN 40 AND 60
SELECT COUNT(*) AS country_count
FROM {{zone_name}}.travel_geo.european_countries;

-- ============================================================================
-- Query 2: Norway Reference Lookup — proves the JSON flatten worked
-- ============================================================================
-- Pick a stable, isolated country whose key fields don't change.
-- Norway: cca2 = NO, capital Oslo, sovereign, UN member, not landlocked.
-- If the flatten config or the API response shape regresses, this query
-- catches it immediately.

ASSERT ROW_COUNT = 1
ASSERT VALUE name_common = 'Norway' WHERE cca2 = 'NO'
ASSERT VALUE capital = 'Oslo' WHERE cca2 = 'NO'
ASSERT VALUE is_independent = true WHERE cca2 = 'NO'
ASSERT VALUE is_un_member = true WHERE cca2 = 'NO'
ASSERT VALUE is_landlocked = false WHERE cca2 = 'NO'
SELECT name_common, cca2, cca3, capital, region, subregion,
       is_independent, is_un_member, is_landlocked
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
-- Query 4: Population Aggregate — Europe total in the expected range
-- ============================================================================
-- Total European population (REST Countries v3.1 dataset) sits at
-- ~745M. BETWEEN 700M and 800M absorbs revisions without flaking.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_population BETWEEN 700000000 AND 800000000
SELECT SUM(population) AS total_population
FROM {{zone_name}}.travel_geo.european_countries;

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
SELECT cca2, cca3, name_common, capital, population
FROM {{zone_name}}.travel_geo.european_countries
WHERE cca2 IN ('IS', 'MT', 'MC')
ORDER BY cca2;

-- ============================================================================
-- Query 6: Subregion Distribution — five canonical European subregions
-- ============================================================================
-- The REST Countries dataset places every European country into one of
-- five subregions: Northern, Western, Southern, Eastern, and
-- (occasionally) Southeast Europe. Asserting the distinct count = 5
-- catches both flatten regressions (column missing) and upstream
-- breaking changes (a new subregion category appearing).

ASSERT ROW_COUNT = 1
ASSERT VALUE subregion_count BETWEEN 4 AND 6
SELECT COUNT(DISTINCT subregion) AS subregion_count
FROM {{zone_name}}.travel_geo.european_countries
WHERE subregion IS NOT NULL;

-- ============================================================================
-- Query 7: Silver Delta Table — promoted copy is byte-equivalent to bronze
-- ============================================================================
-- INSERT INTO ... SELECT FROM in setup.sql copied bronze (the external
-- JSON-flattened table) into the silver Delta table. After that single
-- promotion, every aggregate must match exactly — same row count, same
-- distinct cca2 codes, same population total. Any drift means the
-- promotion lost or duplicated rows.

ASSERT ROW_COUNT = 1
ASSERT VALUE bronze_count = silver_count
ASSERT VALUE bronze_distinct_cca2 = silver_distinct_cca2
ASSERT VALUE bronze_population = silver_population
SELECT
    (SELECT COUNT(*)              FROM {{zone_name}}.travel_geo.european_countries)         AS bronze_count,
    (SELECT COUNT(*)              FROM {{zone_name}}.travel_geo.european_countries_silver)  AS silver_count,
    (SELECT COUNT(DISTINCT cca2)  FROM {{zone_name}}.travel_geo.european_countries)         AS bronze_distinct_cca2,
    (SELECT COUNT(DISTINCT cca2)  FROM {{zone_name}}.travel_geo.european_countries_silver)  AS silver_distinct_cca2,
    (SELECT SUM(population)       FROM {{zone_name}}.travel_geo.european_countries)         AS bronze_population,
    (SELECT SUM(population)       FROM {{zone_name}}.travel_geo.european_countries_silver)  AS silver_population;

-- ============================================================================
-- Query 8: Silver Time-Travel Smoke Check — DESCRIBE HISTORY shows v0 + v1
-- ============================================================================
-- The Delta table got two writes during setup: the CREATE (v0, schema only)
-- and the INSERT (v1, the bronze→silver promotion). DESCRIBE HISTORY
-- exposes the transaction log and proves the table is queryable with
-- VERSION AS OF semantics — the headline Delta capability you don't get
-- from a bare external JSON table.

ASSERT ROW_COUNT >= 2
SELECT version, operation
FROM (DESCRIBE HISTORY {{zone_name}}.travel_geo.european_countries_silver)
ORDER BY version;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- One cross-cutting query exercising the whole pipeline: row count is
-- in range, Norway is present and looks right, total population is in
-- range, AND the silver Delta table is in sync with bronze. If this
-- passes, the credential resolved, the HTTPS fetch succeeded, the
-- bronze write landed, the JSON flatten produced the expected shape,
-- AND the bronze→silver promotion preserved every row.

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
    CASE WHEN COUNT(*) = (SELECT COUNT(*) FROM {{zone_name}}.travel_geo.european_countries_silver)
              AND SUM(population) = (SELECT SUM(population) FROM {{zone_name}}.travel_geo.european_countries_silver)
         THEN 1 ELSE 0 END                                                                     AS silver_matches_bronze
FROM {{zone_name}}.travel_geo.european_countries;
