-- ============================================================================
-- Licence Block Acreage and Relinquishment - Incremental Load and Verification
-- ============================================================================
-- Two licensing rounds, delivered a day apart:
--
--   2026-03-11  APA-2025   quadrants 15 and 16, 7 blocks
--   2026-03-12  APA-2026   quadrant 25, 5 blocks
--
-- 12 blocks in total. Every value below was parsed out of the documents
-- independently before the engine saw them.
--
-- The blocks sit on the real Norwegian quadrant grid, one degree of latitude
-- by two of longitude divided into twelve, so each is a genuine graticule
-- rectangle and its area is the area that rectangle has at that latitude.
--
-- GeoJSON is read through the JSON engine under a curated profile. The
-- profile keeps `geometry` whole rather than flattening it, which is the
-- interesting part: a polygon's coordinate array flattened would produce a
-- column per vertex, and a schema that changes shape with the number of
-- corners in a block is not a schema.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- USING GEOJSON, not USING JSON. A GeoJSON document is valid JSON, so
-- detection needs the type declaration and a geometry rather than the
-- extension, which these files share with every other JSON file on disk.

DISCOVER {{zone_name}}.licensing.block_awards
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ONE ROW PER FEATURE
-- ============================================================================
-- Three feature collections, twelve features.

ASSERT ROW_COUNT = 12
SELECT *
FROM {{zone_name}}.licensing.block_awards;


-- ============================================================================
-- 3. THE GEOMETRY IS KEPT WHOLE
-- ============================================================================
-- Every block carries its polygon as one value rather than as a spray of
-- coordinate columns. All twelve are polygons, and all twelve survived as
-- something that still says so.

ASSERT ROW_COUNT = 1
ASSERT VALUE blocks = 12
ASSERT VALUE with_geometry = 12
ASSERT VALUE polygons = 12
SELECT COUNT(*)                                              AS blocks,
       COUNT(*) FILTER (WHERE geometry IS NOT NULL)          AS with_geometry,
       COUNT(*) FILTER (WHERE STRPOS(geometry, 'Polygon') > 0) AS polygons
FROM {{zone_name}}.licensing.block_awards;


-- ============================================================================
-- 4. THE AWARDS AS FILED
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE blocks = 4 WHERE df_file_name = '2026-03-11_APA-2025_quadrant_15.geojson'
ASSERT VALUE blocks = 3 WHERE df_file_name = '2026-03-11_APA-2025_quadrant_16.geojson'
ASSERT VALUE blocks = 5 WHERE df_file_name = '2026-03-12_APA-2026_quadrant_25.geojson'
SELECT df_file_name,
       COUNT(*)                        AS blocks,
       COUNT(DISTINCT properties_licence) AS licences
FROM {{zone_name}}.licensing.block_awards
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 5. LOAD THE APA-2025 AWARDS
-- ============================================================================

INSERT INTO {{zone_name}}.licensing.licence_blocks
SELECT b.properties_block                                AS block,
       CAST(b.properties_quadrant AS INTEGER)            AS quadrant,
       b.properties_licence                              AS licence,
       b.properties_operator                             AS operator,
       b.properties_licence_round                        AS licence_round,
       CAST(b.properties_awarded_year AS INTEGER)        AS awarded_year,
       CAST(b.properties_term_years AS INTEGER)          AS term_years,
       CAST(b.properties_relinquish_by AS INTEGER)       AS relinquish_by,
       b.properties_work_commitment                      AS work_commitment,
       CAST(b.properties_area_km2 AS DOUBLE)             AS area_km2,
       b.geometry,
       '2026-03-11'                                      AS delivered_on,
       b.df_file_name                                    AS source_file
FROM {{zone_name}}.licensing.block_awards b
WHERE b.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.licensing.licence_blocks c
      WHERE c.source_file = b.df_file_name
  );


-- ============================================================================
-- 6. THE FIRST ROUND LANDED
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE blocks = 7
ASSERT VALUE area_km2 = 7535
ASSERT VALUE quadrants = 2
ASSERT VALUE licences = 4
SELECT COUNT(*)                                 AS blocks,
       CAST(ROUND(SUM(area_km2)) AS BIGINT)     AS area_km2,
       COUNT(DISTINCT quadrant)                 AS quadrants,
       COUNT(DISTINCT licence)                  AS licences
FROM {{zone_name}}.licensing.licence_blocks
WHERE licence_round = 'APA-2025';


-- ============================================================================
-- 7. THE SAME ROUND AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.licensing.licence_blocks
SELECT b.properties_block                                AS block,
       CAST(b.properties_quadrant AS INTEGER)            AS quadrant,
       b.properties_licence                              AS licence,
       b.properties_operator                             AS operator,
       b.properties_licence_round                        AS licence_round,
       CAST(b.properties_awarded_year AS INTEGER)        AS awarded_year,
       CAST(b.properties_term_years AS INTEGER)          AS term_years,
       CAST(b.properties_relinquish_by AS INTEGER)       AS relinquish_by,
       b.properties_work_commitment                      AS work_commitment,
       CAST(b.properties_area_km2 AS DOUBLE)             AS area_km2,
       b.geometry,
       '2026-03-11'                                      AS delivered_on,
       b.df_file_name                                    AS source_file
FROM {{zone_name}}.licensing.block_awards b
WHERE b.df_file_name LIKE '2026-03-11%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.licensing.licence_blocks c
      WHERE c.source_file = b.df_file_name
  );


-- ============================================================================
-- 8. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE blocks = 7
ASSERT VALUE documents = 2
SELECT COUNT(*)                    AS blocks,
       COUNT(DISTINCT source_file) AS documents
FROM {{zone_name}}.licensing.licence_blocks
WHERE delivered_on = '2026-03-11';


-- ============================================================================
-- 9. LOAD THE APA-2026 AWARDS
-- ============================================================================

INSERT INTO {{zone_name}}.licensing.licence_blocks
SELECT b.properties_block                                AS block,
       CAST(b.properties_quadrant AS INTEGER)            AS quadrant,
       b.properties_licence                              AS licence,
       b.properties_operator                             AS operator,
       b.properties_licence_round                        AS licence_round,
       CAST(b.properties_awarded_year AS INTEGER)        AS awarded_year,
       CAST(b.properties_term_years AS INTEGER)          AS term_years,
       CAST(b.properties_relinquish_by AS INTEGER)       AS relinquish_by,
       b.properties_work_commitment                      AS work_commitment,
       CAST(b.properties_area_km2 AS DOUBLE)             AS area_km2,
       b.geometry,
       '2026-03-12'                                      AS delivered_on,
       b.df_file_name                                    AS source_file
FROM {{zone_name}}.licensing.block_awards b
WHERE b.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.licensing.licence_blocks c
      WHERE c.source_file = b.df_file_name
  );


-- ============================================================================
-- 10. THE ACREAGE PORTFOLIO
-- ============================================================================
-- Who holds what. DeltaForge Energy is on six blocks across four licences and
-- 6405 square kilometres, which is half the acreage in the two rounds.

ASSERT ROW_COUNT = 3
ASSERT VALUE blocks = 6 WHERE operator = 'DeltaForge Energy'
ASSERT VALUE area_km2 = 6405 WHERE operator = 'DeltaForge Energy'
ASSERT VALUE licences = 4 WHERE operator = 'DeltaForge Energy'
ASSERT VALUE blocks = 4 WHERE operator = 'Havlys Exploration'
ASSERT VALUE area_km2 = 4255 WHERE operator = 'Havlys Exploration'
ASSERT VALUE blocks = 2 WHERE operator = 'Nordfjell Petroleum'
ASSERT VALUE area_km2 = 2135 WHERE operator = 'Nordfjell Petroleum'
SELECT operator,
       COUNT(*)                                 AS blocks,
       COUNT(DISTINCT licence)                  AS licences,
       CAST(ROUND(SUM(area_km2)) AS BIGINT)     AS area_km2
FROM {{zone_name}}.licensing.licence_blocks
GROUP BY operator
ORDER BY operator;


-- ============================================================================
-- 11. THE RELINQUISHMENT SCHEDULE
-- ============================================================================
-- The query the whole load exists for. A licence hands acreage back at the
-- end of its term, and missing the date costs the block rather than the
-- obligation. Four blocks and 4270 square kilometres fall due by 2030, which
-- is what a work-programme meeting needs to know before it plans anything.

ASSERT ROW_COUNT = 4
ASSERT VALUE blocks = 3 WHERE relinquish_by = 2029
ASSERT VALUE blocks = 1 WHERE relinquish_by = 2030
ASSERT VALUE blocks = 4 WHERE relinquish_by = 2031
ASSERT VALUE blocks = 4 WHERE relinquish_by = 2032
SELECT relinquish_by,
       COUNT(*)                                 AS blocks,
       COUNT(DISTINCT licence)                  AS licences,
       CAST(ROUND(SUM(area_km2)) AS BIGINT)     AS area_km2
FROM {{zone_name}}.licensing.licence_blocks
GROUP BY relinquish_by
ORDER BY relinquish_by;


-- ============================================================================
-- 12. WHAT FALLS DUE FIRST
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE blocks_due = 4
ASSERT VALUE area_due_km2 = 4270
SELECT COUNT(*)                                 AS blocks_due,
       CAST(ROUND(SUM(area_km2)) AS BIGINT)     AS area_due_km2,
       COUNT(DISTINCT operator)                 AS operators_affected
FROM {{zone_name}}.licensing.licence_blocks
WHERE relinquish_by <= 2030;


-- ============================================================================
-- 13. THE WHOLE PORTFOLIO
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE blocks = 12
ASSERT VALUE area_km2 = 12795
ASSERT VALUE rounds = 2
ASSERT VALUE quadrants = 3
SELECT COUNT(*)                                 AS blocks,
       CAST(ROUND(SUM(area_km2)) AS BIGINT)     AS area_km2,
       COUNT(DISTINCT licence_round)            AS rounds,
       COUNT(DISTINCT quadrant)                 AS quadrants,
       COUNT(DISTINCT operator)                 AS operators
FROM {{zone_name}}.licensing.licence_blocks;


-- ============================================================================
-- 14. EVERY DOCUMENT LANDED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT c.source_file, c.curated, b.landed
FROM (
    SELECT source_file, COUNT(*) AS curated
    FROM {{zone_name}}.licensing.licence_blocks
    GROUP BY source_file
) c
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.licensing.block_awards
    GROUP BY df_file_name
) b
  ON b.df_file_name = c.source_file
WHERE c.curated <> b.landed;


-- ============================================================================
-- 15. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT b.df_file_name
FROM {{zone_name}}.licensing.block_awards b
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.licensing.licence_blocks c
    WHERE c.source_file = b.df_file_name
);


-- ============================================================================
-- 16. THE STATE AFTER THE FIRST ROUND, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 7
SELECT *
FROM {{zone_name}}.licensing.licence_blocks VERSION AS OF 1;


-- ============================================================================
-- 17. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.licensing.licence_blocks;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The portfolio as a licence manager would review it: acreage by round, when
-- it has to go back, and the geometry still attached to every block.

ASSERT ROW_COUNT = 2
ASSERT VALUE blocks = 7 WHERE licence_round = 'APA-2025'
ASSERT VALUE area_km2 = 7535 WHERE licence_round = 'APA-2025'
ASSERT VALUE delivered_on = '2026-03-11' WHERE licence_round = 'APA-2025'
ASSERT VALUE with_geometry = 7 WHERE licence_round = 'APA-2025'
ASSERT VALUE blocks = 5 WHERE licence_round = 'APA-2026'
ASSERT VALUE area_km2 = 5261 WHERE licence_round = 'APA-2026'
ASSERT VALUE delivered_on = '2026-03-12' WHERE licence_round = 'APA-2026'
ASSERT VALUE with_geometry = 5 WHERE licence_round = 'APA-2026'
SELECT licence_round,
       MIN(delivered_on)                            AS delivered_on,
       COUNT(*)                                     AS blocks,
       COUNT(DISTINCT quadrant)                     AS quadrants,
       CAST(ROUND(SUM(area_km2)) AS BIGINT)         AS area_km2,
       MIN(relinquish_by)                           AS first_relinquishment,
       COUNT(*) FILTER (WHERE geometry IS NOT NULL) AS with_geometry
FROM {{zone_name}}.licensing.licence_blocks
GROUP BY licence_round
ORDER BY licence_round;
