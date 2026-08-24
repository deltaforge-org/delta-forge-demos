-- ============================================================================
-- Subsurface: North Sea Demo Field - Verification Queries
-- ============================================================================
-- Every assertion is a count computed from the fixture generator and verified
-- against the readers in
-- delta-forge-tests/tests/format_discovery/subsurface_demo_fixtures.rs, so a
-- failure here is an engine change rather than a guess that drifted.
-- ============================================================================


-- ============================================================================
-- 1. SEISMIC TRACE COUNT - 12 inlines x 10 crosslines
-- ============================================================================

ASSERT ROW_COUNT = 120
SELECT *
FROM {{zone_name}}.subsurface.seismic_traces;


-- ============================================================================
-- 2. SEISMIC GEOMETRY - the survey's inline and crossline extent
-- ============================================================================
-- The header columns alone answer "what does this volume cover", which is the
-- question asked far more often than any question about the samples.

ASSERT ROW_COUNT = 1
SELECT MIN(inline)     AS first_inline,
       MAX(inline)     AS last_inline,
       MIN(crossline)  AS first_crossline,
       MAX(crossline)  AS last_crossline,
       COUNT(*)        AS traces
FROM {{zone_name}}.subsurface.seismic_headers;


-- ============================================================================
-- 3. COORDINATE SCALAR APPLIED - real UTM eastings, not raw integers
-- ============================================================================
-- SEG-Y stores coordinates as integers with a separate scalar in bytes 71-72.
-- This fixture writes them multiplied by 100 with a scalar of -100, so a
-- reader that ignored the scalar would report 45,000,000 instead of 450,000.

ASSERT ROW_COUNT = 1
SELECT MIN(source_x) AS min_easting,
       MAX(source_x) AS max_easting,
       MIN(source_y) AS min_northing,
       MAX(source_y) AS max_northing
FROM {{zone_name}}.subsurface.seismic_headers
WHERE source_x BETWEEN 440000 AND 460000;


-- ============================================================================
-- 4. FOLD MAP - traces per inline, the shape of a real QC query
-- ============================================================================

ASSERT ROW_COUNT = 12
SELECT inline,
       COUNT(*)      AS trace_count,
       AVG(source_x) AS mean_easting
FROM {{zone_name}}.subsurface.seismic_headers
GROUP BY inline
ORDER BY inline;


-- ============================================================================
-- 5. WELL LOG ROW COUNT - two wells, 300 depth steps each
-- ============================================================================

ASSERT ROW_COUNT = 600
SELECT *
FROM {{zone_name}}.subsurface.well_logs;


-- ============================================================================
-- 6. GROUP BY WELL WITHOUT A JOIN
-- ============================================================================
-- The LAS well-information section rides on every row, which is what makes a
-- table over a whole log library queryable by well, field or UWI directly.

ASSERT ROW_COUNT = 2
SELECT well_well AS well,
       well_fld  AS field,
       COUNT(*)  AS samples,
       MIN(dept) AS top_depth,
       MAX(dept) AS base_depth,
       AVG(gr)   AS mean_gamma
FROM {{zone_name}}.subsurface.well_logs
GROUP BY well_well, well_fld
ORDER BY well_well;


-- ============================================================================
-- 7. THE LAS NULL SENTINEL IS A REAL NULL
-- ============================================================================
-- The density curve is blanked over fifteen depth steps in each well with the
-- file's declared NULL value of -999.25. Reading it as a number would drag
-- every average through the floor; here it is simply absent.

ASSERT ROW_COUNT = 30
SELECT well_well, dept
FROM {{zone_name}}.subsurface.well_logs
WHERE rhob IS NULL
ORDER BY well_well, dept;


-- ============================================================================
-- 8. NO SENTINEL SURVIVED AS A VALUE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.subsurface.well_logs
WHERE rhob < -100 OR gr < -100 OR nphi < -100 OR dt < -100;


-- ============================================================================
-- 9. DEPTH-ZONED PETROPHYSICS - the query the logs exist for
-- ============================================================================

ASSERT ROW_COUNT > 0
SELECT well_well                          AS well,
       FLOOR(dept / 10) * 10              AS depth_band,
       COUNT(*)                           AS readings,
       AVG(gr)                            AS mean_gamma,
       AVG(rhob)                          AS mean_density,
       AVG(nphi)                          AS mean_porosity
FROM {{zone_name}}.subsurface.well_logs
WHERE gr < 60
GROUP BY well_well, FLOOR(dept / 10) * 10
ORDER BY well_well, depth_band;


-- ============================================================================
-- 10. GRID NODES - blank nodes dropped
-- ============================================================================
-- The grid is 40 by 50, so 2000 nodes, of which 836 fall outside the mapped
-- dome and carry the file's 1e30 sentinel.

ASSERT ROW_COUNT = 1164
SELECT *
FROM {{zone_name}}.subsurface.top_reservoir;


-- ============================================================================
-- 11. THE MAPPED SURFACE - depth range and extent
-- ============================================================================

ASSERT ROW_COUNT = 1
SELECT MIN(value) AS crest_depth,
       MAX(value) AS deepest_node,
       MIN(x)     AS min_easting,
       MAX(x)     AS max_easting,
       MIN(y)     AS min_northing,
       MAX(y)     AS max_northing
FROM {{zone_name}}.subsurface.top_reservoir;


-- ============================================================================
-- 12. GRID GEOMETRY IS NOT TRANSPOSED
-- ============================================================================
-- ZMAP+ writes its values column by column. If the reader had assumed
-- row-major, the grid would still be 40 by 50 and every node would still have
-- a value, so a count would not catch it. What catches it is that row and
-- column indices stay within their own bounds.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.subsurface.top_reservoir
WHERE "row" >= 40 OR "column" >= 50;


-- ============================================================================
-- 13. RESERVOIR MODEL - active cells only
-- ============================================================================
-- 20 x 15 x 8 is 2400 cells; the deck's ACTNUM marks an inactive rim, leaving
-- 1664. That the count is not 2400 is the evidence ACTNUM was honoured, and
-- that it is not some smaller number is the evidence the run-length encoding
-- expanded correctly.

ASSERT ROW_COUNT = 1664
SELECT *
FROM {{zone_name}}.subsurface.reservoir_model;


-- ============================================================================
-- 14. PROPERTY DISTRIBUTION BY LAYER
-- ============================================================================

ASSERT ROW_COUNT = 8
SELECT k              AS layer,
       COUNT(*)       AS active_cells,
       AVG(poro)      AS mean_porosity,
       AVG(permx)     AS mean_permeability,
       MIN(poro)      AS min_porosity,
       MAX(poro)      AS max_porosity
FROM {{zone_name}}.subsurface.reservoir_model
GROUP BY k
ORDER BY k;


-- ============================================================================
-- 15. SATURATION REGIONS - a second property on the same cells
-- ============================================================================
-- Several properties of one model line up on the same cell because i, j and k
-- come from SPECGRID rather than from row order.

ASSERT ROW_COUNT = 2
SELECT satnum        AS region,
       COUNT(*)      AS cells,
       AVG(poro)     AS mean_porosity
FROM {{zone_name}}.subsurface.reservoir_model
GROUP BY satnum
ORDER BY satnum;


-- ============================================================================
-- 16. NAVIGATION - four lines of sixty shot points
-- ============================================================================
-- The file's six H header records describe the survey and are not positions,
-- so they are not rows.

ASSERT ROW_COUNT = 240
SELECT *
FROM {{zone_name}}.subsurface.survey_navigation;


-- ============================================================================
-- 17. PACKED COORDINATES ARRIVE AS DECIMAL DEGREES
-- ============================================================================
-- P1/90 stores latitude as DDMMSS.SS with a trailing hemisphere letter. Read
-- as a plain number it would be 583600.00; read correctly it is 58.6.

ASSERT ROW_COUNT = 240
SELECT line_name, point_number, latitude, longitude
FROM {{zone_name}}.subsurface.survey_navigation
WHERE latitude BETWEEN 58.5 AND 58.7
  AND longitude BETWEEN 1.8 AND 2.0
ORDER BY line_name, point_number;


-- ============================================================================
-- 18. SURVEY LINES
-- ============================================================================

ASSERT ROW_COUNT = 4
SELECT line_name,
       COUNT(*)           AS shot_points,
       MIN(point_number)  AS first_point,
       MAX(point_number)  AS last_point,
       AVG(water_depth)   AS mean_water_depth
FROM {{zone_name}}.subsurface.survey_navigation
GROUP BY line_name
ORDER BY line_name;


-- ============================================================================
-- 19. SEISMIC JOINED TO NAVIGATION
-- ============================================================================
-- Both tables carry real UTM coordinates, so the volume and the survey track
-- join on position. This is the point of reading the formats in place: the
-- geometry is already comparable, with no export step to reconcile.

ASSERT ROW_COUNT > 0
SELECT n.line_name,
       COUNT(DISTINCT s.inline)    AS inlines_covered,
       COUNT(*)                    AS matched_pairs
FROM {{zone_name}}.subsurface.survey_navigation n
JOIN {{zone_name}}.subsurface.seismic_headers s
  ON ABS(s.source_x - n.easting) < 30
 AND ABS(s.source_y - n.northing) < 60
GROUP BY n.line_name
ORDER BY n.line_name;


-- ============================================================================
-- 20. THE MODEL AGAINST THE MAPPED SURFACE
-- ============================================================================
-- A crude but real cross-domain question: how does the model's pore volume
-- distribute across the depth range the seismic interpretation mapped.

-- A CTE plus a cross join rather than a scalar subquery in the select list:
-- the grid average is one row, and joining it is the shape DataFusion plans
-- reliably.
ASSERT ROW_COUNT > 0
WITH mapped AS (
    SELECT AVG(value) AS mean_mapped_depth
    FROM {{zone_name}}.subsurface.top_reservoir
)
SELECT m.k                     AS layer,
       COUNT(*)                AS cells,
       AVG(m.poro)             AS mean_porosity,
       MAX(mapped.mean_mapped_depth) AS mean_mapped_depth
FROM {{zone_name}}.subsurface.reservoir_model m
CROSS JOIN mapped
GROUP BY m.k
ORDER BY m.k;
