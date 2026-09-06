-- ============================================================================
-- RMS Horizon Handover: Irap Surfaces in Both Containers - Verification
-- ============================================================================
-- Three files, two containers, one geometry:
--
--   2026-04-08  TOP_HUGIN.gri    Irap binary, the form RMS stores
--   2026-04-08  TOP_HUGIN.irap   Irap classic ASCII, the same surface exported
--   2026-04-09  BASE_HUGIN.gri   the base horizon
--
-- Each surface is 60 columns by 45 rows on a 200 m spacing, rotated 24 degrees
-- counter-clockwise from east: 2700 nodes of which 1508 are mapped and 1192
-- were never interpreted. Every expected value below was decoded from these
-- files by Equinor's xtgeo 4.25.1 before the engine saw them.
--
-- Four properties of Irap decide whether a reader gets it right, and all four
-- are asserted here:
--
--   1. The value block runs X FASTEST, one whole row of constant Y at a time.
--      This is the reverse of ZMAP+. Read the other way an Irap grid
--      transposes: the node count matches and every value is in the wrong
--      place.
--   2. The ASCII header is NOT in the binary header's field order. The two
--      containers hold the same surface, so if a reader shares one field
--      table between them, they stop agreeing.
--   3. The undefined sentinel is a threshold, and it is a different number in
--      each container.
--   4. Row zero is the SOUTH edge, which is the opposite of ZMAP+.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- Both containers, one format, from the bytes rather than from the extension.

DISCOVER {{zone_name}}.geomodel.horizons
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE MAPPED NODES
-- ============================================================================
-- Three files of 1508 mapped nodes each: 4524 rows. The 1192 blank nodes per
-- surface are dropped, which is why this is not 8100.

ASSERT ROW_COUNT = 4524
SELECT *
FROM {{zone_name}}.geomodel.horizons;


-- ============================================================================
-- 3. THE SAME SURFACE IN TWO CONTAINERS
-- ============================================================================
-- This is the check that no single-file test can make. The binary header
-- writes nrow, then the origins and extents, then the increments; the ASCII
-- header writes nrow, then the INCREMENTS, then the origins. A reader that
-- shares one field table between the two forms passes every other query in
-- this file and fails here, because the ASCII surface would land at the
-- coordinate (200, 200) instead of at 458000 east.

ASSERT ROW_COUNT = 3
ASSERT VALUE nodes = 1508 WHERE df_file_name = '2026-04-08_top_hugin.gri'
ASSERT VALUE nodes = 1508 WHERE df_file_name = '2026-04-08_top_hugin.irap'
ASSERT VALUE nodes = 1508 WHERE df_file_name = '2026-04-09_base_hugin.gri'
SELECT df_file_name,
       COUNT(*) AS nodes
FROM {{zone_name}}.geomodel.horizons
GROUP BY df_file_name
ORDER BY df_file_name;


-- Node for node, the two containers agree on the depth AND on the computed
-- coordinate. The coordinate is the half that catches a swapped header field
-- order: the depths would still agree if a reader read the ASCII increments as
-- the ASCII origins, because the values are the same numbers either way.

ASSERT ROW_COUNT = 1
ASSERT VALUE matched_nodes = 1508
ASSERT VALUE depth_disagreements = 0
ASSERT VALUE coordinate_disagreements = 0
SELECT COUNT(*)                                                   AS matched_nodes,
       COUNT(*) FILTER (WHERE ABS(b.value - a.value) > 0.001)     AS depth_disagreements,
       COUNT(*) FILTER (WHERE ABS(b.x - a.x) > 0.001
                           OR ABS(b.y - a.y) > 0.001)             AS coordinate_disagreements
FROM {{zone_name}}.geomodel.horizons b
JOIN {{zone_name}}.geomodel.horizons a
  ON b."row" = a."row" AND b."column" = a."column"
WHERE b.df_file_name = '2026-04-08_top_hugin.gri'
  AND a.df_file_name = '2026-04-08_top_hugin.irap';


-- ============================================================================
-- 4. THE UNDEFINED SENTINEL BECAME A NULL, IN BOTH CONTAINERS
-- ============================================================================
-- The binary form blanks with 1e30 and the ASCII form with 9999900.0, and both
-- are thresholds rather than exact numbers. A sentinel that survives as a
-- value does not merely skew a mean, it replaces it.

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 2700
ASSERT VALUE mapped = 1508
ASSERT VALUE blank = 1192
ASSERT VALUE sentinel_survivors = 0
SELECT COUNT(*)                                     AS nodes,
       COUNT(*) FILTER (WHERE value IS NOT NULL)    AS mapped,
       COUNT(*) FILTER (WHERE value IS NULL)        AS blank,
       COUNT(*) FILTER (WHERE value > 1000000)      AS sentinel_survivors
FROM {{zone_name}}.geomodel.all_nodes;


-- ============================================================================
-- 5. THE GRID IS NOT TRANSPOSED
-- ============================================================================
-- 60 columns and 45 rows. Read column-major instead of row-major, every node
-- would still have a value and the count would still be 2700, so what catches
-- it is that the indices stay inside their own bounds and the mapped area
-- lands where it should: rows 4 to 41 and columns 3 to 52. The grid is 60 by
-- 45 rather than square on purpose, so a transposed read cannot succeed
-- quietly.

ASSERT ROW_COUNT = 1
ASSERT VALUE out_of_bounds = 0
ASSERT VALUE first_mapped_row = 4
ASSERT VALUE last_mapped_row = 41
ASSERT VALUE first_mapped_column = 3
ASSERT VALUE last_mapped_column = 52
SELECT COUNT(*) FILTER (WHERE "row" >= 45 OR "column" >= 60)  AS out_of_bounds,
       MIN("row")                                             AS first_mapped_row,
       MAX("row")                                             AS last_mapped_row,
       MIN("column")                                          AS first_mapped_column,
       MAX("column")                                          AS last_mapped_column
FROM {{zone_name}}.geomodel.horizons
WHERE df_file_name = '2026-04-08_top_hugin.gri';


-- ============================================================================
-- 6. THE COORDINATES WERE COMPUTED, ROTATION AND ALL
-- ============================================================================
-- The file holds an origin, a spacing and a rotation, and nothing else about
-- where it sits. A reader that ignores the 24 degree rotation would put the
-- west edge exactly on the origin easting of 458000; the rotation carries it
-- 3579 m further west, because the grid's own Y axis leans that way. The
-- south edge stays exactly on the origin northing, because node (0, 0) IS the
-- origin: row zero is the SOUTH edge in Irap, where ZMAP+ counts from the
-- north.

ASSERT ROW_COUNT = 1
ASSERT VALUE west_m = 454421
ASSERT VALUE east_m = 468780
ASSERT VALUE south_m = 6785000
ASSERT VALUE north_m = 6797839
ASSERT VALUE origin_node_is_the_south_west_corner = 1
SELECT CAST(ROUND(MIN(x)) AS BIGINT) AS west_m,
       CAST(ROUND(MAX(x)) AS BIGINT) AS east_m,
       CAST(ROUND(MIN(y)) AS BIGINT) AS south_m,
       CAST(ROUND(MAX(y)) AS BIGINT) AS north_m,
       COUNT(*) FILTER (WHERE "row" = 0 AND "column" = 0
                          AND ROUND(x) = 458000 AND ROUND(y) = 6785000)
                                     AS origin_node_is_the_south_west_corner
FROM {{zone_name}}.geomodel.all_nodes;


-- ============================================================================
-- 7. LOAD THE TOP HORIZON FROM THE BINARY CONTAINER
-- ============================================================================

INSERT INTO {{zone_name}}.geomodel.surfaces
SELECT 'TOP_HUGIN'         AS surface,
       'irap binary'       AS container,
       '2026-04-08'        AS delivered_on,
       h.df_file_name      AS source_file,
       h."row"             AS grid_row,
       h."column"          AS grid_column,
       h.x,
       h.y,
       h.value             AS depth_m
FROM {{zone_name}}.geomodel.horizons h
WHERE h.df_file_name = '2026-04-08_top_hugin.gri'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.geomodel.surfaces s
      WHERE s.source_file = h.df_file_name
  );


-- ============================================================================
-- 8. THE TOP HORIZON LANDED
-- ============================================================================
-- The crest of the structure is at 2215 m and the deepest mapped node at 2380,
-- which is 165 m of relief.

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 1508
ASSERT VALUE crest_m = 2215
ASSERT VALUE deepest_m = 2380
ASSERT VALUE relief_m = 165
SELECT COUNT(*)                                            AS nodes,
       CAST(ROUND(MIN(depth_m)) AS BIGINT)                 AS crest_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT)                 AS deepest_m,
       CAST(ROUND(MAX(depth_m) - MIN(depth_m)) AS BIGINT)  AS relief_m
FROM {{zone_name}}.geomodel.surfaces
WHERE surface = 'TOP_HUGIN';


-- ============================================================================
-- 9. THE SAME LOAD AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.geomodel.surfaces
SELECT 'TOP_HUGIN'         AS surface,
       'irap binary'       AS container,
       '2026-04-08'        AS delivered_on,
       h.df_file_name      AS source_file,
       h."row"             AS grid_row,
       h."column"          AS grid_column,
       h.x,
       h.y,
       h.value             AS depth_m
FROM {{zone_name}}.geomodel.horizons h
WHERE h.df_file_name = '2026-04-08_top_hugin.gri'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.geomodel.surfaces s
      WHERE s.source_file = h.df_file_name
  );


-- ============================================================================
-- 10. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 1508
SELECT COUNT(*) AS nodes
FROM {{zone_name}}.geomodel.surfaces
WHERE surface = 'TOP_HUGIN';


-- ============================================================================
-- 11. LOAD THE BASE HORIZON
-- ============================================================================

INSERT INTO {{zone_name}}.geomodel.surfaces
SELECT 'BASE_HUGIN'        AS surface,
       'irap binary'       AS container,
       '2026-04-09'        AS delivered_on,
       h.df_file_name      AS source_file,
       h."row"             AS grid_row,
       h."column"          AS grid_column,
       h.x,
       h.y,
       h.value             AS depth_m
FROM {{zone_name}}.geomodel.horizons h
WHERE h.df_file_name = '2026-04-09_base_hugin.gri'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.geomodel.surfaces s
      WHERE s.source_file = h.df_file_name
  );


-- ============================================================================
-- 12. BOTH HORIZONS
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE nodes = 1508 WHERE surface = 'TOP_HUGIN'
ASSERT VALUE nodes = 1508 WHERE surface = 'BASE_HUGIN'
ASSERT VALUE crest_m = 2215 WHERE surface = 'TOP_HUGIN'
ASSERT VALUE crest_m = 2442 WHERE surface = 'BASE_HUGIN'
SELECT surface,
       MIN(delivered_on)                   AS delivered_on,
       COUNT(*)                            AS nodes,
       CAST(ROUND(MIN(depth_m)) AS BIGINT) AS crest_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT) AS deepest_m
FROM {{zone_name}}.geomodel.surfaces
GROUP BY surface
ORDER BY surface;


-- ============================================================================
-- 13. THE ISOCHORE
-- ============================================================================
-- The two horizons line up node for node because they share one grid
-- definition, so the thickness between them is a subtraction rather than an
-- interpolation. It runs from 180 m at the flanks to 227 m over the crest.

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 1508
ASSERT VALUE thinnest_m = 180
ASSERT VALUE thickest_m = 227
SELECT COUNT(*)                                              AS nodes,
       CAST(ROUND(MIN(b.depth_m - t.depth_m)) AS BIGINT)     AS thinnest_m,
       CAST(ROUND(MAX(b.depth_m - t.depth_m)) AS BIGINT)     AS thickest_m
FROM {{zone_name}}.geomodel.surfaces t
JOIN {{zone_name}}.geomodel.surfaces b
  ON t.grid_row = b.grid_row AND t.grid_column = b.grid_column
WHERE t.surface = 'TOP_HUGIN' AND b.surface = 'BASE_HUGIN';


-- ============================================================================
-- 14. GROSS ROCK VOLUME
-- ============================================================================
-- Each node stands for one 200 by 200 m cell, so the volume is the summed
-- thickness times 40000 square metres: 12169 million cubic metres of rock.
-- This is the number the whole handover exists to produce, and it depends on
-- every property asserted above: transpose the grid and the two horizons stop
-- lining up, let a sentinel through and the sum is meaningless, drop the
-- rotation and the volume is right while the map of it is not.

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 1508
ASSERT VALUE gross_rock_volume_m3 = 12168580000
ASSERT VALUE gross_rock_volume_mm3 = 12169
SELECT COUNT(*)                                                       AS nodes,
       CAST(ROUND(SUM(b.depth_m - t.depth_m) * 40000) AS BIGINT)      AS gross_rock_volume_m3,
       CAST(ROUND(SUM(b.depth_m - t.depth_m) * 40000 / 1000000) AS BIGINT)
                                                                      AS gross_rock_volume_mm3
FROM {{zone_name}}.geomodel.surfaces t
JOIN {{zone_name}}.geomodel.surfaces b
  ON t.grid_row = b.grid_row AND t.grid_column = b.grid_column
WHERE t.surface = 'TOP_HUGIN' AND b.surface = 'BASE_HUGIN';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The handover as a modeller would sign it off: both horizons, which container
-- each came out of, where each sits, and the extent they share.
--
-- The shared extent is the point of the last two columns. Both surfaces report
-- the same west and north edge because they are the same grid definition, and
-- that is what makes the isochore above a subtraction rather than an
-- interpolation. Note it is the MAPPED extent, 456453 east and 6795400 north,
-- inside the grid's own 454421 to 468780 and up to 6797839: the difference is
-- the blank margin nobody interpreted. Both edges also prove the rotation was
-- applied, because an unrotated grid could not reach west of its own origin at
-- 458000.

ASSERT ROW_COUNT = 2
ASSERT VALUE nodes = 1508 WHERE surface = 'TOP_HUGIN'
ASSERT VALUE container = 'irap binary' WHERE surface = 'TOP_HUGIN'
ASSERT VALUE delivered_on = '2026-04-08' WHERE surface = 'TOP_HUGIN'
ASSERT VALUE crest_m = 2215 WHERE surface = 'TOP_HUGIN'
ASSERT VALUE deepest_m = 2380 WHERE surface = 'TOP_HUGIN'
ASSERT VALUE nodes = 1508 WHERE surface = 'BASE_HUGIN'
ASSERT VALUE delivered_on = '2026-04-09' WHERE surface = 'BASE_HUGIN'
ASSERT VALUE crest_m = 2442 WHERE surface = 'BASE_HUGIN'
ASSERT VALUE deepest_m = 2560 WHERE surface = 'BASE_HUGIN'
ASSERT VALUE west_m = 456453 WHERE surface = 'TOP_HUGIN'
ASSERT VALUE west_m = 456453 WHERE surface = 'BASE_HUGIN'
ASSERT VALUE north_m = 6795400 WHERE surface = 'TOP_HUGIN'
ASSERT VALUE north_m = 6795400 WHERE surface = 'BASE_HUGIN'
SELECT surface,
       MIN(container)                       AS container,
       MIN(delivered_on)                    AS delivered_on,
       COUNT(*)                             AS nodes,
       CAST(ROUND(MIN(depth_m)) AS BIGINT)  AS crest_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT)  AS deepest_m,
       CAST(ROUND(MIN(x)) AS BIGINT)        AS west_m,
       CAST(ROUND(MAX(y)) AS BIGINT)        AS north_m
FROM {{zone_name}}.geomodel.surfaces
GROUP BY surface
ORDER BY surface;
