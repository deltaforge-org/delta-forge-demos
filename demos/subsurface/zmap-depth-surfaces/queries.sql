-- ============================================================================
-- Depth Surface Handover and Gross Rock Volume - Load and Verification
-- ============================================================================
-- Three ZMAP+ grids delivered over two days:
--
--   2026-03-11  TOP_HUGIN_V1   the first depth conversion
--   2026-03-12  TOP_HUGIN_V2   the same top after the velocity model was
--                              corrected, and BASE_HUGIN
--
-- Each grid is 45 rows by 60 columns on a 200 m spacing: 2700 nodes, of which
-- 1441 fall inside the mapped polygon and 1259 were never interpreted. Every
-- value below was decoded from the files by a second, independent reader
-- before the engine saw them.
--
-- Two things about ZMAP+ decide whether a reader gets it right, and both are
-- asserted here:
--
--   1. Values run COLUMN BY COLUMN, not row by row. A reader that assumes
--      row-major produces a grid of exactly the right size where every node
--      holds a neighbour's value, and no row count catches it.
--   2. Nodes outside the polygon carry a 1e30 sentinel. Read as a number it
--      is not merely wrong, it is 1e30, and one of them in an average
--      destroys the answer.
--
-- The grid is 45 by 60 rather than square on purpose, so a transposed read
-- runs off the end rather than quietly succeeding.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.mapping.depth_grids
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE MAPPED NODES
-- ============================================================================
-- Three grids of 1441 interpreted nodes each: 4323 rows. The 1259 blank nodes
-- per grid are dropped, which is why this is not 8100.

ASSERT ROW_COUNT = 4323
SELECT *
FROM {{zone_name}}.mapping.depth_grids;


-- ============================================================================
-- 3. THE SAME GRID WITH ITS BLANKS KEPT
-- ============================================================================
-- 2700 nodes, of which 1441 carry a depth and 1259 are NULL. The sentinel
-- became a real null rather than staying 1e30, which is the difference
-- between a grid you can average and one you cannot.

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 2700
ASSERT VALUE mapped = 1441
ASSERT VALUE blank = 1259
ASSERT VALUE sentinel_survivors = 0
SELECT COUNT(*)                                     AS nodes,
       COUNT(*) FILTER (WHERE value IS NOT NULL)    AS mapped,
       COUNT(*) FILTER (WHERE value IS NULL)        AS blank,
       COUNT(*) FILTER (WHERE value > 1000000)      AS sentinel_survivors
FROM {{zone_name}}.mapping.all_nodes;


-- ============================================================================
-- 4. THE GRID IS NOT TRANSPOSED
-- ============================================================================
-- 45 rows and 60 columns. Read row-major instead of column-major, every node
-- would still have a value and the count would still be 2700, so what catches
-- it is that the indices stay inside their own bounds and the mapped polygon
-- lands where it should: rows 3 to 41 and columns 4 to 50.

ASSERT ROW_COUNT = 1
ASSERT VALUE out_of_bounds = 0
ASSERT VALUE first_mapped_row = 3
ASSERT VALUE last_mapped_row = 41
ASSERT VALUE first_mapped_column = 4
ASSERT VALUE last_mapped_column = 50
SELECT COUNT(*) FILTER (WHERE "row" >= 45 OR "column" >= 60)  AS out_of_bounds,
       MIN("row")                                             AS first_mapped_row,
       MAX("row")                                             AS last_mapped_row,
       MIN("column")                                          AS first_mapped_column,
       MAX("column")                                          AS last_mapped_column
FROM {{zone_name}}.mapping.depth_grids
WHERE df_file_name = '2026-03-11_top_hugin_v1.zmap';


-- ============================================================================
-- 5. THE COORDINATES WERE COMPUTED FROM THE HEADER
-- ============================================================================
-- The file holds no coordinates: it holds an extent and a run of numbers.
-- 200 m spacing in both directions across 60 columns and 45 rows gives an
-- 11800 by 8800 metre grid, and the corner is where the header says.

ASSERT ROW_COUNT = 1
ASSERT VALUE west_m = 460000
ASSERT VALUE east_m = 471800
ASSERT VALUE south_m = 6540000
ASSERT VALUE north_m = 6548800
SELECT CAST(ROUND(MIN(x)) AS BIGINT) AS west_m,
       CAST(ROUND(MAX(x)) AS BIGINT) AS east_m,
       CAST(ROUND(MIN(y)) AS BIGINT) AS south_m,
       CAST(ROUND(MAX(y)) AS BIGINT) AS north_m
FROM {{zone_name}}.mapping.all_nodes;


-- ============================================================================
-- 6. LOAD THE FIRST DEPTH CONVERSION
-- ============================================================================

INSERT INTO {{zone_name}}.mapping.depth_surfaces
SELECT 'TOP_HUGIN'         AS surface,
       1                   AS iteration,
       '2026-03-11'        AS delivered_on,
       g.df_file_name      AS source_file,
       g."row"             AS grid_row,
       g."column"          AS grid_column,
       g.x,
       g.y,
       g.value             AS depth_m
FROM {{zone_name}}.mapping.depth_grids g
WHERE g.df_file_name = '2026-03-11_top_hugin_v1.zmap'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.mapping.depth_surfaces s
      WHERE s.source_file = g.df_file_name
  );


-- ============================================================================
-- 7. THE FIRST SURFACE LANDED
-- ============================================================================
-- The crest of the structure is at 2440 m and the deepest mapped node at
-- 2585, which is 145 m of relief.

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 1441
ASSERT VALUE crest_m = 2440
ASSERT VALUE deepest_m = 2585
ASSERT VALUE relief_m = 145
SELECT COUNT(*)                                            AS nodes,
       CAST(ROUND(MIN(depth_m)) AS BIGINT)                 AS crest_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT)                 AS deepest_m,
       CAST(ROUND(MAX(depth_m) - MIN(depth_m)) AS BIGINT)  AS relief_m
FROM {{zone_name}}.mapping.depth_surfaces
WHERE iteration = 1;


-- ============================================================================
-- 8. THE SAME GRID AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.mapping.depth_surfaces
SELECT 'TOP_HUGIN'         AS surface,
       1                   AS iteration,
       '2026-03-11'        AS delivered_on,
       g.df_file_name      AS source_file,
       g."row"             AS grid_row,
       g."column"          AS grid_column,
       g.x,
       g.y,
       g.value             AS depth_m
FROM {{zone_name}}.mapping.depth_grids g
WHERE g.df_file_name = '2026-03-11_top_hugin_v1.zmap'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.mapping.depth_surfaces s
      WHERE s.source_file = g.df_file_name
  );


-- ============================================================================
-- 9. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 1441
SELECT COUNT(*) AS nodes
FROM {{zone_name}}.mapping.depth_surfaces
WHERE iteration = 1;


-- ============================================================================
-- 10. LOAD THE CORRECTED TOP AND THE BASE
-- ============================================================================

INSERT INTO {{zone_name}}.mapping.depth_surfaces
SELECT CASE WHEN STRPOS(g.df_file_name, 'base') > 0
            THEN 'BASE_HUGIN' ELSE 'TOP_HUGIN' END  AS surface,
       2                                            AS iteration,
       '2026-03-12'                                 AS delivered_on,
       g.df_file_name                               AS source_file,
       g."row"                                      AS grid_row,
       g."column"                                   AS grid_column,
       g.x,
       g.y,
       g.value                                      AS depth_m
FROM {{zone_name}}.mapping.depth_grids g
WHERE g.df_file_name LIKE '2026-03-12%'
  AND NOT EXISTS (
      SELECT 1
      FROM {{zone_name}}.mapping.depth_surfaces s
      WHERE s.source_file = g.df_file_name
  );


-- ============================================================================
-- 11. ALL THREE SURFACES
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE nodes = 1441 WHERE source_file = '2026-03-11_top_hugin_v1.zmap'
ASSERT VALUE nodes = 1441 WHERE source_file = '2026-03-12_top_hugin_v2.zmap'
ASSERT VALUE nodes = 1441 WHERE source_file = '2026-03-12_base_hugin.zmap'
ASSERT VALUE crest_m = 2458 WHERE source_file = '2026-03-12_top_hugin_v2.zmap'
ASSERT VALUE crest_m = 2516 WHERE source_file = '2026-03-12_base_hugin.zmap'
SELECT source_file,
       MIN(surface)                        AS surface,
       MIN(iteration)                      AS iteration,
       COUNT(*)                            AS nodes,
       CAST(ROUND(MIN(depth_m)) AS BIGINT) AS crest_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT) AS deepest_m
FROM {{zone_name}}.mapping.depth_surfaces
GROUP BY source_file
ORDER BY source_file;


-- ============================================================================
-- 12. WHAT THE VELOCITY CORRECTION DID
-- ============================================================================
-- The second depth conversion put the whole structure 18 m deeper, at every
-- one of the 1441 nodes. That the shift is identical everywhere is what says
-- it was a velocity change rather than a reinterpretation, and that the two
-- grids line up node for node is what makes the comparison possible at all.

ASSERT ROW_COUNT = 1
ASSERT VALUE compared_nodes = 1441
ASSERT VALUE min_shift_m = 18
ASSERT VALUE max_shift_m = 18
SELECT COUNT(*)                                        AS compared_nodes,
       CAST(ROUND(MIN(v2.depth_m - v1.depth_m)) AS BIGINT) AS min_shift_m,
       CAST(ROUND(MAX(v2.depth_m - v1.depth_m)) AS BIGINT) AS max_shift_m
FROM {{zone_name}}.mapping.depth_surfaces v1
JOIN {{zone_name}}.mapping.depth_surfaces v2
  ON v2.grid_row = v1.grid_row
 AND v2.grid_column = v1.grid_column
 AND v2.source_file = '2026-03-12_top_hugin_v2.zmap'
WHERE v1.source_file = '2026-03-11_top_hugin_v1.zmap';


-- ============================================================================
-- 13. GROSS ROCK VOLUME
-- ============================================================================
-- The number the whole handover exists to produce. Thickness is base minus
-- top at each node, and each node stands for one 200 by 200 metre cell, so
-- 40000 square metres. Two billion cubic metres of rock, and the reservoir is
-- 12 m thick on the flanks and 58 m at the crest.
--
-- This is computed against the CORRECTED top, which is the point of having
-- loaded both: run against the first pass it would be 1441 times 18 times
-- 40000 cubic metres too large.

ASSERT ROW_COUNT = 1
ASSERT VALUE nodes = 1441
ASSERT VALUE thinnest_m = 12
ASSERT VALUE thickest_m = 58
ASSERT VALUE grv_million_m3 = 2012
SELECT COUNT(*)                                                       AS nodes,
       CAST(ROUND(MIN(b.depth_m - t.depth_m)) AS BIGINT)              AS thinnest_m,
       CAST(ROUND(MAX(b.depth_m - t.depth_m)) AS BIGINT)              AS thickest_m,
       CAST(ROUND(SUM(b.depth_m - t.depth_m) * 40000.0 / 1000000.0) AS BIGINT) AS grv_million_m3
FROM {{zone_name}}.mapping.depth_surfaces t
JOIN {{zone_name}}.mapping.depth_surfaces b
  ON b.grid_row = t.grid_row
 AND b.grid_column = t.grid_column
 AND b.surface = 'BASE_HUGIN'
WHERE t.surface = 'TOP_HUGIN'
  AND t.iteration = 2;


-- ============================================================================
-- 14. THE BASE IS EVERYWHERE BELOW THE TOP
-- ============================================================================
-- A node where it is not would be a reservoir of negative thickness, which is
-- a mapping error rather than a geological one. The count that matters is
-- zero.

ASSERT ROW_COUNT = 0
SELECT t.grid_row, t.grid_column, t.depth_m AS top_m, b.depth_m AS base_m
FROM {{zone_name}}.mapping.depth_surfaces t
JOIN {{zone_name}}.mapping.depth_surfaces b
  ON b.grid_row = t.grid_row
 AND b.grid_column = t.grid_column
 AND b.surface = 'BASE_HUGIN'
WHERE t.surface = 'TOP_HUGIN'
  AND t.iteration = 2
  AND b.depth_m <= t.depth_m;


-- ============================================================================
-- 15. EVERY GRID LOADED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT s.source_file, s.curated, g.landed
FROM (
    SELECT source_file, COUNT(*) AS curated
    FROM {{zone_name}}.mapping.depth_surfaces
    GROUP BY source_file
) s
JOIN (
    SELECT df_file_name, COUNT(*) AS landed
    FROM {{zone_name}}.mapping.depth_grids
    GROUP BY df_file_name
) g
  ON g.df_file_name = s.source_file
WHERE s.curated <> g.landed;


-- ============================================================================
-- 16. NOTHING WAS LEFT BEHIND
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT g.df_file_name
FROM {{zone_name}}.mapping.depth_grids g
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.mapping.depth_surfaces s
    WHERE s.source_file = g.df_file_name
);


-- ============================================================================
-- 17. THE STATE AFTER THE FIRST HANDOVER, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 1441
SELECT *
FROM {{zone_name}}.mapping.depth_surfaces VERSION AS OF 1;


-- ============================================================================
-- 18. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.mapping.depth_surfaces;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The handover as a mapper would sign it off: three surfaces, where each sits,
-- and the extent they all share. Note that this extent is the MAPPED one,
-- 460800 to 470000 east, which is inside the grid's own 460000 to 471800:
-- the difference is the blank margin where nothing was interpreted.

ASSERT ROW_COUNT = 3
ASSERT VALUE nodes = 1441 WHERE source_file = '2026-03-11_top_hugin_v1.zmap'
ASSERT VALUE crest_m = 2440 WHERE source_file = '2026-03-11_top_hugin_v1.zmap'
ASSERT VALUE delivered_on = '2026-03-11' WHERE source_file = '2026-03-11_top_hugin_v1.zmap'
ASSERT VALUE crest_m = 2458 WHERE source_file = '2026-03-12_top_hugin_v2.zmap'
ASSERT VALUE delivered_on = '2026-03-12' WHERE source_file = '2026-03-12_top_hugin_v2.zmap'
ASSERT VALUE crest_m = 2516 WHERE source_file = '2026-03-12_base_hugin.zmap'
ASSERT VALUE west_m = 460800 WHERE source_file = '2026-03-12_base_hugin.zmap'
ASSERT VALUE north_m = 6548200 WHERE source_file = '2026-03-12_base_hugin.zmap'
SELECT source_file,
       MIN(surface)                         AS surface,
       MIN(delivered_on)                    AS delivered_on,
       COUNT(*)                             AS nodes,
       CAST(ROUND(MIN(depth_m)) AS BIGINT)  AS crest_m,
       CAST(ROUND(MAX(depth_m)) AS BIGINT)  AS deepest_m,
       CAST(ROUND(MIN(x)) AS BIGINT)        AS west_m,
       CAST(ROUND(MAX(y)) AS BIGINT)        AS north_m
FROM {{zone_name}}.mapping.depth_surfaces
GROUP BY source_file
ORDER BY source_file;
