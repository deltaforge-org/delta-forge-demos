-- ============================================================================
-- Geomodel Export Audit - Reconciliation and Verification
-- ============================================================================
-- A geomodeller hands over a reservoir model. The simulation team is about to
-- spend weeks history matching against the GRDECL export, so first they check
-- that the export is the same model as the ROFF file RMS holds.
--
--   02 April  reservoir_model.roff     20 x 15 x 8, 2400 cells, 1540 solved
--   02 April  reservoir_model.roffasc  the same model, ASCII, for review
--   03 April  grid_export.grdecl       the export, same grid, same properties
--   04 April  top_reservoir.roffasc    a depth surface, which has no grid
--
-- The two grid formats number their cells in OPPOSITE DIRECTIONS. ROFF stores
-- a per-cell array with K varying fastest and I slowest; ECLIPSE, and so
-- GRDECL, runs I fastest and K slowest. Cell (0,0,1) is ordinal 1 in the ROFF
-- file and ordinal 300 in the deck.
--
-- Query 12 is what that costs an audit written the obvious way.
--
-- All four files are synthetic. Every number below was recomputed from the
-- files on disk by compute_proofs.py, which parses them with its own reader,
-- before the engine was asked anything.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================
-- ROFF opens with an eight-byte magic, roff-bin or roff-asc, that appears in
-- no other format the engine reads, so this is one of the few detections in
-- the subsurface family that needs no extension and no guessing.

DISCOVER {{zone_name}}.handover.model
    PATH '{{data_subdir}}/landing/2026-04-02_reservoir_model.roff'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE MODEL AS THE SIMULATOR SOLVES IT
-- ============================================================================
-- 1540 cells. Not 2400, which is the grid.

ASSERT ROW_COUNT = 1540
SELECT *
FROM {{zone_name}}.handover.model;


-- ============================================================================
-- 3. THE FULL EXTENT, INACTIVE CELLS INCLUDED
-- ============================================================================
-- What the grid covers, as opposed to what it solves, and a check on the index
-- derivation at the same time: i, j and k are computed from the dimensions tag
-- rather than read, so every index must be present and none beyond its bound.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 2400
ASSERT VALUE active_cells = 1540
ASSERT VALUE inactive_cells = 860
ASSERT VALUE distinct_i = 20
ASSERT VALUE distinct_j = 15
ASSERT VALUE distinct_k = 8
ASSERT VALUE max_i = 19
ASSERT VALUE max_j = 14
ASSERT VALUE max_k = 7
ASSERT VALUE first_cell = 0
ASSERT VALUE last_cell = 2399
SELECT COUNT(*)                                     AS cells,
       COUNT(*) FILTER (WHERE active = true)        AS active_cells,
       COUNT(*) FILTER (WHERE active = false)       AS inactive_cells,
       COUNT(DISTINCT i)                            AS distinct_i,
       COUNT(DISTINCT j)                            AS distinct_j,
       COUNT(DISTINCT k)                            AS distinct_k,
       MAX(i)                                       AS max_i,
       MAX(j)                                       AS max_j,
       MAX(k)                                       AS max_k,
       MIN(cell_index)                              AS first_cell,
       MAX(cell_index)                              AS last_cell
FROM {{zone_name}}.handover.model_all;


-- ============================================================================
-- 4. ONE LAYER IS EXCLUDED FROM THE FLOW MODEL
-- ============================================================================
-- Layer 5 is a shale break the modeller took out. It is still in the grid, all
-- 300 cells of it, and not one of them is solved. This only shows up once the
-- active flags are honoured per cell rather than assumed uniform.

ASSERT ROW_COUNT = 1
ASSERT VALUE k = 5
ASSERT VALUE cells = 300
ASSERT VALUE active_cells = 0
SELECT k,
       COUNT(*)                               AS cells,
       COUNT(*) FILTER (WHERE active = true)  AS active_cells
FROM {{zone_name}}.handover.model_all
GROUP BY k
HAVING COUNT(*) FILTER (WHERE active = true) = 0
ORDER BY k;


-- ============================================================================
-- 5. HOW ROFF NUMBERS ITS CELLS: K FIRST
-- ============================================================================
-- The first eight ordinals in the ROFF file are one vertical column of the
-- grid: the same i, the same j, and every one of the eight layers. K is the
-- fastest moving index and I the slowest.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_i = 1
ASSERT VALUE distinct_j = 1
ASSERT VALUE distinct_k = 8
ASSERT VALUE max_k = 7
SELECT COUNT(DISTINCT i) AS distinct_i,
       COUNT(DISTINCT j) AS distinct_j,
       COUNT(DISTINCT k) AS distinct_k,
       MAX(k)            AS max_k
FROM {{zone_name}}.handover.model_all
WHERE cell_index < 8;


-- ============================================================================
-- 6. HOW GRDECL NUMBERS ITS CELLS: I FIRST
-- ============================================================================
-- The same eight ordinals in the deck are eight cells along a row of the top
-- layer. Exactly the reverse. This is not a quirk of these two files: it is
-- what the two formats say, and it is why the audit below has to join on the
-- indices rather than on the ordinal.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_i = 8
ASSERT VALUE distinct_j = 1
ASSERT VALUE distinct_k = 1
ASSERT VALUE max_i = 7
SELECT COUNT(DISTINCT i) AS distinct_i,
       COUNT(DISTINCT j) AS distinct_j,
       COUNT(DISTINCT k) AS distinct_k,
       MAX(i)            AS max_i
FROM {{zone_name}}.handover.grid_export_all
WHERE cell_index < 8;


-- ============================================================================
-- 7. FACIES ARRIVES AS A WORD, NOT ONLY A CODE
-- ============================================================================
-- A ROFF discrete parameter carries codeNames beside its codes, so the reader
-- emits both the number and what the number means. Nineteen cells hold the
-- undefined marker and get no label rather than a label for 255.

ASSERT ROW_COUNT = 4
ASSERT VALUE cells = 584 WHERE facies_name = 'sand'
ASSERT VALUE cells = 580 WHERE facies_name = 'shale'
ASSERT VALUE cells = 357 WHERE facies_name = 'silt'
ASSERT VALUE cells = 19 WHERE facies_name = 'undefined'
SELECT COALESCE(facies_label, 'undefined') AS facies_name,
       COUNT(*)                            AS cells
FROM {{zone_name}}.handover.model
GROUP BY COALESCE(facies_label, 'undefined')
ORDER BY cells DESC;


-- ============================================================================
-- 8. THE CELLS RMS LEFT UNDEFINED
-- ============================================================================
-- RMS writes -999.0 where a float property was never populated. That is the
-- writer's convention rather than anything the format says, so the reader
-- turns it into a null and an average over the column is an average over the
-- cells that have a value.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 1540
ASSERT VALUE defined_poro = 1519
ASSERT VALUE undefined_poro = 21
SELECT COUNT(*)                                AS cells,
       COUNT(poro)                             AS defined_poro,
       COUNT(*) FILTER (WHERE poro IS NULL)    AS undefined_poro
FROM {{zone_name}}.handover.model;


-- ============================================================================
-- 9. THE ASCII COPY IS THE SAME MODEL
-- ============================================================================
-- The two forms of ROFF are the same grammar with a zero byte where the text
-- form has whitespace. The values are rounded to four decimals here because
-- the file holds them as four-byte floats, which is the precision the model
-- actually carries.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 1540
ASSERT VALUE defined_poro = 1519
ASSERT VALUE poro_sum = 332.45
ASSERT VALUE poro_min = 0.136
ASSERT VALUE poro_max = 0.2973
SELECT COUNT(*)                                     AS cells,
       COUNT(poro)                                  AS defined_poro,
       ROUND(SUM(CAST(poro AS DOUBLE)), 2)          AS poro_sum,
       ROUND(CAST(MIN(poro) AS DOUBLE), 4)          AS poro_min,
       ROUND(CAST(MAX(poro) AS DOUBLE), 4)          AS poro_max
FROM {{zone_name}}.handover.model_ascii;


-- ============================================================================
-- 10. AND IT IS THE SAME MODEL CELL BY CELL
-- ============================================================================
-- Aggregates can agree while the cells underneath them do not. Both files are
-- ROFF, so the ordinal means the same thing in each and the join is safe.

ASSERT ROW_COUNT = 1
ASSERT VALUE compared = 1519
ASSERT VALUE differing = 0
SELECT COUNT(*)                                        AS compared,
       COUNT(*) FILTER (WHERE b.poro != a.poro)        AS differing
FROM {{zone_name}}.handover.model b
JOIN {{zone_name}}.handover.model_ascii a
  ON b.cell_index = a.cell_index
WHERE b.poro IS NOT NULL;


-- ============================================================================
-- 11. THE AUDIT: THE EXPORT IS FAITHFUL
-- ============================================================================
-- Joined on i, j and k, which is what identifies a cell in both formats. The
-- comparison is to a millionth because one side is a four-byte float and the
-- other an eight-byte one; the largest disagreement across 1519 cells is
-- fifteen billionths, which is the width of the float and nothing else.

ASSERT ROW_COUNT = 1
ASSERT VALUE compared = 1519
ASSERT VALUE beyond_tolerance = 0
ASSERT VALUE largest_difference = 0.0
SELECT COUNT(*)                                                                  AS compared,
       COUNT(*) FILTER (WHERE ABS(CAST(m.poro AS DOUBLE) - e.poro) > 0.000001)   AS beyond_tolerance,
       ROUND(MAX(ABS(CAST(m.poro AS DOUBLE) - e.poro)), 6)                       AS largest_difference
FROM {{zone_name}}.handover.model m
JOIN {{zone_name}}.handover.grid_export e
  ON m.i = e.i AND m.j = e.j AND m.k = e.k
WHERE m.poro IS NOT NULL;


-- ============================================================================
-- 12. THE SAME AUDIT, JOINED ON THE CELL ORDINAL
-- ============================================================================
-- This is the query the audit gets written as when nobody has checked which
-- way each format counts. It runs. It returns 968 rows. Every one of those
-- rows compares a cell in the model to a DIFFERENT cell in the export, and
-- not one of the 968 lines up: same_cell is zero.
--
-- Nothing about the result looks wrong. There is no error, no null, no
-- suspicious count, and the porosities it compares are all plausible reservoir
-- numbers. An audit written this way passes or fails at random and means
-- nothing either way.

ASSERT ROW_COUNT = 1
ASSERT VALUE rows_returned = 968
ASSERT VALUE same_cell = 0
SELECT COUNT(*)                                                            AS rows_returned,
       COUNT(*) FILTER (WHERE m.i = e.i AND m.j = e.j AND m.k = e.k)       AS same_cell
FROM {{zone_name}}.handover.model m
JOIN {{zone_name}}.handover.grid_export e
  ON m.cell_index = e.cell_index;


-- ============================================================================
-- 13. WHAT THE EXPORT COULD NOT CARRY
-- ============================================================================
-- The export is faithful about the numbers and loses two things anyway, both
-- because GRDECL has nowhere to put them. It has no null, so the 21 cells RMS
-- left undefined arrive as the number -999. And it has no code names, so the
-- 19 undefined facies arrive as 255 and the words shale, sand and silt are
-- simply not in the file.

ASSERT ROW_COUNT = 1
ASSERT VALUE undefined_in_rms = 21
ASSERT VALUE carried_as_minus_999 = 21
ASSERT VALUE facies_undefined = 19
ASSERT VALUE facies_carried_as_255 = 19
SELECT COUNT(*) FILTER (WHERE m.poro IS NULL)                          AS undefined_in_rms,
       COUNT(*) FILTER (WHERE m.poro IS NULL AND e.poro = -999.0)      AS carried_as_minus_999,
       COUNT(*) FILTER (WHERE m.facies IS NULL)                        AS facies_undefined,
       COUNT(*) FILTER (WHERE m.facies IS NULL AND e.facies = 255.0)   AS facies_carried_as_255
FROM {{zone_name}}.handover.model m
JOIN {{zone_name}}.handover.grid_export e
  ON m.i = e.i AND m.j = e.j AND m.k = e.k;


-- ============================================================================
-- 14. THE ZONES THE MODEL IS DIVIDED INTO
-- ============================================================================
-- A ROFF subgrids tag gives the layer count of each zone, and the reader turns
-- that into a per-cell zone index in the file's own K direction. The middle
-- zone stops at layer 4 rather than 5 because layer 5 is the excluded shale.

ASSERT ROW_COUNT = 3
ASSERT VALUE cells = 660 WHERE subgrid = 0
ASSERT VALUE cells = 440 WHERE subgrid = 1
ASSERT VALUE cells = 440 WHERE subgrid = 2
ASSERT VALUE last_layer = 2 WHERE subgrid = 0
ASSERT VALUE last_layer = 4 WHERE subgrid = 1
ASSERT VALUE first_layer = 6 WHERE subgrid = 2
SELECT subgrid,
       COUNT(*) AS cells,
       MIN(k)   AS first_layer,
       MAX(k)   AS last_layer
FROM {{zone_name}}.handover.model
GROUP BY subgrid
ORDER BY subgrid;


-- ============================================================================
-- 15. THE DEPTH SURFACE, WHICH IS NOT A GRID
-- ============================================================================
-- RMS writes surfaces, points, polygons and wells into the same container as
-- its grids. This file carries no dimensions tag, so there are no cells to
-- hang rows on. Rather than refuse it, the reader reports the container: one
-- row per element of one key of one tag. Three tags carry keys, 300 of the 312
-- rows are the surface's own nodes, and the file says what it is.

ASSERT ROW_COUNT = 1
ASSERT VALUE rows_total = 312
ASSERT VALUE nodes = 300
ASSERT VALUE tags = 3
ASSERT VALUE deepest_node = 2243.69
SELECT COUNT(*)                                              AS rows_total,
       COUNT(*) FILTER (WHERE tag_key = 'values')            AS nodes,
       COUNT(DISTINCT tag)                                   AS tags,
       ROUND(MAX(value) FILTER (WHERE tag_key = 'values'), 2) AS deepest_node
FROM {{zone_name}}.handover.top_reservoir;


-- ============================================================================
-- 16. THE SURFACE DECLARES ITS OWN TYPE
-- ============================================================================
-- filetype is what separates a surface from a grid inside the container, and
-- in long form it is simply a row like any other.

ASSERT ROW_COUNT = 1
ASSERT VALUE text = 'surface'
ASSERT VALUE element_type = 'char'
ASSERT VALUE element_index = -1
SELECT text, element_type, element_index
FROM {{zone_name}}.handover.top_reservoir
WHERE tag = 'filedata' AND tag_key = 'filetype';


-- ============================================================================
-- 17. LOAD THE RECONCILED MODEL
-- ============================================================================
-- Every active cell: what RMS holds, what the export carries for that same
-- cell, and the difference. Joined on i, j and k, because query 12 is what
-- happens otherwise.

INSERT INTO {{zone_name}}.handover.audited_model
SELECT 'tarbert_2026_04'         AS model_name,
       '2026-04-02'              AS delivered_on,
       m.df_file_name            AS source_file,
       m.cell_index              AS cell_index,
       m.i                       AS i,
       m.j                       AS j,
       m.k                       AS k,
       CASE m.subgrid
           WHEN 0 THEN 'Upper Tarbert'
           WHEN 1 THEN 'Lower Tarbert'
           WHEN 2 THEN 'Ness'
           ELSE 'Unzoned'
       END                       AS zone_label,
       CAST(m.poro AS DOUBLE)    AS poro,
       CAST(m.permx AS DOUBLE)   AS permx,
       CAST(m.ntg AS DOUBLE)     AS ntg,
       m.facies                  AS facies_code,
       m.facies_label            AS facies_name,
       e.poro                    AS export_poro,
       CAST(m.poro AS DOUBLE) - e.poro AS poro_delta
FROM {{zone_name}}.handover.model m
JOIN {{zone_name}}.handover.grid_export e
  ON m.i = e.i AND m.j = e.j AND m.k = e.k;


-- ============================================================================
-- 18. THE CURATED MODEL, ZONE BY ZONE
-- ============================================================================
-- The subgrid index has become the zone the geologist named, and the layer
-- ranges show the excluded shale again from the other side: the middle zone
-- was given three layers by the subgrids tag and contributes two.

ASSERT ROW_COUNT = 3
ASSERT VALUE cells = 660 WHERE zone_label = 'Upper Tarbert'
ASSERT VALUE cells = 440 WHERE zone_label = 'Lower Tarbert'
ASSERT VALUE cells = 440 WHERE zone_label = 'Ness'
ASSERT VALUE defined_poro = 651 WHERE zone_label = 'Upper Tarbert'
ASSERT VALUE defined_poro = 434 WHERE zone_label = 'Lower Tarbert'
ASSERT VALUE defined_poro = 434 WHERE zone_label = 'Ness'
ASSERT VALUE first_layer = 3 WHERE zone_label = 'Lower Tarbert'
ASSERT VALUE last_layer = 4 WHERE zone_label = 'Lower Tarbert'
ASSERT VALUE first_layer = 6 WHERE zone_label = 'Ness'
SELECT zone_label,
       COUNT(*)    AS cells,
       COUNT(poro) AS defined_poro,
       MIN(k)      AS first_layer,
       MAX(k)      AS last_layer
FROM {{zone_name}}.handover.audited_model
GROUP BY zone_label
ORDER BY MIN(k);


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The number a static model exists to produce, computed over the cells that
-- have a porosity: 1540 cells handed over, 1519 of them populated, three
-- zones, 1521 with a named facies, and a worst-case disagreement with the
-- export of zero at six decimal places. A cell is 50 by 50 by 5 metres, so
-- 12,500 cubic metres of bulk rock.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 1540
ASSERT VALUE defined_poro = 1519
ASSERT VALUE zones = 3
ASSERT VALUE facies_named = 1521
ASSERT VALUE worst_delta = 0.0
ASSERT VALUE pore_volume_m3 = 3595388
SELECT COUNT(*)                                              AS cells,
       COUNT(poro)                                           AS defined_poro,
       COUNT(DISTINCT zone_label)                            AS zones,
       COUNT(facies_name)                                    AS facies_named,
       ROUND(MAX(ABS(poro_delta)), 6)                        AS worst_delta,
       CAST(ROUND(SUM(12500.0 * poro * ntg)) AS BIGINT)      AS pore_volume_m3
FROM {{zone_name}}.handover.audited_model;
