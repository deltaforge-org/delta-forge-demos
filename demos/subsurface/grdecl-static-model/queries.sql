-- ============================================================================
-- Static Model Deck Ingestion - Incremental Load and Verification
-- ============================================================================
-- Two versions of a static model deck, delivered a day apart:
--
--   2026-03-11  static_model_v1   ACTNUM, PORO, PERMX, SATNUM
--   2026-03-12  static_model_v2   the same plus NTG
--
-- The grid is 30 by 24 by 10, so 7200 cells of which 7152 are inside the
-- fault block. Every value below was expanded from the decks by a second,
-- independent reader before the engine saw them.
--
-- The thing to watch is the size. Each deck is about a kilobyte and describes
-- 7200 cells four or five times over, because a GRDECL property is RUN-LENGTH
-- ENCODED: a whole property here is ten tokens, one per layer, and the first
-- of them is `720*0.263`. A reader that does not expand the repeats produces
-- a model of forty-eight cells and no error at all.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.static_modelling.model_v1
    PATH '{{data_subdir}}/landing/2026-03-11_static_model_v1.grdecl'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE RUN LENGTH ENCODING EXPANDED
-- ============================================================================
-- 7152 active cells out of a 7200-cell grid, from a deck of 48 property
-- tokens. If the repeats were not being expanded this would be a number in
-- the tens.

ASSERT ROW_COUNT = 7152
SELECT *
FROM {{zone_name}}.static_modelling.model_v1;


-- ============================================================================
-- 3. THE FULL EXTENT, INACTIVE CELLS INCLUDED
-- ============================================================================
-- What the model covers, as opposed to what it solves. The 48 inactive cells
-- are a wedge on the eastern flank of the top two layers, outside the fault
-- block.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 7200
ASSERT VALUE active = 7152
ASSERT VALUE inactive = 48
SELECT COUNT(*)                              AS cells,
       COUNT(*) FILTER (WHERE actnum = 1)    AS active,
       COUNT(*) FILTER (WHERE actnum = 0)    AS inactive
FROM {{zone_name}}.static_modelling.all_cells;


-- ============================================================================
-- 4. THE CELL INDICES COVER THE GRID EXACTLY
-- ============================================================================
-- i, j and k are derived from SPECGRID rather than read from the file, so
-- this checks the derivation: 30 by 24 by 10, zero based, with every index
-- present and none beyond its bound.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_i = 30
ASSERT VALUE distinct_j = 24
ASSERT VALUE distinct_k = 10
ASSERT VALUE max_i = 29
ASSERT VALUE max_j = 23
ASSERT VALUE max_k = 9
ASSERT VALUE first_cell = 0
ASSERT VALUE last_cell = 7199
SELECT COUNT(DISTINCT i)  AS distinct_i,
       COUNT(DISTINCT j)  AS distinct_j,
       COUNT(DISTINCT k)  AS distinct_k,
       MAX(i)             AS max_i,
       MAX(j)             AS max_j,
       MAX(k)             AS max_k,
       MIN(cell_index)    AS first_cell,
       MAX(cell_index)    AS last_cell
FROM {{zone_name}}.static_modelling.all_cells;


-- ============================================================================
-- 5. LOAD THE FIRST DECK
-- ============================================================================
-- ntg is null rather than 1.0, because the first version does not have it and
-- defaulting it would make the two versions agree on pore volume, which is
-- the comparison the revision exists for.

INSERT INTO {{zone_name}}.static_modelling.static_model
SELECT 'v1'                     AS model_version,
       '2026-03-11'             AS delivered_on,
       m.df_file_name           AS source_file,
       m.cell_index,
       m.i,
       m.j,
       m.k,
       m.poro,
       m.permx,
       m.satnum,
       CAST(NULL AS DOUBLE)     AS ntg
FROM {{zone_name}}.static_modelling.model_v1 m
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.static_modelling.static_model s
    WHERE s.source_file = m.df_file_name
);


-- ============================================================================
-- 6. THE PROPERTIES ARE CONSTANT WITHIN EACH LAYER
-- ============================================================================
-- Which is what made the run-length encoding compress so far, and is also the
-- shape a layer-cake model has. One porosity per layer, falling with depth
-- from 0.263 at the top to 0.148 at the base.

ASSERT ROW_COUNT = 10
ASSERT VALUE cells = 696 WHERE k = 0
ASSERT VALUE cells = 696 WHERE k = 1
ASSERT VALUE cells = 720 WHERE k = 2
ASSERT VALUE cells = 720 WHERE k = 9
ASSERT VALUE distinct_poro = 1 WHERE k = 0
ASSERT VALUE distinct_poro = 1 WHERE k = 9
ASSERT VALUE poro = 0.263 WHERE k = 0
ASSERT VALUE poro = 0.148 WHERE k = 9
ASSERT VALUE permx = 1850 WHERE k = 0
ASSERT VALUE permx = 72 WHERE k = 9
SELECT k,
       COUNT(*)                 AS cells,
       COUNT(DISTINCT poro)     AS distinct_poro,
       MIN(poro)                AS poro,
       MIN(permx)               AS permx
FROM {{zone_name}}.static_modelling.static_model
WHERE model_version = 'v1'
GROUP BY k
ORDER BY k;


-- ============================================================================
-- 7. THE INACTIVE WEDGE IS ONLY IN THE TOP TWO LAYERS
-- ============================================================================
-- 24 cells missing from each of the first two layers and none from the other
-- eight, which is the fault block cutting the crest of the model.

ASSERT ROW_COUNT = 2
ASSERT VALUE cells = 696 WHERE k = 0
ASSERT VALUE cells = 696 WHERE k = 1
SELECT k, COUNT(*) AS cells
FROM {{zone_name}}.static_modelling.static_model
WHERE model_version = 'v1'
GROUP BY k
HAVING COUNT(*) < 720
ORDER BY k;


-- ============================================================================
-- 8. THE SAME DECK AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.static_modelling.static_model
SELECT 'v1'                     AS model_version,
       '2026-03-11'             AS delivered_on,
       m.df_file_name           AS source_file,
       m.cell_index,
       m.i,
       m.j,
       m.k,
       m.poro,
       m.permx,
       m.satnum,
       CAST(NULL AS DOUBLE)     AS ntg
FROM {{zone_name}}.static_modelling.model_v1 m
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.static_modelling.static_model s
    WHERE s.source_file = m.df_file_name
);


-- ============================================================================
-- 9. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 7152
SELECT COUNT(*) AS cells
FROM {{zone_name}}.static_modelling.static_model
WHERE model_version = 'v1';


-- ============================================================================
-- 10. LOAD THE REVISION
-- ============================================================================

INSERT INTO {{zone_name}}.static_modelling.static_model
SELECT 'v2'                     AS model_version,
       '2026-03-12'             AS delivered_on,
       m.df_file_name           AS source_file,
       m.cell_index,
       m.i,
       m.j,
       m.k,
       m.poro,
       m.permx,
       m.satnum,
       m.ntg
FROM {{zone_name}}.static_modelling.model_v2 m
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.static_modelling.static_model s
    WHERE s.source_file = m.df_file_name
);


-- ============================================================================
-- 11. BOTH VERSIONS
-- ============================================================================
-- The same grid twice, and the revision carries a property the first does not
-- have on any of its 7152 cells.

ASSERT ROW_COUNT = 2
ASSERT VALUE cells = 7152 WHERE model_version = 'v1'
ASSERT VALUE with_ntg = 0 WHERE model_version = 'v1'
ASSERT VALUE cells = 7152 WHERE model_version = 'v2'
ASSERT VALUE with_ntg = 7152 WHERE model_version = 'v2'
SELECT model_version,
       MIN(delivered_on)                         AS delivered_on,
       COUNT(*)                                  AS cells,
       COUNT(*) FILTER (WHERE ntg IS NOT NULL)   AS with_ntg
FROM {{zone_name}}.static_modelling.static_model
GROUP BY model_version
ORDER BY model_version;


-- ============================================================================
-- 12. SATURATION REGIONS
-- ============================================================================
-- Three regions stacked down the model, which is a different property of the
-- same cells: SATNUM and PORO are separate keywords in the deck and the same
-- row here, because i, j and k came from SPECGRID rather than from row order.

ASSERT ROW_COUNT = 3
ASSERT VALUE cells = 2832 WHERE satnum = 1
ASSERT VALUE cells = 2880 WHERE satnum = 2
ASSERT VALUE cells = 1440 WHERE satnum = 3
ASSERT VALUE layers = 4 WHERE satnum = 1
ASSERT VALUE layers = 4 WHERE satnum = 2
ASSERT VALUE layers = 2 WHERE satnum = 3
SELECT satnum,
       COUNT(*)               AS cells,
       COUNT(DISTINCT k)      AS layers,
       MIN(k)                 AS top_layer,
       MAX(k)                 AS base_layer
FROM {{zone_name}}.static_modelling.static_model
WHERE model_version = 'v1'
GROUP BY satnum
ORDER BY satnum;


-- ============================================================================
-- 13. WHAT NET TO GROSS DID TO THE VOLUMES
-- ============================================================================
-- The reason the revision was made. Summing porosity across the active cells
-- gives one number; summing porosity times net-to-gross gives 1132, which is
-- 24 percent less. That difference is the optimism somebody questioned.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 7152
ASSERT VALUE gross_units = 1489
ASSERT VALUE net_units = 1132
ASSERT VALUE reduction_pct = 24
SELECT COUNT(*)                                                    AS cells,
       CAST(ROUND(SUM(poro)) AS BIGINT)                            AS gross_units,
       CAST(ROUND(SUM(poro * ntg)) AS BIGINT)                       AS net_units,
       CAST(ROUND(100.0 * (1.0 - SUM(poro * ntg) / SUM(poro))) AS BIGINT) AS reduction_pct
FROM {{zone_name}}.static_modelling.static_model
WHERE model_version = 'v2';


-- ============================================================================
-- 14. PERMEABILITY FOLLOWS POROSITY
-- ============================================================================
-- Not a coincidence: it is the relationship a static model is built on, and a
-- deck where it did not hold would be one somebody should look at. Every
-- layer with more porosity than the one below it also has more permeability.

ASSERT ROW_COUNT = 0
SELECT a.k AS upper_layer, b.k AS lower_layer,
       a.poro AS upper_poro, b.poro AS lower_poro,
       a.permx AS upper_permx, b.permx AS lower_permx
FROM (
    SELECT k, MIN(poro) AS poro, MIN(permx) AS permx
    FROM {{zone_name}}.static_modelling.static_model
    WHERE model_version = 'v1'
    GROUP BY k
) a
JOIN (
    SELECT k, MIN(poro) AS poro, MIN(permx) AS permx
    FROM {{zone_name}}.static_modelling.static_model
    WHERE model_version = 'v1'
    GROUP BY k
) b
  ON b.k = a.k + 1
WHERE a.poro > b.poro
  AND a.permx <= b.permx;


-- ============================================================================
-- 15. EVERY DECK LOADED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE cells = 7152 WHERE source_file = '2026-03-11_static_model_v1.grdecl'
ASSERT VALUE cells = 7152 WHERE source_file = '2026-03-12_static_model_v2.grdecl'
ASSERT VALUE distinct_cells = 7152 WHERE source_file = '2026-03-11_static_model_v1.grdecl'
SELECT source_file,
       COUNT(*)                     AS cells,
       COUNT(DISTINCT cell_index)   AS distinct_cells
FROM {{zone_name}}.static_modelling.static_model
GROUP BY source_file
ORDER BY source_file;


-- ============================================================================
-- 16. THE STATE AFTER THE FIRST DECK, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 7152
SELECT *
FROM {{zone_name}}.static_modelling.static_model VERSION AS OF 1;


-- ============================================================================
-- 17. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.static_modelling.static_model;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The model as the simulation team would sign it off: both versions, the same
-- grid, and what the revision changed.

ASSERT ROW_COUNT = 2
ASSERT VALUE cells = 7152 WHERE model_version = 'v1'
ASSERT VALUE layers = 10 WHERE model_version = 'v1'
ASSERT VALUE regions = 3 WHERE model_version = 'v1'
ASSERT VALUE mean_poro_pct = 21 WHERE model_version = 'v1'
ASSERT VALUE with_ntg = 0 WHERE model_version = 'v1'
ASSERT VALUE cells = 7152 WHERE model_version = 'v2'
ASSERT VALUE layers = 10 WHERE model_version = 'v2'
ASSERT VALUE mean_poro_pct = 21 WHERE model_version = 'v2'
ASSERT VALUE with_ntg = 7152 WHERE model_version = 'v2'
SELECT model_version,
       MIN(delivered_on)                                AS delivered_on,
       COUNT(*)                                         AS cells,
       COUNT(DISTINCT k)                                AS layers,
       COUNT(DISTINCT satnum)                           AS regions,
       CAST(ROUND(100.0 * AVG(poro)) AS BIGINT)         AS mean_poro_pct,
       COUNT(*) FILTER (WHERE ntg IS NOT NULL)          AS with_ntg
FROM {{zone_name}}.static_modelling.static_model
GROUP BY model_version
ORDER BY model_version;
