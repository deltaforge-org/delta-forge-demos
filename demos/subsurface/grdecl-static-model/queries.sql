-- ============================================================================
-- Static Model Deck Ingestion - Incremental Load and Verification
-- ============================================================================
-- Two GRDECL decks, delivered a day apart, written in opposite styles:
--
--   2026-03-11  norne_2004     46 x 112 x 22, 453,376 values, 5.7 MB
--   2026-03-12  sector_model   30 x 24 x 10,   36,000 values, 1.1 kB
--
-- Both numbers are the count of values the deck describes. The difference is
-- how they are written. A GRDECL property is RUN-LENGTH ENCODED: `720*0.263`
-- is seven hundred and twenty cells of 0.263, not one cell holding a string.
-- The Norne deck uses no repeats anywhere, so its 453,376 values take 453,376
-- numeric tokens. The sector deck is almost nothing but repeats, so its 36,000
-- values take 62. A reader that does not expand the repeats gets Norne exactly
-- right and turns the sector model into 62 cells, with no error at all.
--
-- The Norne deck is the real published model of the Norne field, offshore
-- Norway, under the Open Database License. See ATTRIBUTION.md. Every value
-- asserted below was expanded from the decks by a second, independent reader
-- before the engine saw them.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.static_modelling.norne
    PATH '{{data_subdir}}/landing/2026-03-11_norne_2004.grdecl'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. THE REAL MODEL, AS THE SIMULATOR SOLVES IT
-- ============================================================================
-- 44,927 cells. Not 113,344, which is the grid, and not 62, which is what a
-- token count would give you.

ASSERT ROW_COUNT = 44927
SELECT *
FROM {{zone_name}}.static_modelling.norne;


-- ============================================================================
-- 3. THE FULL EXTENT, INACTIVE CELLS INCLUDED
-- ============================================================================
-- What the model covers, as opposed to what it solves. On a real field model
-- these are very different numbers: three cells in five are outside the
-- simulated volume. Dropping them is the default for good reason, because
-- their property values are in the file and mean nothing.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 113344
ASSERT VALUE active = 44927
ASSERT VALUE inactive = 68417
SELECT COUNT(*)                              AS cells,
       COUNT(*) FILTER (WHERE actnum = 1)    AS active,
       COUNT(*) FILTER (WHERE actnum = 0)    AS inactive
FROM {{zone_name}}.static_modelling.norne_all;


-- ============================================================================
-- 4. THE CELL INDICES COVER THE GRID EXACTLY
-- ============================================================================
-- i, j and k are derived from the grid dimensions rather than read from the
-- file, so this checks the derivation: 46 by 112 by 22, zero based, every
-- index present and none beyond its bound.

ASSERT ROW_COUNT = 1
ASSERT VALUE distinct_i = 46
ASSERT VALUE distinct_j = 112
ASSERT VALUE distinct_k = 22
ASSERT VALUE max_i = 45
ASSERT VALUE max_j = 111
ASSERT VALUE max_k = 21
ASSERT VALUE first_cell = 0
ASSERT VALUE last_cell = 113343
SELECT COUNT(DISTINCT i)  AS distinct_i,
       COUNT(DISTINCT j)  AS distinct_j,
       COUNT(DISTINCT k)  AS distinct_k,
       MAX(i)             AS max_i,
       MAX(j)             AS max_j,
       MAX(k)             AS max_k,
       MIN(cell_index)    AS first_cell,
       MAX(cell_index)    AS last_cell
FROM {{zone_name}}.static_modelling.norne_all;


-- ============================================================================
-- 5. ONE LAYER OF THE MODEL IS ENTIRELY INACTIVE
-- ============================================================================
-- Layer 3 has 5152 cells in the grid and not one of them is solved. This is a
-- real property of the published Norne model rather than an artefact, and it
-- is the kind of thing that only shows up once the ACTNUM flags are actually
-- being honoured per cell instead of assumed uniform.

ASSERT ROW_COUNT = 1
ASSERT VALUE k = 3
ASSERT VALUE cells = 5152
ASSERT VALUE active = 0
SELECT k,
       COUNT(*)                            AS cells,
       COUNT(*) FILTER (WHERE actnum = 1)  AS active
FROM {{zone_name}}.static_modelling.norne_all
GROUP BY k
HAVING COUNT(*) FILTER (WHERE actnum = 1) = 0
ORDER BY k;


-- ============================================================================
-- 6. LOAD THE NORNE DECK
-- ============================================================================
-- satnum is null rather than 1, because this deck does not carry saturation
-- regions. Defaulting it would invent a property the file does not state.

INSERT INTO {{zone_name}}.static_modelling.static_model
SELECT 'norne'                 AS model,
       '2026-03-11'            AS delivered_on,
       m.df_file_name          AS source_file,
       m.cell_index,
       m.i,
       m.j,
       m.k,
       m.poro,
       m.permx,
       m.ntg,
       CAST(NULL AS DOUBLE)    AS satnum
FROM {{zone_name}}.static_modelling.norne m
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.static_modelling.static_model s
    WHERE s.source_file = m.df_file_name
);


-- ============================================================================
-- 7. THE MODEL LAYER BY LAYER
-- ============================================================================
-- 21 layers, not 22, because layer 3 contributes no solved cells and so does
-- not appear at all. The active cell count varies layer by layer between 1403
-- and 2263, which is the shape of a field outline rather than a box.

ASSERT ROW_COUNT = 21
ASSERT VALUE cells = 2221 WHERE k = 0
ASSERT VALUE cells = 2263 WHERE k = 1
ASSERT VALUE cells = 2036 WHERE k = 10
ASSERT VALUE cells = 2263 WHERE k = 21
ASSERT VALUE mean_poro_pct = 27 WHERE k = 0
ASSERT VALUE mean_poro_pct = 22 WHERE k = 2
ASSERT VALUE mean_ntg_pct = 98 WHERE k = 0
ASSERT VALUE mean_ntg_pct = 58 WHERE k = 2
SELECT k,
       COUNT(*)                                  AS cells,
       CAST(ROUND(100.0 * AVG(poro)) AS BIGINT)  AS mean_poro_pct,
       CAST(ROUND(100.0 * AVG(ntg)) AS BIGINT)   AS mean_ntg_pct,
       CAST(ROUND(MAX(permx)) AS BIGINT)         AS max_permx
FROM {{zone_name}}.static_modelling.static_model
WHERE model = 'norne'
GROUP BY k
ORDER BY k;


-- ============================================================================
-- 8. EVERY CELL CARRIES ITS OWN POROSITY
-- ============================================================================
-- Layer 0 holds 2221 solved cells and 2221 distinct porosity values, so no two
-- cells in it share a porosity. Permeability has 2220, meaning exactly one
-- pair of cells in the layer happens to land on the same number, and net to
-- gross has 1981 because it is a coarser quantity. That is what makes this
-- deck 5.7 MB and what makes run-length encoding useless on it: there is
-- almost nothing to repeat.
--
-- These three counts are the assertion that would catch a reader quietly
-- reusing a value across cells. An average would absorb that; a distinct
-- count one short of the row count would not, and here two of the three are
-- deliberately not round.

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 2221
ASSERT VALUE distinct_poro = 2221
ASSERT VALUE distinct_permx = 2220
ASSERT VALUE distinct_ntg = 1981
SELECT COUNT(*)                  AS cells,
       COUNT(DISTINCT poro)      AS distinct_poro,
       COUNT(DISTINCT permx)     AS distinct_permx,
       COUNT(DISTINCT ntg)       AS distinct_ntg
FROM {{zone_name}}.static_modelling.static_model
WHERE model = 'norne' AND k = 0;


-- ============================================================================
-- 9. THE SAME DECK AGAIN
-- ============================================================================

INSERT INTO {{zone_name}}.static_modelling.static_model
SELECT 'norne'                 AS model,
       '2026-03-11'            AS delivered_on,
       m.df_file_name          AS source_file,
       m.cell_index,
       m.i,
       m.j,
       m.k,
       m.poro,
       m.permx,
       m.ntg,
       CAST(NULL AS DOUBLE)    AS satnum
FROM {{zone_name}}.static_modelling.norne m
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.static_modelling.static_model s
    WHERE s.source_file = m.df_file_name
);


-- ============================================================================
-- 10. THE RE-RUN ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE cells = 44927
SELECT COUNT(*) AS cells
FROM {{zone_name}}.static_modelling.static_model
WHERE model = 'norne';


-- ============================================================================
-- 11. LOAD THE SECTOR MODEL
-- ============================================================================
-- The other deck, and the other half of the format. 1165 bytes on disk.

INSERT INTO {{zone_name}}.static_modelling.static_model
SELECT 'sector'                AS model,
       '2026-03-12'            AS delivered_on,
       m.df_file_name          AS source_file,
       m.cell_index,
       m.i,
       m.j,
       m.k,
       m.poro,
       m.permx,
       m.ntg,
       m.satnum
FROM {{zone_name}}.static_modelling.sector m
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.static_modelling.static_model s
    WHERE s.source_file = m.df_file_name
);


-- ============================================================================
-- 12. THE RUN LENGTH ENCODING EXPANDED
-- ============================================================================
-- 7152 solved cells out of a 7200-cell grid, from a file of 62 numeric tokens.
-- Every layer holds exactly one porosity value, which is what let it compress
-- that far, and is the exact opposite of the Norne deck four queries up. If
-- the repeats were not being expanded this would be a number in the tens.

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
WHERE model = 'sector'
GROUP BY k
ORDER BY k;


-- ============================================================================
-- 13. SATURATION REGIONS
-- ============================================================================
-- Three regions stacked down the sector model, which is a different property
-- of the same cells: SATNUM and PORO are separate keywords in the deck and the
-- same row here, because i, j and k came from the grid header rather than from
-- row order. The Norne deck carries no SATNUM, so its cells are null here and
-- are not counted as a fourth region.

ASSERT ROW_COUNT = 3
ASSERT VALUE cells = 2832 WHERE satnum = 1
ASSERT VALUE cells = 2880 WHERE satnum = 2
ASSERT VALUE cells = 1440 WHERE satnum = 3
ASSERT VALUE layers = 4 WHERE satnum = 1
ASSERT VALUE layers = 4 WHERE satnum = 2
ASSERT VALUE layers = 2 WHERE satnum = 3
ASSERT VALUE top_layer = 8 WHERE satnum = 3
SELECT satnum,
       COUNT(*)               AS cells,
       COUNT(DISTINCT k)      AS layers,
       MIN(k)                 AS top_layer,
       MAX(k)                 AS base_layer
FROM {{zone_name}}.static_modelling.static_model
WHERE satnum IS NOT NULL
GROUP BY satnum
ORDER BY satnum;


-- ============================================================================
-- 14. WHAT NET TO GROSS DOES TO THE VOLUMES
-- ============================================================================
-- The number a static model exists to produce. Summing porosity across the
-- solved cells gives the gross figure; summing porosity times net-to-gross
-- gives the net. On Norne that is a 13 percent reduction, on the sector model
-- 24 percent, and both come out of the same two columns of the same table.

ASSERT ROW_COUNT = 2
ASSERT VALUE cells = 44927 WHERE model = 'norne'
ASSERT VALUE gross_units = 10878 WHERE model = 'norne'
ASSERT VALUE net_units = 9467 WHERE model = 'norne'
ASSERT VALUE reduction_pct = 13 WHERE model = 'norne'
ASSERT VALUE cells = 7152 WHERE model = 'sector'
ASSERT VALUE gross_units = 1489 WHERE model = 'sector'
ASSERT VALUE net_units = 1132 WHERE model = 'sector'
ASSERT VALUE reduction_pct = 24 WHERE model = 'sector'
SELECT model,
       COUNT(*)                                                           AS cells,
       CAST(ROUND(SUM(poro)) AS BIGINT)                                   AS gross_units,
       CAST(ROUND(SUM(poro * ntg)) AS BIGINT)                             AS net_units,
       CAST(ROUND(100.0 * (1.0 - SUM(poro * ntg) / SUM(poro))) AS BIGINT) AS reduction_pct
FROM {{zone_name}}.static_modelling.static_model
GROUP BY model
ORDER BY model;


-- ============================================================================
-- 15. PERMEABILITY FOLLOWS POROSITY
-- ============================================================================
-- Not a coincidence: it is the relationship a static model is built on, and a
-- deck where it did not hold would be one somebody should look at. In the
-- sector model every layer with more porosity than the one below it also has
-- more permeability, and no pair breaks it.

ASSERT ROW_COUNT = 0
SELECT a.k AS upper_layer, b.k AS lower_layer,
       a.poro AS upper_poro, b.poro AS lower_poro,
       a.permx AS upper_permx, b.permx AS lower_permx
FROM (
    SELECT k, MIN(poro) AS poro, MIN(permx) AS permx
    FROM {{zone_name}}.static_modelling.static_model
    WHERE model = 'sector'
    GROUP BY k
) a
JOIN (
    SELECT k, MIN(poro) AS poro, MIN(permx) AS permx
    FROM {{zone_name}}.static_modelling.static_model
    WHERE model = 'sector'
    GROUP BY k
) b
  ON b.k = a.k + 1
WHERE a.poro > b.poro
  AND a.permx <= b.permx;


-- ============================================================================
-- 16. EVERY DECK LOADED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE cells = 44927 WHERE source_file = '2026-03-11_norne_2004.grdecl'
ASSERT VALUE distinct_cells = 44927 WHERE source_file = '2026-03-11_norne_2004.grdecl'
ASSERT VALUE cells = 7152 WHERE source_file = '2026-03-12_sector_model.grdecl'
ASSERT VALUE distinct_cells = 7152 WHERE source_file = '2026-03-12_sector_model.grdecl'
SELECT source_file,
       COUNT(*)                     AS cells,
       COUNT(DISTINCT cell_index)   AS distinct_cells
FROM {{zone_name}}.static_modelling.static_model
GROUP BY source_file
ORDER BY source_file;


-- ============================================================================
-- 17. THE STATE AFTER THE FIRST DECK, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 44927
SELECT *
FROM {{zone_name}}.static_modelling.static_model VERSION AS OF 1;


-- ============================================================================
-- 18. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.static_modelling.static_model;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Both models as the simulation team would sign them off: 52,079 solved cells
-- across two decks that share nothing but a format, one written value by value
-- and one written as repeat counts.

ASSERT ROW_COUNT = 2
ASSERT VALUE cells = 44927 WHERE model = 'norne'
ASSERT VALUE layers = 21 WHERE model = 'norne'
ASSERT VALUE regions = 0 WHERE model = 'norne'
ASSERT VALUE mean_poro_pct = 24 WHERE model = 'norne'
ASSERT VALUE mean_permx = 389 WHERE model = 'norne'
ASSERT VALUE with_ntg = 44927 WHERE model = 'norne'
ASSERT VALUE cells = 7152 WHERE model = 'sector'
ASSERT VALUE layers = 10 WHERE model = 'sector'
ASSERT VALUE regions = 3 WHERE model = 'sector'
ASSERT VALUE mean_poro_pct = 21 WHERE model = 'sector'
ASSERT VALUE mean_permx = 697 WHERE model = 'sector'
ASSERT VALUE with_ntg = 7152 WHERE model = 'sector'
SELECT model,
       MIN(delivered_on)                                AS delivered_on,
       COUNT(*)                                         AS cells,
       COUNT(DISTINCT k)                                AS layers,
       COUNT(DISTINCT satnum)                           AS regions,
       CAST(ROUND(100.0 * AVG(poro)) AS BIGINT)         AS mean_poro_pct,
       CAST(ROUND(AVG(permx)) AS BIGINT)                AS mean_permx,
       COUNT(*) FILTER (WHERE ntg IS NOT NULL)          AS with_ntg
FROM {{zone_name}}.static_modelling.static_model
GROUP BY model
ORDER BY model;
