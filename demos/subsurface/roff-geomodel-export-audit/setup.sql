-- ============================================================================
-- Geomodel Export Audit - Setup Script
-- ============================================================================
-- A geomodeller finishes a reservoir model in RMS and hands it to the
-- simulation team, who are about to spend weeks history matching against it.
-- Before they start, they audit the handover: is the GRDECL export the
-- modeller produced actually the same model as the ROFF file RMS holds?
--
--   02 April   reservoir_model.roff      the model itself, binary ROFF
--   02 April   reservoir_model.roffasc   the same model in ASCII, for review
--   03 April   grid_export.grdecl        the export the simulator will run
--   04 April   top_reservoir.roffasc     the depth surface, no grid at all
--
-- ROFF is what RMS keeps a model in; GRDECL is what leaves for the simulator.
-- They describe the same 20 x 15 x 8 grid and the same four properties, and
-- they number their cells in OPPOSITE DIRECTIONS. ROFF stores a per-cell array
-- with K varying fastest and I slowest; ECLIPSE, and so GRDECL, runs I fastest
-- and K slowest. The same cell therefore has a different ordinal in each file.
--
-- That is the whole reason this audit is worth running, and query 11 is the
-- reason it is worth running carefully: joining the export to the original on
-- the cell ordinal returns 968 rows that look entirely convincing, and not one
-- of them compares the same cell to itself.
--
-- All four files are synthetic. Every value is a deterministic function of
-- (i, j, k), written by generate_data.py, and every number asserted in
-- queries.sql was recomputed from the files on disk by compute_proofs.py,
-- which parses them with its own reader rather than trusting the writer.
--
--   1. model          external, the ROFF model as the simulator solves it
--   2. model_all      external, the same file with its inactive cells kept
--   3. model_ascii    external, the ASCII copy, which must read identically
--   4. grid_export    external, the GRDECL export
--   5. top_reservoir  external, a ROFF surface, which reads in long form
--   6. audited_model  DELTA, the reconciled model
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.handover
    COMMENT 'An RMS model handover, read in place and audited against its export';


-- ----------------------------------------------------------------------------
-- STEP 2: The model RMS holds
-- ----------------------------------------------------------------------------
-- One row per cell, with i, j and k derived from the dimensions tag, then one
-- column per parameter. A discrete parameter that names its codes gets a
-- companion label column, so facies arrives as both the code and the word.
--
-- Inactive cells are dropped by default. A cell outside the model still has
-- property values in the file and they mean nothing, so averaging them in is
-- how a model's porosity quietly drifts.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.model;

DISCOVER {{zone_name}}.handover.model
    PATH '{{data_subdir}}/landing/2026-04-02_reservoir_model.roff'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The same file, with its inactive cells kept
-- ----------------------------------------------------------------------------
-- What the grid covers, as opposed to what the simulator solves. Both are real
-- questions and on this model they differ by 860 cells, one whole layer of
-- which the modeller excluded as a shale break.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.model_all;

DISCOVER {{zone_name}}.handover.model_all
    PATH '{{data_subdir}}/landing/2026-04-02_reservoir_model.roff'
    WITH (include_inactive = 'true');


-- ----------------------------------------------------------------------------
-- STEP 4: The ASCII copy the modeller sent for review
-- ----------------------------------------------------------------------------
-- ROFF has a binary form and an ASCII form of the same grammar, and a modeller
-- who wants a colleague to read the file sends the second. They must decode to
-- the same numbers or one of the two reviews the wrong model.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.model_ascii;

DISCOVER {{zone_name}}.handover.model_ascii
    PATH '{{data_subdir}}/landing/2026-04-02_reservoir_model.roffasc';


-- ----------------------------------------------------------------------------
-- STEP 5: The export the simulator will actually run
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.grid_export;

DISCOVER {{zone_name}}.handover.grid_export
    PATH '{{data_subdir}}/landing/2026-04-03_grid_export.grdecl';

-- The deck with its inactive cells kept, which is the only way to line the two
-- orderings up side by side: the first eight ordinals of the deck are cells
-- outside the model outline, so an active-only table has none of them.

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.grid_export_all;

DISCOVER {{zone_name}}.handover.grid_export_all
    PATH '{{data_subdir}}/landing/2026-04-03_grid_export.grdecl'
    WITH (include_inactive = 'true');


-- ----------------------------------------------------------------------------
-- STEP 6: The depth surface, which is not a grid
-- ----------------------------------------------------------------------------
-- RMS writes surfaces, points, polygons and wells into the same container as
-- its grids. A surface carries no dimensions tag, so there are no cells to
-- hang rows on and the reader reports the container itself: one row per
-- element of one key of one tag. Nothing is transcribed and nothing is
-- refused.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.handover.top_reservoir;

DISCOVER {{zone_name}}.handover.top_reservoir
    PATH '{{data_subdir}}/landing/2026-04-04_top_reservoir.roffasc';


-- ----------------------------------------------------------------------------
-- STEP 7: The reconciled model
-- ----------------------------------------------------------------------------
-- The audit's output: every active cell, the property values RMS holds, the
-- value the export carries for that same cell, and the difference between
-- them. poro is nullable because RMS leaves cells undefined and the export has
-- no way to say so, which is one of the two things this audit finds.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.handover.audited_model (
    model_name     VARCHAR,
    delivered_on   VARCHAR,
    source_file    VARCHAR,
    cell_index     INTEGER,
    i              INTEGER,
    j              INTEGER,
    k              INTEGER,
    zone_label     VARCHAR,
    poro           DOUBLE,
    permx          DOUBLE,
    ntg            DOUBLE,
    facies_code    INTEGER,
    facies_name    VARCHAR,
    export_poro    DOUBLE,
    poro_delta     DOUBLE
) LOCATION '{{data_subdir}}/curated/audited_model';
