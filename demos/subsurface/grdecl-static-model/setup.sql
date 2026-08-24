-- ============================================================================
-- Static Model Deck Ingestion - Setup Script
-- ============================================================================
-- A geomodeller exports the static reservoir model as an ECLIPSE GRDECL deck
-- and hands it to the simulation team, who load it to check the property
-- distributions before building a run from it: is the porosity sensible layer
-- by layer, does the permeability follow it, and how many cells does the
-- model actually solve.
--
--   11 March   static_model_v1   ACTNUM, PORO, PERMX, SATNUM
--   12 March   static_model_v2   the same plus NTG, added after the volumes
--                                looked optimistic
--
-- The grid is 30 by 24 by 10, so 7200 cells, of which 7152 are inside the
-- fault block. Each deck is about a kilobyte, because a GRDECL property is
-- run-length encoded: `720*0.263` is seven hundred and twenty cells, and a
-- whole property here is ten tokens.
--
--   1. model_v1     external, DISCOVER over the first deck
--   2. model_v2     external, DISCOVER over the revision
--   3. all_cells    external, the first deck with its inactive cells kept
--   4. static_model DELTA, the curated model
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.static_modelling
    COMMENT 'GRDECL static model decks read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register both decks with DISCOVER
-- ----------------------------------------------------------------------------
-- One row per cell, one column per property keyword, with i, j and k derived
-- from SPECGRID. That derivation is what makes several properties of one
-- model line up on the same cell without a join: porosity and permeability
-- are different keywords in the file and the same row here.
--
-- The two versions are registered separately because the revision carries a
-- keyword the first does not, and a curated table that has to hold both is
-- the right place to reconcile that rather than the scan.
--
-- Inactive cells are dropped by default. A cell outside the fault block has
-- property values in the file and no meaning, and averaging it in is how a
-- model's porosity quietly drifts.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.model_v1;

DISCOVER {{zone_name}}.static_modelling.model_v1
    PATH '{{data_subdir}}/landing/2026-03-11_static_model_v1.grdecl'
    WITH (FILE_METADATA = true);

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.model_v2;

DISCOVER {{zone_name}}.static_modelling.model_v2
    PATH '{{data_subdir}}/landing/2026-03-12_static_model_v2.grdecl'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The same deck with its inactive cells kept
-- ----------------------------------------------------------------------------
-- include_inactive is how you ask what the model's full extent is, as opposed
-- to what it solves. Both are real questions and they have different answers.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.all_cells;

DISCOVER {{zone_name}}.static_modelling.all_cells
    PATH '{{data_subdir}}/landing/2026-03-11_static_model_v1.grdecl'
    WITH (include_inactive = 'true');


-- ----------------------------------------------------------------------------
-- STEP 4: The curated Delta model
-- ----------------------------------------------------------------------------
-- ntg is nullable because the first version genuinely does not have it. A
-- curated table that defaulted it to 1.0 would make the two versions agree on
-- pore volume, which is exactly the comparison the revision was made for.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.static_modelling.static_model (
    model_version  VARCHAR,
    delivered_on   VARCHAR,
    source_file    VARCHAR,
    cell_index     INTEGER,
    i              INTEGER,
    j              INTEGER,
    k              INTEGER,
    poro           DOUBLE,
    permx          DOUBLE,
    satnum         DOUBLE,
    ntg            DOUBLE
) LOCATION '{{data_subdir}}/curated/static_model';
