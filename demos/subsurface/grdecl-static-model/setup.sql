-- ============================================================================
-- Static Model Deck Ingestion - Setup Script
-- ============================================================================
-- A static-model archive receives ECLIPSE GRDECL decks from two places, and
-- the simulation team loads both to check the property distributions before
-- building a run: is the porosity sensible layer by layer, does permeability
-- follow it, and how many cells does the model actually solve.
--
--   11 March   norne_2004     the Norne field's own model, 46 x 112 x 22
--   12 March   sector_model   a coarse sector model, 30 x 24 x 10
--
-- The two decks are written completely differently, which is the point of
-- loading them through one reader:
--
--   norne_2004     453,376 values, written out one per cell, 5.7 MB
--   sector_model    36,000 values, written as 62 tokens, 1.1 kB
--
-- A GRDECL property is RUN-LENGTH ENCODED: `720*0.263` is seven hundred and
-- twenty cells of 0.263, not one cell holding a string. The Norne deck happens
-- to use no repeats at all and the sector deck is almost nothing but repeats,
-- so between them they exercise both halves of the parser.
--
-- The Norne deck is real. See ATTRIBUTION.md in the parent folder: it is the
-- published Norne model under the Open Database License, assembled from the
-- property files it ships as separate INCLUDE files with its own grid
-- dimensions in front, which is what a simulator sees once it has resolved
-- those includes. The property blocks are byte for byte the files they came
-- from, licence headers and all.
--
--   1. norne        external, DISCOVER over the real deck
--   2. norne_all    external, the same deck with its inactive cells kept
--   3. sector       external, DISCOVER over the run-length encoded deck
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
-- from the grid dimensions. That derivation is what makes several properties
-- of one model line up on the same cell without a join: porosity and
-- permeability are different keywords in the file and the same row here.
--
-- The decks are registered separately because they are different grids with
-- different keywords, and a curated table that holds both is the right place
-- to reconcile that rather than the scan.
--
-- Inactive cells are dropped by default. A cell outside the model has property
-- values in the file and no meaning, and averaging it in is how a model's
-- porosity quietly drifts. On Norne that matters more than it sounds: three
-- cells in five are inactive.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.norne;

DISCOVER {{zone_name}}.static_modelling.norne
    PATH '{{data_subdir}}/landing/2026-03-11_norne_2004.grdecl'
    WITH (FILE_METADATA = true);

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.sector;

DISCOVER {{zone_name}}.static_modelling.sector
    PATH '{{data_subdir}}/landing/2026-03-12_sector_model.grdecl'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The Norne deck with its inactive cells kept
-- ----------------------------------------------------------------------------
-- include_inactive is how you ask what the model's full extent is, as opposed
-- to what it solves. Both are real questions and on a real field model they
-- have very different answers.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.norne_all;

DISCOVER {{zone_name}}.static_modelling.norne_all
    PATH '{{data_subdir}}/landing/2026-03-11_norne_2004.grdecl'
    WITH (include_inactive = 'true');


-- ----------------------------------------------------------------------------
-- STEP 4: The curated Delta model
-- ----------------------------------------------------------------------------
-- satnum is nullable because Norne's deck does not carry saturation regions
-- and the sector model does. ntg is carried by both. A curated table that
-- defaulted a missing property rather than leaving it null would make two
-- models agree on something neither deck actually says.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.static_modelling.static_model (
    model          VARCHAR,
    delivered_on   VARCHAR,
    source_file    VARCHAR,
    cell_index     INTEGER,
    i              INTEGER,
    j              INTEGER,
    k              INTEGER,
    poro           DOUBLE,
    permx          DOUBLE,
    ntg            DOUBLE,
    satnum         DOUBLE
) LOCATION '{{data_subdir}}/curated/static_model';
