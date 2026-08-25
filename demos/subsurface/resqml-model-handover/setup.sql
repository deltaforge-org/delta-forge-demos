-- ============================================================================
-- Reservoir Model Handover Audit - Setup Script
-- ============================================================================
-- A partner delivers reservoir models as RESQML packages. Before the asset
-- team loads them into the modelling software somebody has to audit what
-- actually arrived: what objects, how they reference each other, and which of
-- them point at bulk arrays living in a companion HDF5 file.
--
--   11 March   block          26 objects
--   12 March   s_bend         24 objects
--              tic_tac_toe     6 objects
--
-- The packages are REAL. They are the example models from the resqpy project
-- (bp/resqpy, MIT), written by resqpy itself, and their .h5 companions are
-- delivered alongside so the handover is complete rather than illustrative.
-- See ATTRIBUTION.md in the parent folder.
--
--   1. model_packages   external, DISCOVER over the .epc deliveries
--   2. model_inventory  DELTA, the audit register
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.model_handover
    COMMENT 'RESQML packages read in place, audited into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the packages with DISCOVER
-- ----------------------------------------------------------------------------
-- An .epc is an Open Packaging Conventions ZIP, and DeltaForge opens the
-- archive itself rather than handing it to the XML engine, because an XML
-- engine reads files and this is a container.
--
-- That distinction is load bearing. A ZIP holding XML parts is also what an
-- .xlsx is, so a detector that stops at the ZIP header calls a reservoir
-- model a spreadsheet. RESQML is claimed by its own signature before the
-- Office Open XML check ever runs.
--
-- The .h5 companions sit in a folder of their own. They are the bulk arrays
-- these packages name, they are delivered with them, and nothing here reads
-- them: RESQML records the reference and the reader records it too.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.model_handover.model_packages;

DISCOVER {{zone_name}}.model_handover.model_packages
    PATH '{{data_subdir}}/landing/packages'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta audit register
-- ----------------------------------------------------------------------------
-- external_arrays is the column the audit turns on. The reader records the
-- HDF5 dataset an object names WITHOUT following it, so a dependency that was
-- never delivered is visible as a row rather than as a grid that silently has
-- no geometry.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.model_handover.model_inventory (
    model                 VARCHAR,
    delivered_on          VARCHAR,
    source_file           VARCHAR,
    part_name             VARCHAR,
    object_type           VARCHAR,
    uuid                  VARCHAR,
    title                 VARCHAR,
    originator            VARCHAR,
    schema_version        VARCHAR,
    reference_count       INTEGER,
    external_array_count  INTEGER,
    external_arrays       VARCHAR
) LOCATION '{{data_subdir}}/curated/model_inventory';
