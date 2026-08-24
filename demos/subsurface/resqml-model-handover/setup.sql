-- ============================================================================
-- Reservoir Model Handover Audit - Setup Script
-- ============================================================================
-- A partner delivers a static reservoir model for a unitisation study as a
-- RESQML package. Before the asset team loads it into the modelling software
-- somebody has to audit what actually arrived, and the question that matters
-- is not how many objects there are: it is which of them point at bulk arrays
-- living in a companion HDF5 file that may or may not have been delivered.
--
-- That is the classic handover failure. The .epc opens, every object is
-- present, every citation is filled in, and the grid has no geometry because
-- the .h5 it names never arrived.
--
--   11 March   static_model_v1   14 objects
--   12 March   static_model_v2   18 objects, plus one unpacked loose part
--
--   1. model_packages   external, DISCOVER over the .epc deliveries
--   2. unpacked_parts   external, DISCOVER over the loose part
--   3. model_inventory  DELTA, the audit register
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.model_handover
    COMMENT 'RESQML packages read in place, audited into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register both shapes with DISCOVER
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
-- The loose part is registered separately. RESQML is also exchanged unpacked,
-- one object per .xml file, and a bare .xml sitting next to .epc files would
-- not be picked up by a scan that filters on the package extension.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.model_handover.model_packages;

DISCOVER {{zone_name}}.model_handover.model_packages
    PATH '{{data_subdir}}/landing/packages'
    WITH (FILE_METADATA = true);

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.model_handover.unpacked_parts;

DISCOVER {{zone_name}}.model_handover.unpacked_parts
    PATH '{{data_subdir}}/landing/unpacked'
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
    model_version         VARCHAR,
    delivered_on          VARCHAR,
    source_file           VARCHAR,
    part_name             VARCHAR,
    object_type           VARCHAR,
    uuid                  VARCHAR,
    title                 VARCHAR,
    originator            VARCHAR,
    reference_count       INTEGER,
    external_array_count  INTEGER,
    external_arrays       VARCHAR
) LOCATION '{{data_subdir}}/curated/model_inventory';
