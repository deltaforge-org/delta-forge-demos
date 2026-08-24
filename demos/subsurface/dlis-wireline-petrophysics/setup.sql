-- ============================================================================
-- Volve Composite Log Ingestion - Setup Script
-- ============================================================================
-- A petrophysicist is building an analysis-ready log table for the Volve
-- field. The logs arrive as DLIS composites in two waves, from two different
-- tool strings:
--
--   11 March   LWD composites for 15/9-F-9 and 15/9-F-11. Six channels:
--              DEPTH, GR, RDEP, RMED, ROP, BS. Recorded while drilling.
--   12 March   The wireline composite for 15/9-F-15 C. Twelve channels: the
--              six above plus CALI, DEN, DENC, PEF, NEU and AC, which is the
--              triple-combo suite a petrophysical evaluation needs.
--
-- The second wave is wider than the first, so the curated table has to grow
-- when it lands. That is not a contrived demo step: it is what happens every
-- time a wireline run follows an LWD pass.
--
-- The data is real. These are the Volve composite logs Equinor released and
-- the OSDU Forum redistributes in its open test data; see ATTRIBUTION.md in
-- the parent folder. Nothing here was written for the demo, which is why the
-- awkward parts are present: the depth channel is recorded in tenths of an
-- inch in two of the files and in millimetres in the third, and every channel
-- uses -999.25 for a reading that was not taken.
--
--   1. lwd_composite       external, DISCOVER over the LWD drop
--   2. wireline_composite  external, DISCOVER over the wireline drop
--   3. well_logs           DELTA, the curated target, depths in metres
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.petrophysics
    COMMENT 'Volve composite logs read in place from DLIS, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register both drops with DISCOVER
-- ----------------------------------------------------------------------------
-- DLIS is identified from the storage unit label and the visible-record
-- structure, not from the file extension. No column is named by hand here:
-- the channels come from the CHANNEL and FRAME sets inside each file, which
-- is the only place they exist.
--
-- The two tool strings are registered separately because they are separate
-- deliveries with separate channel sets, and keeping them apart is what lets
-- the loader treat the wider one as the schema change it is.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.petrophysics.lwd_composite;

DISCOVER {{zone_name}}.petrophysics.lwd_composite
    PATH '{{data_path}}/landing/lwd'
    WITH (FILE_METADATA = true);

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.petrophysics.wireline_composite;

DISCOVER {{zone_name}}.petrophysics.wireline_composite
    PATH '{{data_path}}/landing/wireline'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta target
-- ----------------------------------------------------------------------------
-- Created with the LWD channel set only, because on 11 March that is all
-- there is. The wireline columns are added in queries.sql at the point the
-- wireline delivery lands, which is the honest order of events.
--
-- depth_m is the reason this table exists. The DLIS files record depth in
-- whichever unit the logging contractor's system used, and two of these three
-- disagree; a table that appended the raw channel would hold a depth column
-- mixing tenths of an inch with millimetres and no way to tell which is
-- which. The loader converts on the way in and the curated column carries one
-- unit, named in the column.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.petrophysics.well_logs (
    well            VARCHAR,
    tool_string     VARCHAR,
    delivered_on    VARCHAR,
    source_file     VARCHAR,
    frame_number    BIGINT,
    depth_m         DOUBLE,
    gr              DOUBLE,
    rdep            DOUBLE,
    rmed            DOUBLE,
    rop             DOUBLE,
    bs              DOUBLE
) LOCATION '{{data_path}}/curated/well_logs';
