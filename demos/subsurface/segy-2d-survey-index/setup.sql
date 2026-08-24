-- ============================================================================
-- 2D Survey CDP Index - Setup Script
-- ============================================================================
-- A processing centre delivers a reprocessed 2D survey one line at a time.
-- Before an interpreter can ask "which lines cross this prospect", somebody
-- has to build a CDP navigation index: one row per common depth point, with
-- the real-world coordinate it sits on.
--
-- The lines are real. These are two of the 34 lines of Equinor's Volve ST0299
-- 2D survey as the OSDU Forum redistributes them; see ATTRIBUTION.md in the
-- parent folder.
--
--   11 March   ST0299-05005    984 traces
--   12 March   ST0299-15010   1030 traces
--
-- Each line is about 12 MB, and the index never reads a byte of it beyond the
-- trace headers. That is the point of the demo as much as the incremental
-- load is: include_samples = 'false' turns a 12 MB line into a 240-byte read
-- per trace, and the same arithmetic is what makes indexing a 40 GB volume
-- cost the headers rather than the volume.
--
--   1. survey_lines  external, DISCOVER over the landing folder, headers only
--   2. cdp_index     DELTA, the curated navigation index
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.seismic_survey
    COMMENT 'Volve ST0299 2D survey read in place, indexed into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- SEG-Y is identified from the binary reel header, not from the .segy
-- extension. That distinction is load-bearing rather than pedantic: a CSV
-- export of trace attributes named .segy is a real thing that arrives in
-- landing folders, and claiming it because of its name produces a table that
-- reads and returns nonsense.
--
-- include_samples = 'false' drops the sample column. The trace headers carry
-- everything an index needs, and they are 240 bytes against 12000 bytes of
-- samples per trace.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.seismic_survey.survey_lines;

DISCOVER {{zone_name}}.seismic_survey.survey_lines
    PATH '{{data_subdir}}/landing'
    WITH (
        FILE_METADATA = true,
        include_samples = 'false'
    );


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta index
-- ----------------------------------------------------------------------------
-- source_x and source_y are nullable and the loader means it. Ninety-five of
-- the first line's traces and ninety-one of the second's carry a source
-- coordinate of exactly zero, which is the file saying it does not know
-- rather than the survey having been shot off the coast of Ghana. An index
-- that stored the zero would report a survey extent running to the origin.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.seismic_survey.cdp_index (
    line                VARCHAR,
    delivered_on        VARCHAR,
    source_file         VARCHAR,
    trace_sequence_line INTEGER,
    field_record        INTEGER,
    cdp                 INTEGER,
    offset_m            INTEGER,
    cdp_x               DOUBLE,
    cdp_y               DOUBLE,
    source_x            DOUBLE,
    source_y            DOUBLE,
    sample_count        INTEGER,
    sample_interval_us  INTEGER
) LOCATION '{{data_subdir}}/curated/cdp_index';
