-- ============================================================================
-- SEG-D Field Record QC - Setup Script
-- ============================================================================
-- A land seismic crew drops one folder of SEG-D field records per acquisition
-- day. The processing centre runs a scheduled loader against that folder: each
-- day's drop is read in place and appended to a curated Delta table that the
-- QC desk and the processing geophysicists both work from.
--
-- This file declares the catalog objects only. The loads themselves live in
-- queries.sql, where the assertions between them are what prove the load is
-- incremental rather than a full reload.
--
--   1. field_records   external, registered by DISCOVER over the landing
--                      folder; headers only, one row per trace
--   2. record_1043     external, one record with its sample payload
--   3. trace_inventory DELTA, the curated target the loader appends to
--
-- SEG-D headers are binary-coded decimal, which is why none of this can be
-- done with a text tool: the file number 1041 is stored as the two bytes
-- 0x10 0x41 and reads as 4161 to anything that treats them as an integer.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.seismic_acquisition
    COMMENT 'Field seismic acquisition: SEG-D records read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the landing folder with DISCOVER
-- ----------------------------------------------------------------------------
-- DISCOVER reads the bytes at the path, recognises SEG-D from its general
-- header rather than from the file extension, and registers the external
-- table. Nothing about the format is written by hand here.
--
-- Two options ride along. FILE_METADATA adds df_file_name and df_row_number,
-- and df_file_name is what the incremental loader keys on: it is the record's
-- identity, so a file already loaded is a file the loader can skip.
--
-- include_samples = 'false' skips sample decoding. That is what makes a QC
-- pass over a whole day's acquisition cheap, and it is also what lets one
-- table span records of different geometry: the samples column is a
-- fixed-size list whose width is the record's sample count, so a 512-sample
-- record and a 1024-sample record cannot share a schema while it is present.
--
-- The DROP first is what makes this file re-runnable: DISCOVER in EXECUTE
-- mode refuses to register a name the catalog already holds. Without WITH
-- FILES, so the landed records themselves are untouched.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.seismic_acquisition.field_records;

DISCOVER {{zone_name}}.seismic_acquisition.field_records
    PATH '{{data_subdir}}/landing'
    WITH (
        FILE_METADATA = true,
        include_samples = 'false'
    );


-- ----------------------------------------------------------------------------
-- STEP 3: One record with its sample payload
-- ----------------------------------------------------------------------------
-- The same command pointed at a single file rather than a folder, with sample
-- decoding left on. The samples arrive as one fixed-size array per trace, so
-- a trace stays a trace instead of becoming 512 columns.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.seismic_acquisition.record_1043;

DISCOVER {{zone_name}}.seismic_acquisition.record_1043
    PATH '{{data_subdir}}/landing/2026-03-11_fr_1043.segd';


-- ----------------------------------------------------------------------------
-- STEP 4: The curated Delta target
-- ----------------------------------------------------------------------------
-- Created empty. source_file carries the name of the record each row came
-- from, which is both the audit trail and the loader's watermark: the loader
-- appends a record's traces only when that record is not already represented
-- here, so re-running it is a no-op rather than a duplicate.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.seismic_acquisition.trace_inventory (
    acquisition_date   VARCHAR,
    source_file        VARCHAR,
    file_number        INTEGER,
    scan_type          INTEGER,
    channel_set        INTEGER,
    trace_number       INTEGER,
    sample_count       INTEGER,
    sample_interval_us INTEGER
) LOCATION '{{data_subdir}}/curated/trace_inventory';
