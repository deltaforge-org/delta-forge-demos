-- ============================================================================
-- LIS Tape Archive Recovery - Setup Script
-- ============================================================================
-- An operator's data-management team is bringing a well-log tape archive into
-- the lake. The tapes are LIS-79, the format that preceded DLIS, and they are
-- wrapped in Tape Image Format: twelve bytes of tape framing around every
-- physical record. Nothing modern opens them without a vendor conversion
-- step, which is exactly what the team is trying to avoid paying for a second
-- time.
--
-- The archive is digitised in batches, and the loader runs per batch:
--
--   11 March   The Volve 15/9-F-4 composite. 12 curves, 21505 frames.
--   12 March   A reprocessed composite for 15/9-F-4 A. 26 curves, 640 frames.
--
-- The first tape is real. It is Equinor's Volve 15/9-F-4 composite log as the
-- OSDU Forum redistributes it, byte for byte; see ATTRIBUTION.md in the
-- parent folder. Its awkwardness is real too, and the load has to deal with
-- it: the tape has no null convention, so a curve the tool was not recording
-- reads as a number indistinguishable from zero, and one of its density
-- correction readings is minus fifty-seven thousand grams per cubic
-- centimetre.
--
-- The second tape is written by generate_data.py, because no open corpus
-- publishes a second LIS tape. It carries the shape the real one does not: a
-- 26-curve specification is 1049 bytes against the 1024-byte physical record
-- cap, so it arrives split across two physical records with the successor and
-- predecessor bits set.
--
--   1. volve_composite       external, DISCOVER over the 11 March batch
--   2. reprocessed_composite external, DISCOVER over the 12 March batch
--   3. tape_archive          DELTA, the curated target
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.log_archive
    COMMENT 'Legacy LIS-79 well-log tapes read in place, curated into Delta';


-- ----------------------------------------------------------------------------
-- STEP 2: Register both batches with DISCOVER
-- ----------------------------------------------------------------------------
-- LIS is identified from the physical record structure underneath the tape
-- framing, which means detection has to strip the framing first. A detector
-- that reads the raw head sees twelve bytes of little-endian tape offsets and
-- concludes nothing.
--
-- The two batches are registered separately because their curve sets differ.
-- Keeping them apart is what lets the loader project each onto the core the
-- curated table keeps, rather than widening the table for every reprocessing
-- product a vendor happened to ship.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.log_archive.volve_composite;

DISCOVER {{zone_name}}.log_archive.volve_composite
    PATH '{{data_subdir}}/landing/volve'
    WITH (FILE_METADATA = true);

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.log_archive.reprocessed_composite;

DISCOVER {{zone_name}}.log_archive.reprocessed_composite
    PATH '{{data_subdir}}/landing/reprocessed'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated Delta target
-- ----------------------------------------------------------------------------
-- Nine core curves, chosen because every tape in the archive carries them.
-- The reprocessed tape's other seventeen are reprocessing products, not
-- measurements, and a curated table that grew a column every time a vendor
-- shipped one would be unusable within a year.
--
-- Every curve here is nullable and the loader means it. On these tapes a
-- reading the tool did not take is written as a number, so the load's job is
-- to decide which numbers are measurements and null the rest. The rules it
-- applies are in queries.sql, stated rather than buried.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.log_archive.tape_archive (
    tape           VARCHAR,
    well           VARCHAR,
    digitised_on   VARCHAR,
    source_file    VARCHAR,
    frame_number   BIGINT,
    dept           DOUBLE,
    gr             DOUBLE,
    cali           DOUBLE,
    rdep           DOUBLE,
    rmed           DOUBLE,
    den            DOUBLE,
    neu            DOUBLE,
    ac             DOUBLE,
    bs             DOUBLE
) LOCATION '{{data_subdir}}/curated/tape_archive';
