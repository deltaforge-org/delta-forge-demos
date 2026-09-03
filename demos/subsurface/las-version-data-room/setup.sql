-- ============================================================================
-- Licence Divestment Data Room - Setup Script
-- ============================================================================
-- A buyer's subsurface team has three weeks with the data room for Dutch
-- block P/12 before the bid is due, and the log package in it was written
-- across forty-five years by whatever software was current at the time. Six
-- wells, three versions of the Log ASCII Standard:
--
--   P12-01  1979  LAS 1.2   gamma only, converted off tape        120 steps
--   P12-03  1984  LAS 1.2   gamma and sonic                       140 steps
--   P12-07  2003  LAS 2.0   triple combo                          160 steps
--   P12-09  2007  LAS 2.0   triple combo                          180 steps
--   P12-11  2021  LAS 3.0   triple combo plus lithology, COMMA    200 steps
--   P12-14  2024  LAS 3.0   triple combo plus lithology, TAB      220 steps
--
-- 1020 depth steps. The three versions are not cosmetic variations of one
-- format; each one changes something a reader has to get right:
--
--   1.2  puts a non-numeric well entry's LABEL before the colon and its
--        VALUE after, the opposite of every later version. Read as though it
--        were 2.0, a 1.2 file does not fail. It answers `WELL` for the name
--        of the well and `FIELD` for the field, uniformly, silently, for
--        every 1.2 file in the room. The two oldest wells in this package
--        are exactly that trap.
--   2.0  is the version everything else in the industry assumes.
--   3.0  names its sections by topic (`~Log_Definition`, `~Log_Data`), may
--        separate its readings with a comma or a tab instead of whitespace,
--        may declare a curve to be text rather than a number, and may carry
--        further topics in the same file at a completely different grain.
--        P12-14 ships its core analysis in the same file as its log.
--
-- This demo is about reading all three as one table. The other LAS demo,
-- las-well-log-library, is about merging differing curve sets within LAS 2.0;
-- this one holds the curve sets deliberately ordinary so that the version is
-- the only thing being tested.
--
-- The files were written for this demo from the CWLS standards. They are not
-- real logs and no third-party licence applies to them.
--
--   1. log_files     external, DISCOVER over the data room
--   2. room_audit    DELTA, the per-well coverage summary the bid needs
-- ============================================================================

-- ----------------------------------------------------------------------------
-- STEP 1: Zone and schema
-- ----------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.data_room
    COMMENT 'Block P/12 divestment data room: LAS 1.2, 2.0 and 3.0 read in place';


-- ----------------------------------------------------------------------------
-- STEP 2: Register the data room with DISCOVER
-- ----------------------------------------------------------------------------
-- LAS is identified from its `~` section markers rather than from a magic
-- number or an extension, and the same detection covers all three versions
-- because all three open with a version section.
--
-- One table spans the room. The curve columns come from each file's own
-- curve section and the well columns from its well section, so the version
-- differences resolve inside the reader and never reach the query. What
-- reaches the query is one table with a well name on every row.
--
-- FILE_METADATA carries `df_file_name`, which is how the audit below tells
-- one export vintage from another: the data room's export convention stamps
-- the LAS version into the file name, which is the only place outside the
-- file's own `~V` section that it is recorded.
-- ----------------------------------------------------------------------------

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.data_room.log_files;

DISCOVER {{zone_name}}.data_room.log_files
    PATH '{{data_subdir}}/data_room'
    WITH (FILE_METADATA = true);


-- ----------------------------------------------------------------------------
-- STEP 3: The curated audit table
-- ----------------------------------------------------------------------------
-- What the bid committee actually asks for is not the logs, it is one page
-- saying which wells have which curves and how much of each curve is real.
-- Every coverage column is a count of readings that are present, because in
-- a package this old the interesting number is always how much is missing:
-- the 1979 well has no density because no compensated density tool was on
-- the truck, and the 2021 well is missing a quarter of its gamma because the
-- delivered file writes those depths as an empty field.
--
-- Coverage counts are nullable-free by construction (a count is never null),
-- but every curve in the source is nullable, because a curve a tool never
-- ran is absent and not zero.
-- ----------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.data_room.room_audit (
    well          VARCHAR,
    las_version   VARCHAR,
    logged_on     VARCHAR,
    service_co    VARCHAR,
    uwi           VARCHAR,
    source_file   VARCHAR,
    steps         BIGINT,
    top_depth     DOUBLE,
    base_depth    DOUBLE,
    live_gr       BIGINT,
    live_dt       BIGINT,
    live_rhob     BIGINT,
    live_nphi     BIGINT,
    live_lith     BIGINT
) LOCATION '{{data_subdir}}/curated/room_audit';
