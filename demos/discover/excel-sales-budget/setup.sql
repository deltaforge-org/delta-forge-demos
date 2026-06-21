-- ============================================================================
-- Demo: Regional Sales-Budget Auto-Onboarding, DISCOVER over Excel (.xlsx)
-- Feature: DISCOVER auto-detects the file format from raw bytes, reads the
--          first sheet's header row, infers column types, and registers an
--          external table in one statement. No hand-written CREATE EXTERNAL
--          TABLE, no OPTIONS, no sheet_name argument.
-- ============================================================================
--
-- Real-world story: a finance team drops the regional sales-budget workbook
-- into a landing folder each quarter. The file is an Excel .xlsx workbook,
-- a single sheet with a header row then the budget rows. DISCOVER recognises
-- it as Excel by its OOXML/ZIP magic header (the bytes PK\x03\x04 at the
-- start of the file), not by the .xlsx extension, then reads the first
-- sheet's header row to name the columns and infer their types.
--
-- This is the Excel entry in the DISCOVER category. Rather than hand-write a
-- CREATE EXTERNAL TABLE USING EXCEL with sheet_name and has_header options,
-- this demo points DISCOVER at the folder and lets it do the work: it reads
-- the actual bytes (binary magic-number classification, not the file
-- extension), opens the first sheet, treats the first row as the header, and
-- registers the table.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are
-- shown there), mirroring the discover/json-clickstream demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables, demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
