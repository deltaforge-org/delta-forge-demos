-- ============================================================================
-- Demo: EU Point-of-Sale Onboarding, DISCOVER over semicolon-delimited CSV
-- Feature: DISCOVER sniffs the CSV dialect from the raw bytes (delimiter ';',
--          header present), synthesizes the right OPTIONS, and registers an
--          external table in one statement. No hand-written CREATE EXTERNAL
--          TABLE, no manual DELIMITER ';' option.
-- ============================================================================
--
-- Real-world story: a European retail chain exports its point-of-sale receipts
-- as semicolon-delimited CSV, the common EU convention (a comma is the decimal
-- separator in most European locales, so the field separator is ';'). The file
-- carries a header row and a clean '.' decimal point in the price columns.
--
-- This is the CSV dialect-detection entry in the DISCOVER category. A naive
-- comma reader pointed at this file would see ONE giant column, because there
-- are no commas to split on. DISCOVER instead reads the actual bytes, sniffs
-- the dialect (it finds ';' as the consistent field separator and a header
-- row), and emits the matching OPTIONS automatically.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are shown
-- there), mirroring the discover/json-clickstream demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables, demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
