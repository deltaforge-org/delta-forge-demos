-- ============================================================================
-- Demo: Book Catalog Auto-Onboarding, DISCOVER over XML
-- Feature: DISCOVER auto-detects the file format from raw bytes, classifies
--          the text shape as XML, synthesizes the xml_flatten_config (row
--          anchor = the repeating element), and registers an external table
--          in one statement. No hand-written CREATE EXTERNAL TABLE, no OPTIONS.
-- ============================================================================
--
-- Real-world story: a publisher exports its book catalog as a single XML file.
-- Each book is a repeating <book> element under a <catalog> root, with flat
-- leaf elements (isbn, title, author, genre, price, year, in_stock). Nobody
-- hand-writes a flatten config: DISCOVER reads the bytes, recognises the XML
-- shape, picks the repeating <book> element as the row anchor, and registers
-- the table.
--
-- This is the XML entry in the DISCOVER category. Rather than hand-write a
-- CREATE EXTERNAL TABLE with an explicit xml_flatten_config (the approach the
-- other XML demos take), this demo points DISCOVER at the folder and lets it
-- do the work: it classifies the text shape (not the file extension),
-- synthesizes the row_xpath and the leaf-element columns, and registers the
-- table.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are shown
-- there), mirroring the json-clickstream demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables, demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
