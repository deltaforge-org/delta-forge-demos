-- ============================================================================
-- Demo: EDI X12 Auto-Onboarding, DISCOVER over X12 EDI documents
-- Feature: DISCOVER classifies the raw bytes as EDI from the ISA/GS/ST
--          envelope, registers the external table USING EDI, and auto-detects
--          the X12 dialect. No hand-written CREATE EXTERNAL TABLE, no OPTIONS,
--          no edi_config.
-- ============================================================================
--
-- Real-world story: a distributor (RIVERBEND DISTRIBUTION) receives X12 EDI
-- documents from its trading partners. Purchase orders (transaction set 850)
-- flow out to suppliers and invoices (transaction set 810) flow back in. All
-- of it lands in a single onboarding folder as raw .edi files.
--
-- This is the EDI entry in the DISCOVER category. Rather than hand-write a
-- CREATE EXTERNAL TABLE with USING EDI and an explicit edi_config, this demo
-- points DISCOVER at the folder and lets it do the work: it reads the actual
-- bytes, recognises the ISA/GS/ST interchange envelope, classifies the feed
-- as EDI, auto-detects the X12 dialect (DISCOVER passes no edi_config; the
-- connector infers X12 from the ISA header), and registers the table. The
-- result exposes per-segment columns (including st_1, the transaction set
-- code) plus the full parsed message as JSON.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are shown
-- there), mirroring the json-clickstream discover demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables : demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
