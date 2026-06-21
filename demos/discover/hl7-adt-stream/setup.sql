-- ============================================================================
-- Demo: HL7 v2 ADT Auto-Onboarding, DISCOVER over an HL7 message stream
-- Feature: DISCOVER classifies the raw text shape as HL7 (the MSH segment
--          header), registers the table USING HL7 with the engine default
--          mapping, and exposes the MSH header fields plus df_message_json
--          in one statement. No hand-written hl7_config, no OPTIONS.
-- ============================================================================
--
-- Real-world story: a regional Health Information Exchange (Northwind HIE)
-- receives HL7 v2 ADT (admit / discharge / transfer) messages from several
-- hospital EHRs (Harborview, St. Mary's, Evergreen). Each EHR speaks a
-- different HL7 version (2.3, 2.5.1, 2.6) and emits a mix of trigger events:
-- admissions (A01), discharges (A03), and a demographics update (A08).
--
-- This is the HL7 entry in the DISCOVER category. Rather than hand-write a
-- CREATE EXTERNAL TABLE ... USING HL7 with an explicit hl7_config (the
-- approach the hl7/hl7-patient-admin demo takes), this demo points DISCOVER
-- at the folder and lets it do the work: it reads the actual bytes, sees the
-- MSH|^~\& segment header that marks an HL7 v2 message, registers the table
-- USING HL7 with the engine default mapping, and exposes the MSH header
-- fields (msh_1 .. msh_21) plus df_message_json and df_message_id.
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
