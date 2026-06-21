-- ============================================================================
-- Demo: FHIR Bulk-Export Auto-Onboarding, DISCOVER over Patient NDJSON
-- Feature: DISCOVER auto-detects the file format from raw bytes, synthesizes
--          the flatten config, and registers an external table in one
--          statement. No hand-written CREATE EXTERNAL TABLE, no OPTIONS, no
--          json_flatten_config.
-- ============================================================================
--
-- Real-world story: a hospital runs a FHIR Bulk Data ($export) job. The job
-- emits newline-delimited JSON (NDJSON) files of Patient resources, one
-- complete FHIR R4 Patient per line. Today an analyst would hand-write a
-- CREATE EXTERNAL TABLE plus a json_flatten_config to map the FHIR paths to
-- columns before anyone could query the cohort.
--
-- This is the FHIR entry in the DISCOVER category. Instead of that manual
-- onboarding, a single DISCOVER statement reads the actual bytes of the
-- exported file, classifies the newline-delimited JSON shape (one object per
-- line, registers USING JSON), synthesizes the flatten config from the
-- top-level scalar keys, and registers the external table. Analysts can query
-- the patient cohort immediately.
--
-- This file declares ONLY the zone and schema. The external table itself is
-- created by DISCOVER in queries.sql (both PRINT and EXECUTE modes are shown
-- there), mirroring the json-clickstream demo.
-- ============================================================================

-- --------------------------------------------------------------------------
-- Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.discover_demos
    COMMENT 'Tables auto-registered by the DISCOVER command';
