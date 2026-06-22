-- ============================================================================
-- Demo: Healthcare Interoperability Onboarding, DISCOVER across HL7, FHIR, EDI
-- Feature: DISCOVER auto-detects three different healthcare standards from raw
--          bytes, makes each instantly queryable as an external table, and the
--          queries then promote each into a governed Delta table.
-- ============================================================================
--
-- Real-world story: a hospital integration team receives clinical and billing
-- data in three different healthcare interchange standards, all landing in one
-- bronze zone:
--
--   hl7/   HL7 v2 ADT messages   (admit / discharge / transfer events)
--   fhir/  FHIR Patient resources (NDJSON bulk export, one per line)
--   edi/   X12 EDI claims         (837 professional/dental/institutional + 835 remittance)
--
-- Instead of hand-writing a CREATE EXTERNAL TABLE with the right connector and
-- OPTIONS for each standard, the team points DISCOVER at each landing folder.
-- DISCOVER reads the actual bytes, recognises HL7 (the MSH segment), FHIR
-- (the resource shape), and X12 EDI (the ISA envelope), and registers the
-- matching external table on its own. The queries.sql script then queries each
-- bronze feed on the fly and converts it into a typed silver Delta table.
--
-- This file declares ONLY the zone and the healthcare schema. Every external
-- table is created by DISCOVER in queries.sql, and every Delta table is
-- created there too, so the whole onboarding flow is visible end to end.
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.healthcare
    COMMENT 'Healthcare interoperability: HL7 v2, FHIR, and X12 EDI, auto-onboarded by DISCOVER';
