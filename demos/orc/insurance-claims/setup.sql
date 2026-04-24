-- ==========================================================================
-- Demo: ORC Insurance Claims — Policy Cross-Reference
-- Feature: Complex JOINs and subqueries across ORC tables
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.orc_insurance
    COMMENT 'ORC-backed insurance tables';

-- --------------------------------------------------------------------------
-- Table 1: policies — 80 insurance policies
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_insurance.policies
USING ORC
LOCATION '{{data_path}}/policies.orc';

DETECT SCHEMA FOR TABLE {{zone_name}}.orc_insurance.policies;

-- --------------------------------------------------------------------------
-- Table 2: claims — 200 claims (180 reference valid policies, 20 orphaned)
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_insurance.claims
USING ORC
LOCATION '{{data_path}}/claims.orc';

DETECT SCHEMA FOR TABLE {{zone_name}}.orc_insurance.claims;
