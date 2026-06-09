-- ============================================================================
-- Pseudonymisation Lifecycle — Insurance Claims — Setup Script
-- ============================================================================
-- Creates an insurance claims table with sample data and applies 5
-- pseudonymisation rules covering different PII columns. These rules
-- demonstrate the full rule lifecycle: creation, review, selective removal,
-- and verification.
--
-- Table created:
--   1. insurance_claims — Insurance claim records with PII fields
--
-- Pseudonymisation rules applied:
--   - ssn:              redact (full replacement with mask)
--   - claimant_name:    keyed_hash (deterministic pseudonym, scope person)
--   - date_of_birth:    generalize (reduce precision, scope relationship)
--   - policy_holder_id: tokenize (opaque token, scope person)
--   - description:      mask (partial visibility, first 10 chars)
--
-- Variables (auto-injected by DeltaForge):
--   zone_name     — Target zone name (defaults to 'external')
--   data_path     — Root path where demo data files are stored
--   current_user  — Username of the current logged-in user
-- ============================================================================


-- ============================================================================
-- STEP 1: Zone & Schema
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}}
    TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.pseudonymisation_demos
    COMMENT 'Pseudonymisation demo — insurance claims with protection rules';


-- ============================================================================
-- TABLE: insurance_claims — Insurance Claim Records
-- ============================================================================
-- Contains policyholder PII (name, DOB, SSN), claim details, and status.
-- Five pseudonymisation rules protect different columns with different
-- transform types to demonstrate lifecycle management.
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation_demos.insurance_claims (
    claim_id        VARCHAR,
    policy_holder_id VARCHAR,
    claimant_name   VARCHAR,
    date_of_birth   VARCHAR,
    ssn             VARCHAR,
    claim_type      VARCHAR,
    description     VARCHAR,
    amount          DOUBLE,
    status          VARCHAR,
    filed_date      VARCHAR
) LOCATION 'pseudonymisation-lifecycle/insurance_claims';


DELETE FROM {{zone_name}}.pseudonymisation_demos.insurance_claims WHERE 1=1;

INSERT INTO {{zone_name}}.pseudonymisation_demos.insurance_claims VALUES
    ('CLM-2024-001', 'P-1001', 'John Smith', '1978-04-12', '555-12-3456', 'Auto', 'Collision damage to front bumper', 4500.00, 'approved', '2024-01-15'),
    ('CLM-2024-002', 'P-1002', 'Sarah Connor', '1985-09-30', '555-23-4567', 'Home', 'Water damage from pipe burst', 12800.00, 'approved', '2024-02-20'),
    ('CLM-2024-003', 'P-1003', 'James Wilson', '1992-07-18', '555-34-5678', 'Auto', 'Rear-end collision repair', 3200.00, 'pending', '2024-03-10'),
    ('CLM-2024-004', 'P-1001', 'John Smith', '1978-04-12', '555-12-3456', 'Life', 'Term life policy claim', 50000.00, 'under_review', '2024-04-05'),
    ('CLM-2024-005', 'P-1004', 'Maria Garcia', '1968-11-25', '555-45-6789', 'Home', 'Storm damage to roof', 8900.00, 'approved', '2024-05-18');


-- ============================================================================
-- STEP 2: Pseudonymisation Rules
-- ============================================================================
-- Five rules covering different PII columns with different transform types.
-- These rules will be managed throughout the demo lifecycle.

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.insurance_claims (ssn)
    TRANSFORM redact
    PRIORITY 20
    PARAMS (mask = '***-**-****');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.insurance_claims (claimant_name)
    TRANSFORM keyed_hash
    SCOPE person
    PRIORITY 10
    PARAMS (salt = 'insurance_name_salt');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.insurance_claims (date_of_birth)
    TRANSFORM generalize
    SCOPE relationship
    PARAMS (range = 10);

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.insurance_claims (policy_holder_id)
    TRANSFORM tokenize
    SCOPE person
    PRIORITY 5;

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.insurance_claims (description)
    TRANSFORM mask
    PARAMS (show = 10);
