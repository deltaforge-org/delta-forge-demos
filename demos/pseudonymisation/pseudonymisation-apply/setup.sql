-- ============================================================================
-- Pseudonymisation Apply — Clinical Trial De-identification — Setup Script
-- ============================================================================
-- Creates a clinical trial participants table with sample data and applies
-- query-time pseudonymisation rules for PII protection. This simulates a
-- pharmaceutical company preparing trial data for external researcher access.
--
-- Table created:
--   trial_participants — 6 clinical trial subjects across 2 trials
--
-- Pseudonymisation rules (query-time):
--   1. ssn              → redact (full replacement with mask)
--   2. participant_name  → keyed_hash (deterministic pseudonym for linkage)
--   3. email            → mask (partial visibility, first 3 chars)
--
-- Compliance context:
--   GDPR Article 17  — Right to erasure for withdrawn participants
--   ICH GCP E6(R2)   — Good Clinical Practice data integrity requirements
--   21 CFR Part 11    — Electronic records / electronic signatures
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
    COMMENT 'Pseudonymisation demo — clinical trial data with protection rules';


-- ============================================================================
-- TABLE: trial_participants — Clinical Trial Subjects
-- ============================================================================
-- 6 participants across 2 trials (TR-2024-A Phase III, TR-2024-B Phase II).
-- Statuses: Active (4), Completed (1), Withdrawn (1).
-- Treatment arms: Drug A (2), Drug B (2), Placebo (2).
-- ============================================================================

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.pseudonymisation_demos.trial_participants (
    subject_id      VARCHAR,
    trial_id        VARCHAR,
    trial_phase     VARCHAR,
    participant_name VARCHAR,
    date_of_birth   VARCHAR,
    email           VARCHAR,
    ssn             VARCHAR,
    status          VARCHAR,
    treatment_arm   VARCHAR,
    efficacy_score  DOUBLE,
    outcome         VARCHAR
) LOCATION 'pseudonymisation-apply/trial_participants';


DELETE FROM {{zone_name}}.pseudonymisation_demos.trial_participants WHERE 1=1;

INSERT INTO {{zone_name}}.pseudonymisation_demos.trial_participants VALUES
    ('SUBJ-001', 'TR-2024-A', 'Phase III', 'Emily Watson', '1982-05-20', 'emily.w@trial.org', '555-11-2233', 'Active', 'Drug A', 0.85, 'Partial Response'),
    ('SUBJ-002', 'TR-2024-A', 'Phase III', 'Robert Chen', '1975-11-08', 'robert.c@trial.org', '555-22-3344', 'Active', 'Placebo', 0.42, 'Stable Disease'),
    ('SUBJ-003', 'TR-2024-A', 'Phase III', 'Ana Silva', '1990-03-14', 'ana.s@trial.org', '555-33-4455', 'Withdrawn', 'Drug A', 0.91, 'Complete Response'),
    ('SUBJ-004', 'TR-2024-B', 'Phase II', 'Thomas Berg', '1988-09-02', 'thomas.b@trial.org', '555-44-5566', 'Active', 'Drug B', 0.67, 'Partial Response'),
    ('SUBJ-005', 'TR-2024-B', 'Phase II', 'Priya Patel', '1995-07-17', 'priya.p@trial.org', '555-55-6677', 'Completed', 'Drug B', 0.78, 'Complete Response'),
    ('SUBJ-006', 'TR-2024-B', 'Phase II', 'Lars Eriksen', '1970-01-30', 'lars.e@trial.org', '555-66-7788', 'Active', 'Placebo', 0.31, 'Progressive Disease');


-- ============================================================================
-- STEP 2: Pseudonymisation Rules (Query-Time)
-- ============================================================================
-- These rules transform PII at query time without modifying stored data.
-- Three transform types cover the most common clinical trial de-identification
-- patterns: redact SSN, hash participant names for linkage, mask emails.

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.trial_participants (ssn)
    TRANSFORM redact
    PARAMS (mask = '***-**-****');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.trial_participants (participant_name)
    TRANSFORM keyed_hash
    SCOPE person
    PARAMS (salt = 'trial_name_salt_2024');

CREATE PSEUDONYMISATION RULE ON {{zone_name}}.pseudonymisation_demos.trial_participants (email)
    TRANSFORM mask
    PARAMS (show = 3);
