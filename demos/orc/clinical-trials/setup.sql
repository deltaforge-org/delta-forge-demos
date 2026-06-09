-- ==========================================================================
-- Demo: ORC Clinical Trials — Patient Outcome Analysis
-- Feature: NULL handling, CASE expressions, string functions on ORC data
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.orc_trials
    COMMENT 'ORC-backed clinical trial tables';

-- --------------------------------------------------------------------------
-- Table: patients — 150 patients with high NULL density
-- --------------------------------------------------------------------------
-- NULLs in: followup_score (~31%), adverse_event (~33%), notes (~20%),
--           bmi (~13%), gender (~19%). Empty strings in notes (~21%).
-- Tests ORC NULL bitmap encoding with varied NULL patterns.
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.orc_trials.patients
USING ORC
LOCATION 'clinical-trials/patients.orc';

DETECT SCHEMA FOR TABLE {{zone_name}}.orc_trials.patients;
