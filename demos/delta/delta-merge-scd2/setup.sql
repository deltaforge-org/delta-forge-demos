-- ============================================================================
-- Delta MERGE — SCD Type 2 (Slowly Changing Dimensions) — Setup Script
-- ============================================================================
-- Creates the target dimension table and source changes table for the SCD2
-- MERGE demo.
--
-- Scenario: Insurance Policy Management
--   An insurance company tracks policy changes over time. When a policyholder
--   upgrades coverage, changes address, or adjusts their premium, the old
--   policy record is expired and a new version is inserted. This preserves
--   the full history for regulatory audits and claims processing.
--
-- Tables:
--   1. policy_dim     — 15 current policies (SCD2 dimension table)
--   2. policy_changes — 8 incoming changes effective 2025-01-15
--
-- The queries.sql will:
--   - MERGE to expire 8 current rows (set valid_to, is_current=0)
--   - INSERT 8 new current versions with updated attributes
--   - Final count: 15 original + 8 new = 23 rows (8 expired + 15 current)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: policy_dim — 15 current policies (SCD2 dimension table)
-- ============================================================================
-- SCD2 structure: surrogate_key uniquely identifies each row version.
-- valid_from/valid_to define the effective date range.
-- is_current=1 marks the active version of each policy.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.policy_dim (
    surrogate_key  INT,
    policy_id      VARCHAR,
    holder_name    VARCHAR,
    coverage_type  VARCHAR,
    annual_premium DOUBLE,
    region         VARCHAR,
    risk_score     INT,
    valid_from     VARCHAR,
    valid_to       VARCHAR,
    is_current     INT
) LOCATION 'delta-merge-scd2/policy_dim';


INSERT INTO {{zone_name}}.delta_demos.policy_dim VALUES
    (1,  'POL-1001', 'Alice Johnson',    'standard',  2400.00, 'northeast', 45, '2024-01-01', '9999-12-31', 1),
    (2,  'POL-1002', 'Bob Martinez',     'basic',     1200.00, 'southeast', 30, '2024-01-01', '9999-12-31', 1),
    (3,  'POL-1003', 'Carol Chen',       'premium',   4800.00, 'west',      65, '2024-01-01', '9999-12-31', 1),
    (4,  'POL-1004', 'David Kim',        'standard',  2600.00, 'midwest',   50, '2024-01-01', '9999-12-31', 1),
    (5,  'POL-1005', 'Elena Rodriguez',  'basic',     1100.00, 'pacific',   25, '2024-01-01', '9999-12-31', 1),
    (6,  'POL-1006', 'Frank O''Brien',   'platinum',  7200.00, 'northeast', 80, '2024-01-01', '9999-12-31', 1),
    (7,  'POL-1007', 'Grace Patel',      'standard',  2200.00, 'southeast', 40, '2024-01-01', '9999-12-31', 1),
    (8,  'POL-1008', 'Henry Nakamura',   'premium',   5100.00, 'west',      70, '2024-01-01', '9999-12-31', 1),
    (9,  'POL-1009', 'Irene Fischer',    'basic',     1300.00, 'midwest',   28, '2024-01-01', '9999-12-31', 1),
    (10, 'POL-1010', 'James Cooper',     'standard',  2500.00, 'pacific',   48, '2024-01-01', '9999-12-31', 1),
    (11, 'POL-1011', 'Karen Liu',        'premium',   4600.00, 'northeast', 62, '2024-01-01', '9999-12-31', 1),
    (12, 'POL-1012', 'Leo Washington',   'basic',     1400.00, 'southeast', 32, '2024-01-01', '9999-12-31', 1),
    (13, 'POL-1013', 'Maria Gonzalez',   'standard',  2300.00, 'west',      42, '2024-01-01', '9999-12-31', 1),
    (14, 'POL-1014', 'Nathan Brooks',    'platinum',  6800.00, 'midwest',   75, '2024-01-01', '9999-12-31', 1),
    (15, 'POL-1015', 'Olivia Thompson',  'premium',   5400.00, 'pacific',   68, '2024-01-01', '9999-12-31', 1);


-- ============================================================================
-- TABLE 2: policy_changes — 8 incoming changes effective 2025-01-15
-- ============================================================================
-- These represent policy modifications arriving in a batch:
--   POL-1001: coverage upgrade standard -> premium
--   POL-1003: coverage upgrade premium -> platinum
--   POL-1005: coverage upgrade basic -> standard
--   POL-1007: region change southeast -> midwest, premium increase
--   POL-1009: premium increase only (no coverage change)
--   POL-1010: coverage upgrade standard -> premium
--   POL-1012: coverage upgrade basic -> standard
--   POL-1014: premium decrease (loyalty discount)
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.policy_changes (
    policy_id      VARCHAR,
    holder_name    VARCHAR,
    coverage_type  VARCHAR,
    annual_premium DOUBLE,
    region         VARCHAR,
    risk_score     INT,
    effective_date VARCHAR
) LOCATION 'delta-merge-scd2/policy_changes';


INSERT INTO {{zone_name}}.delta_demos.policy_changes VALUES
    ('POL-1001', 'Alice Johnson',    'premium',   4200.00, 'northeast', 52, '2025-01-15'),
    ('POL-1003', 'Carol Chen',       'platinum',  7500.00, 'west',      68, '2025-01-15'),
    ('POL-1005', 'Elena Rodriguez',  'standard',  2100.00, 'pacific',   35, '2025-01-15'),
    ('POL-1007', 'Grace Patel',      'standard',  2800.00, 'midwest',   44, '2025-01-15'),
    ('POL-1009', 'Irene Fischer',    'basic',     1500.00, 'midwest',   30, '2025-01-15'),
    ('POL-1010', 'James Cooper',     'premium',   4400.00, 'pacific',   55, '2025-01-15'),
    ('POL-1012', 'Leo Washington',   'standard',  2000.00, 'southeast', 38, '2025-01-15'),
    ('POL-1014', 'Nathan Brooks',    'platinum',  7100.00, 'midwest',   72, '2025-01-15');
