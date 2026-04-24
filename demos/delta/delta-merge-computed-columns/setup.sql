-- ============================================================================
-- Delta MERGE — Computed Columns & CASE Logic — Setup Script
-- ============================================================================
-- Creates tables for the computed columns MERGE demo.
--
-- Tables:
--   1. subscriptions         — 12 existing subscriptions (target)
--   2. subscription_changes  — 10 changes (source): 7 renewals + 3 new
--
-- The MERGE in queries.sql will compute derived columns at merge time:
--   - tier: based on monthly_amount thresholds (CASE)
--   - discount_pct: based on months_active loyalty brackets (CASE)
--   - priority_score: arithmetic formula (amount * tenure weight)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: subscriptions — 12 existing subscriptions (target)
-- ============================================================================
-- The tier, discount_pct, and priority_score columns are derived —
-- they are computed by the MERGE, not stored independently.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.subscriptions (
    id              INT,
    customer_name   VARCHAR,
    plan            VARCHAR,
    monthly_amount  DOUBLE,
    months_active   INT,
    tier            VARCHAR,
    discount_pct    DOUBLE,
    priority_score  DOUBLE
) LOCATION 'subscriptions';


INSERT INTO {{zone_name}}.delta_demos.subscriptions VALUES
    (1,  'Acme Corp',       'business',   150.00, 24, 'gold',     10.0, 360.0),
    (2,  'Bolt Industries', 'starter',     29.00,  3, 'bronze',    0.0,  29.0),
    (3,  'Cascade Labs',    'enterprise', 500.00, 36, 'platinum', 15.0, 1800.0),
    (4,  'DataFlow Inc',    'business',   150.00, 11, 'gold',      5.0, 165.0),
    (5,  'Echo Systems',    'starter',     29.00,  8, 'bronze',    0.0,  29.0),
    (6,  'Forge Analytics', 'professional', 89.00, 18, 'silver',  10.0, 160.2),
    (7,  'GridPoint',       'enterprise', 500.00,  5, 'platinum',  0.0, 500.0),
    (8,  'HyperNet',        'professional', 89.00, 30, 'silver',  15.0, 267.0),
    (9,  'InnoTech',        'starter',     29.00, 14, 'bronze',    5.0,  40.6),
    (10, 'JetStream',       'business',   150.00,  2, 'gold',      0.0, 150.0),
    (11, 'KineticAI',       'professional', 89.00,  1, 'silver',   0.0,  89.0),
    (12, 'LogicWave',       'starter',     29.00, 20, 'bronze',   10.0,  58.0);


-- ============================================================================
-- TABLE 2: subscription_changes — 10 changes (source)
-- ============================================================================
-- IDs 1-5,7,9: renewals with plan upgrades or extended tenure
-- IDs 13-15: brand new subscriptions
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.subscription_changes (
    id              INT,
    customer_name   VARCHAR,
    plan            VARCHAR,
    monthly_amount  DOUBLE,
    months_active   INT
) LOCATION 'subscription_changes';


INSERT INTO {{zone_name}}.delta_demos.subscription_changes VALUES
    -- Renewals (plan upgrades and extended tenure)
    (1,  'Acme Corp',       'enterprise', 500.00, 25),
    (2,  'Bolt Industries', 'professional', 89.00,  4),
    (3,  'Cascade Labs',    'enterprise', 500.00, 37),
    (4,  'DataFlow Inc',    'enterprise', 500.00, 12),
    (5,  'Echo Systems',    'business',   150.00,  9),
    (7,  'GridPoint',       'enterprise', 500.00,  6),
    (9,  'InnoTech',        'business',   150.00, 15),
    -- New subscriptions
    (13, 'NovaStar',        'starter',     29.00,  1),
    (14, 'OmniFlow',        'business',   150.00,  1),
    (15, 'PrismData',       'enterprise', 500.00,  1);
