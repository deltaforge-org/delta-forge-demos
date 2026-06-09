-- ============================================================================
-- Delta Feature Activation Lifecycle — Setup Script
-- ============================================================================
-- Creates a single Delta table with NO extra features, simulating a plain
-- table created early in a project's life. The queries.sql file then
-- progressively activates features (CDC, constraints) and observes how
-- each activation changes the table's protocol and behavior.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: customer_events — Plain Delta table (no extra features yet)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customer_events (
    id              INT,
    customer_id     VARCHAR,
    event_type      VARCHAR,
    channel         VARCHAR,
    revenue         DOUBLE,
    event_date      VARCHAR
) LOCATION 'delta-feature-activation-lifecycle/customer_events';


-- 25 customer interaction events across 8 customers and 5 channels
INSERT INTO {{zone_name}}.delta_demos.customer_events VALUES
    (1,  'C-100', 'purchase',   'web',     120.00, '2025-01-01'),
    (2,  'C-101', 'purchase',   'mobile',  85.50,  '2025-01-01'),
    (3,  'C-102', 'signup',     'web',     0.00,   '2025-01-02'),
    (4,  'C-100', 'purchase',   'web',     210.00, '2025-01-03'),
    (5,  'C-103', 'purchase',   'store',   340.00, '2025-01-03'),
    (6,  'C-104', 'signup',     'mobile',  0.00,   '2025-01-04'),
    (7,  'C-101', 'refund',     'web',     -45.00, '2025-01-05'),
    (8,  'C-105', 'purchase',   'store',   175.00, '2025-01-05'),
    (9,  'C-102', 'purchase',   'web',     95.00,  '2025-01-06'),
    (10, 'C-106', 'signup',     'email',   0.00,   '2025-01-07'),
    (11, 'C-103', 'purchase',   'mobile',  420.00, '2025-01-08'),
    (12, 'C-100', 'refund',     'web',     -60.00, '2025-01-09'),
    (13, 'C-107', 'signup',     'partner', 0.00,   '2025-01-10'),
    (14, 'C-104', 'purchase',   'mobile',  155.00, '2025-01-11'),
    (15, 'C-105', 'purchase',   'store',   290.00, '2025-01-12'),
    (16, 'C-101', 'purchase',   'web',     130.00, '2025-01-13'),
    (17, 'C-106', 'purchase',   'email',   78.00,  '2025-01-14'),
    (18, 'C-102', 'refund',     'web',     -30.00, '2025-01-15'),
    (19, 'C-107', 'purchase',   'partner', 560.00, '2025-01-16'),
    (20, 'C-103', 'purchase',   'store',   195.00, '2025-01-17'),
    (21, 'C-100', 'purchase',   'mobile',  88.00,  '2025-01-18'),
    (22, 'C-104', 'purchase',   'web',     245.00, '2025-01-19'),
    (23, 'C-105', 'refund',     'store',   -75.00, '2025-01-20'),
    (24, 'C-106', 'purchase',   'mobile',  310.00, '2025-01-21'),
    (25, 'C-107', 'purchase',   'partner', 440.00, '2025-01-22');
