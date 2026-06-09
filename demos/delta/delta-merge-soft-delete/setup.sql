-- ============================================================================
-- Delta MERGE — Soft Delete with BY SOURCE — Setup Script
-- ============================================================================
-- Creates the target and source tables for the soft delete MERGE demo.
--
-- Tables:
--   1. vendors      — 14 vendors (target) with is_active flag
--   2. vendor_feed  — 8 vendors from latest compliance feed (source)
--
-- The MERGE in queries.sql will:
--   - Update 6 vendors (ids 1-6) with refreshed data
--   - Insert 2 new vendors (ids 15-16)
--   - Soft-delete vendors not in feed, with two tiers:
--       * Annual spend >= 50000 → flagged for review (is_active stays 1)
--       * Annual spend <  50000 → deactivated (is_active = 0)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: vendors — 14 active vendors (target)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.vendors (
    id              INT,
    vendor_name     VARCHAR,
    category        VARCHAR,
    annual_spend    DOUBLE,
    is_active       INT,
    status_note     VARCHAR,
    last_verified   VARCHAR
) LOCATION 'delta-merge-soft-delete/vendors';


INSERT INTO {{zone_name}}.delta_demos.vendors VALUES
    (1,  'Alpha Supplies',    'materials',  120000.00, 1, 'verified',        '2025-01-10'),
    (2,  'Beta Components',   'electronics', 85000.00, 1, 'verified',        '2025-01-10'),
    (3,  'Gamma Logistics',   'shipping',    45000.00, 1, 'verified',        '2025-01-10'),
    (4,  'Delta Services',    'consulting',  30000.00, 1, 'verified',        '2025-01-10'),
    (5,  'Epsilon Tech',      'electronics', 95000.00, 1, 'verified',        '2025-01-10'),
    (6,  'Zeta Manufacturing','materials',   200000.00,1, 'verified',        '2025-01-10'),
    (7,  'Eta Packaging',     'materials',    18000.00, 1, 'verified',       '2025-01-10'),
    (8,  'Theta Consulting',  'consulting',   60000.00, 1, 'verified',      '2025-01-10'),
    (9,  'Iota Transport',    'shipping',     22000.00, 1, 'verified',      '2025-01-10'),
    (10, 'Kappa Solutions',   'consulting',   75000.00, 1, 'verified',      '2025-01-10'),
    (11, 'Lambda Cloud',      'electronics', 150000.00, 1, 'verified',      '2025-01-10'),
    (12, 'Mu Freight',        'shipping',     12000.00, 1, 'verified',      '2025-01-10'),
    (13, 'Nu Staffing',       'consulting',    8000.00, 1, 'verified',      '2025-01-10'),
    (14, 'Xi Hardware',       'electronics',  35000.00, 1, 'verified',      '2025-01-10');


-- ============================================================================
-- TABLE 2: vendor_feed — 8 vendors from latest compliance feed (source)
-- ============================================================================
-- Only 6 existing vendors (ids 1-6) appear in this feed.
-- Vendors 7-14 are NOT in the feed → triggers NOT MATCHED BY SOURCE.
-- Two new vendors (ids 15-16) are joining.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.vendor_feed (
    id              INT,
    vendor_name     VARCHAR,
    category        VARCHAR,
    annual_spend    DOUBLE
) LOCATION 'delta-merge-soft-delete/vendor_feed';


INSERT INTO {{zone_name}}.delta_demos.vendor_feed VALUES
    -- Existing vendors with refreshed data
    (1,  'Alpha Supplies',    'materials',  125000.00),
    (2,  'Beta Components',   'electronics', 90000.00),
    (3,  'Gamma Logistics',   'shipping',    48000.00),
    (4,  'Delta Services',    'consulting',  32000.00),
    (5,  'Epsilon Tech',      'electronics', 100000.00),
    (6,  'Zeta Manufacturing','materials',   210000.00),
    -- New vendors
    (15, 'Omicron Digital',   'electronics',  55000.00),
    (16, 'Pi Analytics',      'consulting',   40000.00);
