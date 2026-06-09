-- ============================================================================
-- Delta VACUUM Retention Governance — Compliance-Driven Data Lifecycle — Setup
-- ============================================================================
-- Creates a trade settlement records table and inserts 35 pending
-- settlements across 5 counterparties and 5 instrument types. All status
-- transitions, amount adjustments, and deletions happen in queries.sql
-- so you can observe the retention impact at each stage.
--
-- Tables created:
--   1. settlement_records — 35 initial rows, all status='pending'
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: settlement_records — Trade settlement tracking
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.settlement_records (
    id                INT,
    trade_ref         VARCHAR,
    counterparty      VARCHAR,
    instrument        VARCHAR,
    amount            DOUBLE,
    settlement_date   VARCHAR,
    status            VARCHAR,
    trader            VARCHAR
) LOCATION 'delta-vacuum-retention-governance/settlement_records';


-- V1: Insert 35 pending settlements across 5 counterparties
INSERT INTO {{zone_name}}.delta_demos.settlement_records VALUES
    -- Apex Capital (ids 1-7)
    (1,  'TXN-10001', 'Apex Capital',      'US Treasury Bond',     2500000.00, '2025-03-10', 'pending', 'jsmith'),
    (2,  'TXN-10002', 'Apex Capital',      'Corporate Bond',       1750000.00, '2025-03-11', 'pending', 'agarcia'),
    (3,  'TXN-10003', 'Apex Capital',      'Equity Swap',           980000.00, '2025-03-12', 'pending', 'wnguyen'),
    (4,  'TXN-10004', 'Apex Capital',      'FX Forward',            450000.00, '2025-03-13', 'pending', 'kpatel'),
    (5,  'TXN-10005', 'Apex Capital',      'Interest Rate Swap',   3200000.00, '2025-03-14', 'pending', 'lchen'),
    (6,  'TXN-10006', 'Apex Capital',      'Corporate Bond',        620000.00, '2025-03-15', 'pending', 'jsmith'),
    (7,  'TXN-10007', 'Apex Capital',      'US Treasury Bond',     1100000.00, '2025-03-16', 'pending', 'agarcia'),
    -- Meridian Trust (ids 8-14)
    (8,  'TXN-10008', 'Meridian Trust',    'Equity Swap',          1850000.00, '2025-03-10', 'pending', 'wnguyen'),
    (9,  'TXN-10009', 'Meridian Trust',    'FX Forward',            720000.00, '2025-03-11', 'pending', 'kpatel'),
    (10, 'TXN-10010', 'Meridian Trust',    'Interest Rate Swap',   2900000.00, '2025-03-12', 'pending', 'lchen'),
    (11, 'TXN-10011', 'Meridian Trust',    'US Treasury Bond',     4100000.00, '2025-03-13', 'pending', 'jsmith'),
    (12, 'TXN-10012', 'Meridian Trust',    'Corporate Bond',        550000.00, '2025-03-14', 'pending', 'agarcia'),
    (13, 'TXN-10013', 'Meridian Trust',    'Equity Swap',          1200000.00, '2025-03-15', 'pending', 'wnguyen'),
    (14, 'TXN-10014', 'Meridian Trust',    'FX Forward',            380000.00, '2025-03-16', 'pending', 'kpatel'),
    -- Pacific Holdings (ids 15-21)
    (15, 'TXN-10015', 'Pacific Holdings',  'Interest Rate Swap',   5000000.00, '2025-03-10', 'pending', 'lchen'),
    (16, 'TXN-10016', 'Pacific Holdings',  'US Treasury Bond',     3300000.00, '2025-03-11', 'pending', 'jsmith'),
    (17, 'TXN-10017', 'Pacific Holdings',  'Corporate Bond',        890000.00, '2025-03-12', 'pending', 'agarcia'),
    (18, 'TXN-10018', 'Pacific Holdings',  'Equity Swap',          2100000.00, '2025-03-13', 'pending', 'wnguyen'),
    (19, 'TXN-10019', 'Pacific Holdings',  'FX Forward',            670000.00, '2025-03-14', 'pending', 'kpatel'),
    (20, 'TXN-10020', 'Pacific Holdings',  'Interest Rate Swap',   1500000.00, '2025-03-15', 'pending', 'lchen'),
    (21, 'TXN-10021', 'Pacific Holdings',  'US Treasury Bond',      950000.00, '2025-03-16', 'pending', 'jsmith'),
    -- Sterling & Co (ids 22-28)
    (22, 'TXN-10022', 'Sterling & Co',     'Corporate Bond',       1650000.00, '2025-03-10', 'pending', 'agarcia'),
    (23, 'TXN-10023', 'Sterling & Co',     'Equity Swap',           430000.00, '2025-03-11', 'pending', 'wnguyen'),
    (24, 'TXN-10024', 'Sterling & Co',     'FX Forward',           2800000.00, '2025-03-12', 'pending', 'kpatel'),
    (25, 'TXN-10025', 'Sterling & Co',     'Interest Rate Swap',   1900000.00, '2025-03-13', 'pending', 'lchen'),
    (26, 'TXN-10026', 'Sterling & Co',     'US Treasury Bond',      780000.00, '2025-03-14', 'pending', 'jsmith'),
    (27, 'TXN-10027', 'Sterling & Co',     'Corporate Bond',       3600000.00, '2025-03-15', 'pending', 'agarcia'),
    (28, 'TXN-10028', 'Sterling & Co',     'Equity Swap',           510000.00, '2025-03-16', 'pending', 'wnguyen'),
    -- Vanguard Partners (ids 29-35)
    (29, 'TXN-10029', 'Vanguard Partners', 'FX Forward',           1350000.00, '2025-03-10', 'pending', 'kpatel'),
    (30, 'TXN-10030', 'Vanguard Partners', 'Interest Rate Swap',   4500000.00, '2025-03-11', 'pending', 'lchen'),
    (31, 'TXN-10031', 'Vanguard Partners', 'US Treasury Bond',     2200000.00, '2025-03-12', 'pending', 'jsmith'),
    (32, 'TXN-10032', 'Vanguard Partners', 'Corporate Bond',        960000.00, '2025-03-13', 'pending', 'agarcia'),
    (33, 'TXN-10033', 'Vanguard Partners', 'Equity Swap',          1800000.00, '2025-03-14', 'pending', 'wnguyen'),
    (34, 'TXN-10034', 'Vanguard Partners', 'FX Forward',            290000.00, '2025-03-15', 'pending', 'kpatel'),
    (35, 'TXN-10035', 'Vanguard Partners', 'Interest Rate Swap',   3100000.00, '2025-03-16', 'pending', 'lchen');
