-- ============================================================================
-- Delta Partition Replacement with DV Cleanup — Setup Script
-- ============================================================================
-- Creates a monthly_settlements table PARTITIONED BY (settlement_month) with
-- 60 baseline rows across 3 months (Jan, Feb, Mar 2024), 20 each.
--
-- Table: monthly_settlements — financial transaction settlements
--
-- January data intentionally contains two duplicate payments (id=19 is a
-- duplicate of id=1, id=20 is a duplicate of id=5) and two incorrect
-- adjustment amounts (id=6 at $1,500.75 instead of $1,475.50; id=14 at
-- $950.50 instead of $1,125.00). These will be corrected via partition
-- replacement in the demo queries.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: monthly_settlements — 60 financial settlement transactions
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.monthly_settlements (
    id                INT,
    account_id        VARCHAR,
    settlement_month  VARCHAR,
    transaction_type  VARCHAR,
    amount            DECIMAL(12,2),
    currency          VARCHAR,
    counterparty      VARCHAR,
    settled_at        VARCHAR
) LOCATION 'delta-partition-replace-dv/monthly_settlements'
PARTITIONED BY (settlement_month);


-- January 2024: ids 1-20 (contains 2 duplicate payments and 2 incorrect adjustments)
INSERT INTO {{zone_name}}.delta_demos.monthly_settlements VALUES
    (1,  'ACC-1001', '2024-01', 'payment',    12500.00, 'USD', 'Meridian Capital LLC',       '2024-01-03 09:15:00'),
    (2,  'ACC-1002', '2024-01', 'payment',    8750.50,  'USD', 'Crossbridge Partners',       '2024-01-04 11:30:00'),
    (3,  'ACC-1003', '2024-01', 'refund',     2100.00,  'EUR', 'Nordic Trade Finance',       '2024-01-05 14:22:00'),
    (4,  'ACC-1001', '2024-01', 'fee',        875.00,   'USD', 'Clearstream Services',       '2024-01-07 08:45:00'),
    (5,  'ACC-1004', '2024-01', 'payment',    34200.00, 'GBP', 'Sterling Settlements Ltd',   '2024-01-08 10:00:00'),
    (6,  'ACC-1002', '2024-01', 'adjustment', 1500.75,  'USD', 'Meridian Capital LLC',       '2024-01-09 13:10:00'),
    (7,  'ACC-1005', '2024-01', 'payment',    19800.00, 'EUR', 'Deutsche Handelsbank AG',    '2024-01-10 09:30:00'),
    (8,  'ACC-1003', '2024-01', 'payment',    6300.25,  'USD', 'Pacific Rim Holdings',       '2024-01-11 15:45:00'),
    (9,  'ACC-1001', '2024-01', 'refund',     3450.00,  'USD', 'Crossbridge Partners',       '2024-01-14 11:00:00'),
    (10, 'ACC-1004', '2024-01', 'payment',    27650.00, 'GBP', 'London Clearing House',      '2024-01-15 08:20:00'),
    (11, 'ACC-1005', '2024-01', 'fee',        1250.00,  'EUR', 'Euroclear Operations',       '2024-01-16 10:15:00'),
    (12, 'ACC-1002', '2024-01', 'payment',    15900.00, 'USD', 'Apex Financial Group',       '2024-01-17 14:30:00'),
    (13, 'ACC-1003', '2024-01', 'payment',    42100.00, 'USD', 'Meridian Capital LLC',       '2024-01-18 09:00:00'),
    (14, 'ACC-1001', '2024-01', 'adjustment', 950.50,   'USD', 'Clearstream Services',       '2024-01-21 11:45:00'),
    (15, 'ACC-1004', '2024-01', 'payment',    8200.00,  'GBP', 'Sterling Settlements Ltd',   '2024-01-22 13:20:00'),
    (16, 'ACC-1005', '2024-01', 'refund',     4800.00,  'EUR', 'Nordic Trade Finance',       '2024-01-23 08:55:00'),
    (17, 'ACC-1002', '2024-01', 'payment',    11350.00, 'USD', 'Pacific Rim Holdings',       '2024-01-24 15:10:00'),
    (18, 'ACC-1003', '2024-01', 'fee',        625.00,   'USD', 'Apex Financial Group',       '2024-01-25 10:30:00'),
    (19, 'ACC-1001', '2024-01', 'payment',    12500.00, 'USD', 'Meridian Capital LLC',       '2024-01-28 09:15:00'),
    (20, 'ACC-1004', '2024-01', 'payment',    34200.00, 'GBP', 'Sterling Settlements Ltd',   '2024-01-29 10:00:00');

-- February 2024: ids 21-40
INSERT INTO {{zone_name}}.delta_demos.monthly_settlements VALUES
    (21, 'ACC-1001', '2024-02', 'payment',    18200.00, 'USD', 'Crossbridge Partners',       '2024-02-01 09:00:00'),
    (22, 'ACC-1002', '2024-02', 'payment',    9400.75,  'USD', 'Meridian Capital LLC',       '2024-02-03 11:15:00'),
    (23, 'ACC-1003', '2024-02', 'refund',     3200.00,  'EUR', 'Nordic Trade Finance',       '2024-02-04 14:00:00'),
    (24, 'ACC-1004', '2024-02', 'payment',    41500.00, 'GBP', 'London Clearing House',      '2024-02-05 08:30:00'),
    (25, 'ACC-1005', '2024-02', 'fee',        1100.00,  'EUR', 'Euroclear Operations',       '2024-02-06 10:45:00'),
    (26, 'ACC-1001', '2024-02', 'payment',    22750.00, 'USD', 'Apex Financial Group',       '2024-02-07 13:30:00'),
    (27, 'ACC-1002', '2024-02', 'adjustment', 2850.50,  'USD', 'Clearstream Services',       '2024-02-10 09:20:00'),
    (28, 'ACC-1003', '2024-02', 'payment',    7600.00,  'USD', 'Pacific Rim Holdings',       '2024-02-11 15:00:00'),
    (29, 'ACC-1004', '2024-02', 'payment',    29300.00, 'GBP', 'Sterling Settlements Ltd',   '2024-02-12 08:15:00'),
    (30, 'ACC-1005', '2024-02', 'refund',     5400.00,  'EUR', 'Deutsche Handelsbank AG',    '2024-02-13 11:40:00'),
    (31, 'ACC-1001', '2024-02', 'payment',    16800.00, 'USD', 'Meridian Capital LLC',       '2024-02-14 14:20:00'),
    (32, 'ACC-1002', '2024-02', 'fee',        750.00,   'USD', 'Clearstream Services',       '2024-02-17 08:50:00'),
    (33, 'ACC-1003', '2024-02', 'payment',    38900.00, 'USD', 'Crossbridge Partners',       '2024-02-18 10:10:00'),
    (34, 'ACC-1004', '2024-02', 'adjustment', 1650.25,  'GBP', 'London Clearing House',      '2024-02-19 13:00:00'),
    (35, 'ACC-1005', '2024-02', 'payment',    13200.00, 'EUR', 'Nordic Trade Finance',       '2024-02-20 09:35:00'),
    (36, 'ACC-1001', '2024-02', 'payment',    8950.00,  'USD', 'Pacific Rim Holdings',       '2024-02-21 15:25:00'),
    (37, 'ACC-1002', '2024-02', 'refund',     4100.00,  'USD', 'Apex Financial Group',       '2024-02-24 11:00:00'),
    (38, 'ACC-1003', '2024-02', 'payment',    25600.00, 'USD', 'Meridian Capital LLC',       '2024-02-25 08:40:00'),
    (39, 'ACC-1004', '2024-02', 'payment',    19750.00, 'GBP', 'Sterling Settlements Ltd',   '2024-02-26 14:50:00'),
    (40, 'ACC-1005', '2024-02', 'payment',    6200.00,  'EUR', 'Deutsche Handelsbank AG',    '2024-02-28 10:05:00');

-- March 2024: ids 41-60
INSERT INTO {{zone_name}}.delta_demos.monthly_settlements VALUES
    (41, 'ACC-1001', '2024-03', 'payment',    21300.00, 'USD', 'Crossbridge Partners',       '2024-03-01 09:10:00'),
    (42, 'ACC-1002', '2024-03', 'payment',    14750.50, 'USD', 'Meridian Capital LLC',       '2024-03-04 11:25:00'),
    (43, 'ACC-1003', '2024-03', 'refund',     1800.00,  'EUR', 'Nordic Trade Finance',       '2024-03-05 14:15:00'),
    (44, 'ACC-1004', '2024-03', 'payment',    36400.00, 'GBP', 'London Clearing House',      '2024-03-06 08:40:00'),
    (45, 'ACC-1005', '2024-03', 'fee',        950.00,   'EUR', 'Euroclear Operations',       '2024-03-07 10:30:00'),
    (46, 'ACC-1001', '2024-03', 'payment',    28100.00, 'USD', 'Apex Financial Group',       '2024-03-10 13:45:00'),
    (47, 'ACC-1002', '2024-03', 'adjustment', 3200.75,  'USD', 'Clearstream Services',       '2024-03-11 09:00:00'),
    (48, 'ACC-1003', '2024-03', 'payment',    9850.00,  'USD', 'Pacific Rim Holdings',       '2024-03-12 15:20:00'),
    (49, 'ACC-1004', '2024-03', 'payment',    45000.00, 'GBP', 'Sterling Settlements Ltd',   '2024-03-13 08:05:00'),
    (50, 'ACC-1005', '2024-03', 'refund',     6700.00,  'EUR', 'Deutsche Handelsbank AG',    '2024-03-14 11:50:00'),
    (51, 'ACC-1001', '2024-03', 'payment',    17500.00, 'USD', 'Meridian Capital LLC',       '2024-03-17 14:35:00'),
    (52, 'ACC-1002', '2024-03', 'fee',        1025.00,  'USD', 'Clearstream Services',       '2024-03-18 08:25:00'),
    (53, 'ACC-1003', '2024-03', 'payment',    33250.00, 'USD', 'Crossbridge Partners',       '2024-03-19 10:40:00'),
    (54, 'ACC-1004', '2024-03', 'adjustment', 2100.50,  'GBP', 'London Clearing House',      '2024-03-20 13:15:00'),
    (55, 'ACC-1005', '2024-03', 'payment',    11600.00, 'EUR', 'Nordic Trade Finance',       '2024-03-21 09:50:00'),
    (56, 'ACC-1001', '2024-03', 'payment',    7800.00,  'USD', 'Pacific Rim Holdings',       '2024-03-24 15:05:00'),
    (57, 'ACC-1002', '2024-03', 'refund',     3500.00,  'USD', 'Apex Financial Group',       '2024-03-25 11:20:00'),
    (58, 'ACC-1003', '2024-03', 'payment',    19200.00, 'USD', 'Meridian Capital LLC',       '2024-03-26 08:35:00'),
    (59, 'ACC-1004', '2024-03', 'payment',    26850.00, 'GBP', 'Sterling Settlements Ltd',   '2024-03-27 14:00:00'),
    (60, 'ACC-1005', '2024-03', 'payment',    5100.00,  'EUR', 'Deutsche Handelsbank AG',    '2024-03-28 10:15:00');
