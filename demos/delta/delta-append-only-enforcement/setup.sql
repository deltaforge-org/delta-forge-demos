-- ============================================================================
-- Delta Append-Only Enforcement — Setup Script
-- ============================================================================
-- Creates two tables for a financial compliance scenario:
--   1. compliance_ledger — append-only (delta.appendOnly = true)
--   2. mutable_ledger    — normal table (no append-only) as a control
--
-- Both start with the same 20 financial transactions. The queries.sql
-- file then tests the enforcement boundary: mutations succeed on the
-- mutable table but are rejected on the append-only ledger.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: compliance_ledger — Append-only for regulatory compliance
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.compliance_ledger (
    txn_id          INT,
    account_id      VARCHAR,
    txn_type        VARCHAR,
    amount          DOUBLE,
    currency        VARCHAR,
    counterparty    VARCHAR,
    txn_date        VARCHAR,
    reference       VARCHAR
) LOCATION 'delta-append-only-enforcement/compliance_ledger'
TBLPROPERTIES (
    'delta.appendOnly' = 'true'
);


-- 20 financial transactions across 5 accounts
INSERT INTO {{zone_name}}.delta_demos.compliance_ledger VALUES
    (1,  'ACC-1001', 'DEPOSIT',    5000.00,  'USD', 'Wire Transfer',       '2025-01-02', 'REF-20250102-001'),
    (2,  'ACC-1001', 'WITHDRAWAL', -1200.00, 'USD', 'ACH Payment',         '2025-01-03', 'REF-20250103-001'),
    (3,  'ACC-1002', 'DEPOSIT',    8500.00,  'USD', 'Wire Transfer',       '2025-01-03', 'REF-20250103-002'),
    (4,  'ACC-1002', 'TRANSFER',   -2000.00, 'USD', 'Internal Transfer',   '2025-01-04', 'REF-20250104-001'),
    (5,  'ACC-1003', 'DEPOSIT',    12000.00, 'USD', 'Client Payment',      '2025-01-05', 'REF-20250105-001'),
    (6,  'ACC-1001', 'DEPOSIT',    3200.00,  'USD', 'Client Payment',      '2025-01-06', 'REF-20250106-001'),
    (7,  'ACC-1003', 'WITHDRAWAL', -4500.00, 'USD', 'Vendor Payment',      '2025-01-07', 'REF-20250107-001'),
    (8,  'ACC-1004', 'DEPOSIT',    15000.00, 'USD', 'Wire Transfer',       '2025-01-08', 'REF-20250108-001'),
    (9,  'ACC-1004', 'WITHDRAWAL', -3000.00, 'USD', 'Payroll',             '2025-01-09', 'REF-20250109-001'),
    (10, 'ACC-1005', 'DEPOSIT',    6800.00,  'USD', 'Client Payment',      '2025-01-10', 'REF-20250110-001'),
    (11, 'ACC-1002', 'DEPOSIT',    4100.00,  'USD', 'Client Payment',      '2025-01-11', 'REF-20250111-001'),
    (12, 'ACC-1005', 'WITHDRAWAL', -2500.00, 'USD', 'Office Lease',        '2025-01-12', 'REF-20250112-001'),
    (13, 'ACC-1003', 'DEPOSIT',    9200.00,  'USD', 'Wire Transfer',       '2025-01-13', 'REF-20250113-001'),
    (14, 'ACC-1001', 'TRANSFER',   -1500.00, 'USD', 'Internal Transfer',   '2025-01-14', 'REF-20250114-001'),
    (15, 'ACC-1004', 'DEPOSIT',    7600.00,  'USD', 'Client Payment',      '2025-01-15', 'REF-20250115-001'),
    (16, 'ACC-1005', 'TRANSFER',   -1000.00, 'USD', 'Internal Transfer',   '2025-01-16', 'REF-20250116-001'),
    (17, 'ACC-1002', 'WITHDRAWAL', -3500.00, 'USD', 'Equipment Purchase',  '2025-01-17', 'REF-20250117-001'),
    (18, 'ACC-1003', 'WITHDRAWAL', -2800.00, 'USD', 'Tax Payment',         '2025-01-18', 'REF-20250118-001'),
    (19, 'ACC-1004', 'TRANSFER',   -5000.00, 'USD', 'Internal Transfer',   '2025-01-19', 'REF-20250119-001'),
    (20, 'ACC-1001', 'DEPOSIT',    4400.00,  'USD', 'Client Payment',      '2025-01-20', 'REF-20250120-001');


-- ============================================================================
-- TABLE 2: mutable_ledger — Normal table (control group, no append-only)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.mutable_ledger (
    txn_id          INT,
    account_id      VARCHAR,
    txn_type        VARCHAR,
    amount          DOUBLE,
    currency        VARCHAR,
    counterparty    VARCHAR,
    txn_date        VARCHAR,
    reference       VARCHAR
) LOCATION 'delta-append-only-enforcement/mutable_ledger';


-- Same 20 transactions for comparison
INSERT INTO {{zone_name}}.delta_demos.mutable_ledger VALUES
    (1,  'ACC-1001', 'DEPOSIT',    5000.00,  'USD', 'Wire Transfer',       '2025-01-02', 'REF-20250102-001'),
    (2,  'ACC-1001', 'WITHDRAWAL', -1200.00, 'USD', 'ACH Payment',         '2025-01-03', 'REF-20250103-001'),
    (3,  'ACC-1002', 'DEPOSIT',    8500.00,  'USD', 'Wire Transfer',       '2025-01-03', 'REF-20250103-002'),
    (4,  'ACC-1002', 'TRANSFER',   -2000.00, 'USD', 'Internal Transfer',   '2025-01-04', 'REF-20250104-001'),
    (5,  'ACC-1003', 'DEPOSIT',    12000.00, 'USD', 'Client Payment',      '2025-01-05', 'REF-20250105-001'),
    (6,  'ACC-1001', 'DEPOSIT',    3200.00,  'USD', 'Client Payment',      '2025-01-06', 'REF-20250106-001'),
    (7,  'ACC-1003', 'WITHDRAWAL', -4500.00, 'USD', 'Vendor Payment',      '2025-01-07', 'REF-20250107-001'),
    (8,  'ACC-1004', 'DEPOSIT',    15000.00, 'USD', 'Wire Transfer',       '2025-01-08', 'REF-20250108-001'),
    (9,  'ACC-1004', 'WITHDRAWAL', -3000.00, 'USD', 'Payroll',             '2025-01-09', 'REF-20250109-001'),
    (10, 'ACC-1005', 'DEPOSIT',    6800.00,  'USD', 'Client Payment',      '2025-01-10', 'REF-20250110-001'),
    (11, 'ACC-1002', 'DEPOSIT',    4100.00,  'USD', 'Client Payment',      '2025-01-11', 'REF-20250111-001'),
    (12, 'ACC-1005', 'WITHDRAWAL', -2500.00, 'USD', 'Office Lease',        '2025-01-12', 'REF-20250112-001'),
    (13, 'ACC-1003', 'DEPOSIT',    9200.00,  'USD', 'Wire Transfer',       '2025-01-13', 'REF-20250113-001'),
    (14, 'ACC-1001', 'TRANSFER',   -1500.00, 'USD', 'Internal Transfer',   '2025-01-14', 'REF-20250114-001'),
    (15, 'ACC-1004', 'DEPOSIT',    7600.00,  'USD', 'Client Payment',      '2025-01-15', 'REF-20250115-001'),
    (16, 'ACC-1005', 'TRANSFER',   -1000.00, 'USD', 'Internal Transfer',   '2025-01-16', 'REF-20250116-001'),
    (17, 'ACC-1002', 'WITHDRAWAL', -3500.00, 'USD', 'Equipment Purchase',  '2025-01-17', 'REF-20250117-001'),
    (18, 'ACC-1003', 'WITHDRAWAL', -2800.00, 'USD', 'Tax Payment',         '2025-01-18', 'REF-20250118-001'),
    (19, 'ACC-1004', 'TRANSFER',   -5000.00, 'USD', 'Internal Transfer',   '2025-01-19', 'REF-20250119-001'),
    (20, 'ACC-1001', 'DEPOSIT',    4400.00,  'USD', 'Client Payment',      '2025-01-20', 'REF-20250120-001');
