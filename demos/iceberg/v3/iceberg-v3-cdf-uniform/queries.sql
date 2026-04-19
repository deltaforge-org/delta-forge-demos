-- ============================================================================
-- Iceberg V3 UniForm — CDF Payment Reconciliation — Queries
-- ============================================================================
-- Tests that CDF and UniForm V3 coexist correctly. Every mutation generates
-- both CDF change records and Iceberg V3 metadata. After all mutations,
-- an Iceberg external table read-back proves the V3 shadow metadata chain
-- stayed consistent throughout.
--
-- Mutation timeline:
--   V1: Approve 8 pending payments  -> completed
--   V2: Decline 3 pending payments  -> failed
--   V3: Remove 2 fraudulent records -> DELETE
--   V4: Insert 5 new payments       -> pending
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — 30 Transactions
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE cnt = 30
ASSERT VALUE total_amount = 12373.23
SELECT
    COUNT(*) AS cnt,
    ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.iceberg_demos.payment_transactions;


-- ============================================================================
-- Query 2: Baseline Status Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 20 WHERE status = 'pending'
ASSERT VALUE cnt = 8 WHERE status = 'completed'
ASSERT VALUE cnt = 2 WHERE status = 'failed'
SELECT
    status,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.payment_transactions
GROUP BY status
ORDER BY status;


-- ============================================================================
-- V1: Approve 8 Pending Payments
-- ============================================================================
-- CDF records update_preimage (status='pending') and update_postimage
-- (status='completed') for each of these 8 rows. UniForm V3 generates
-- a new Iceberg snapshot reflecting the state change.

ASSERT ROW_COUNT = 8
UPDATE {{zone_name}}.iceberg_demos.payment_transactions
SET status = 'completed'
WHERE payment_id IN (1, 3, 7, 9, 14, 19, 25, 27);

-- Verify: 16 completed
ASSERT VALUE cnt = 16
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.payment_transactions
WHERE status = 'completed';


-- ============================================================================
-- V2: Decline 3 Pending Payments
-- ============================================================================
-- Cards declined — CDF captures pre/post images; V3 metadata updated.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.iceberg_demos.payment_transactions
SET status = 'failed'
WHERE payment_id IN (6, 22, 28);

-- Verify: 5 failed
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.payment_transactions
WHERE status = 'failed';


-- ============================================================================
-- V3: Remove 2 Fraudulent Transactions
-- ============================================================================
-- IDs 18 and 24 (both previously 'failed') are flagged as fraud and
-- permanently removed. CDF records delete entries; V3 metadata updated.

ASSERT ROW_COUNT = 2
DELETE FROM {{zone_name}}.iceberg_demos.payment_transactions
WHERE payment_id IN (18, 24);

-- Verify: deleted rows are gone
ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.iceberg_demos.payment_transactions
WHERE payment_id IN (18, 24);

-- Verify: 28 rows remain, 3 failed
ASSERT VALUE cnt = 28
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.payment_transactions;


-- ============================================================================
-- V4: Insert 5 New Payments
-- ============================================================================
-- Fresh transactions. CDF records _change_type='insert'; V3 metadata adds
-- a new snapshot with the appended data.

INSERT INTO {{zone_name}}.iceberg_demos.payment_transactions VALUES
    (31, 'TechGadgets Inc',     'wei.zhang@email.com',      4999.99, 'USD', 'pending', 'bank_transfer',   '2024-06-04'),
    (32, 'CloudSoft SaaS',      'hannah.fischer@email.com', 1299.00, 'EUR', 'pending', 'bank_transfer',   '2024-06-04'),
    (33, 'UrbanStyle Apparel',  'tom.clark@email.com',       450.00, 'GBP', 'pending', 'credit_card',     '2024-06-04'),
    (34, 'FreshMart Foods',     'ada.okonkwo@email.com',     187.60, 'USD', 'pending', 'debit_card',      '2024-06-04'),
    (35, 'MedPlus Pharmacy',    'ivan.petrov@email.com',     325.00, 'USD', 'pending', 'credit_card',     '2024-06-04');

-- Verify: 33 rows total
ASSERT VALUE cnt = 33
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.payment_transactions;


-- ============================================================================
-- Query 3: Final Status Distribution
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 16 WHERE status = 'completed'
ASSERT VALUE cnt = 14 WHERE status = 'pending'
ASSERT VALUE cnt = 3 WHERE status = 'failed'
ASSERT VALUE total_amount = 8955.80 WHERE status = 'completed'
ASSERT VALUE total_amount = 10066.79 WHERE status = 'pending'
ASSERT VALUE total_amount = 113.24 WHERE status = 'failed'
SELECT
    status,
    COUNT(*) AS cnt,
    ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.iceberg_demos.payment_transactions
GROUP BY status
ORDER BY status;


-- ============================================================================
-- Query 4: Final Per-Merchant Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE cnt = 6 WHERE merchant = 'CloudSoft SaaS'
ASSERT VALUE cnt = 7 WHERE merchant = 'FreshMart Foods'
ASSERT VALUE cnt = 7 WHERE merchant = 'MedPlus Pharmacy'
ASSERT VALUE cnt = 7 WHERE merchant = 'TechGadgets Inc'
ASSERT VALUE cnt = 6 WHERE merchant = 'UrbanStyle Apparel'
ASSERT VALUE total_amount = 6294.00 WHERE merchant = 'CloudSoft SaaS'
ASSERT VALUE total_amount = 881.09 WHERE merchant = 'FreshMart Foods'
ASSERT VALUE total_amount = 941.05 WHERE merchant = 'MedPlus Pharmacy'
ASSERT VALUE total_amount = 9673.45 WHERE merchant = 'TechGadgets Inc'
ASSERT VALUE total_amount = 1346.24 WHERE merchant = 'UrbanStyle Apparel'
SELECT
    merchant,
    COUNT(*) AS cnt,
    ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.iceberg_demos.payment_transactions
GROUP BY merchant
ORDER BY merchant;


-- ============================================================================
-- Iceberg V3 Read-Back — Cross-Engine Verification
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query through the Iceberg V3 metadata chain. This proves the UniForm
-- V3 shadow metadata is readable by an Iceberg engine even after CDF
-- was generating change records alongside.

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.payment_transactions_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.payment_transactions_iceberg
USING ICEBERG
LOCATION '{{data_subdir}}/payment_transactions';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.payment_transactions_iceberg TO USER {{current_user}};


-- ============================================================================
-- Iceberg Verify 1: Row Count Matches Delta
-- ============================================================================

ASSERT ROW_COUNT = 33
SELECT * FROM {{zone_name}}.iceberg_demos.payment_transactions_iceberg
ORDER BY payment_id;


-- ============================================================================
-- Iceberg Verify 2: Status Distribution Matches Delta
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 16 WHERE status = 'completed'
ASSERT VALUE cnt = 14 WHERE status = 'pending'
ASSERT VALUE cnt = 3 WHERE status = 'failed'
SELECT
    status,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_demos.payment_transactions_iceberg
GROUP BY status
ORDER BY status;


-- ============================================================================
-- Iceberg Verify 3: Grand Totals Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 33
ASSERT VALUE total_amount = 19135.83
SELECT
    COUNT(*) AS total_rows,
    ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.iceberg_demos.payment_transactions_iceberg;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check on the Delta table — final state after
-- all 4 mutation rounds with CDF + UniForm V3 active.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 33
ASSERT VALUE total_amount = 19135.83
ASSERT VALUE completed_count = 16
ASSERT VALUE pending_count = 14
ASSERT VALUE failed_count = 3
ASSERT VALUE merchant_count = 5
SELECT
    COUNT(*) AS total_rows,
    ROUND(SUM(amount), 2) AS total_amount,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_count,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    COUNT(DISTINCT merchant) AS merchant_count
FROM {{zone_name}}.iceberg_demos.payment_transactions;
