-- ============================================================================
-- Delta Partition Replacement with DV Cleanup — Educational Queries
-- ============================================================================
-- WHAT: The partition-replace pattern deletes an entire partition and inserts
--       corrected data. The DELETE creates deletion vectors (DVs) for every
--       row in the partition, and OPTIMIZE later cleans them up by physically
--       removing the marked rows and compacting the data files.
-- WHY:  Financial settlement pipelines frequently receive late corrections —
--       duplicate invoice reversals, recalculated adjustments, or audit-
--       driven restatements. Replacing the affected month's partition is
--       surgical: other months are never read or modified.
-- HOW:  DELETE WHERE settlement_month = '2024-01' marks all 20 January rows
--       as deleted via DVs. INSERT loads 18 corrected rows (2 duplicates
--       removed, 2 amounts fixed). OPTIMIZE then merges the DVs into
--       compacted files, eliminating the sidecar .bin files entirely.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — Three Months of Settlement Data
-- ============================================================================
-- The monthly_settlements table is partitioned by settlement_month. Each
-- partition's Parquet files live in their own directory. Starting state:
-- 20 settlements per month, 60 total across Jan/Feb/Mar 2024.

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 20 WHERE settlement_month = '2024-01'
ASSERT VALUE txn_count = 20 WHERE settlement_month = '2024-02'
ASSERT VALUE txn_count = 20 WHERE settlement_month = '2024-03'
ASSERT VALUE total_amount = 249002.00 WHERE settlement_month = '2024-01'
ASSERT VALUE total_amount = 277201.50 WHERE settlement_month = '2024-02'
ASSERT VALUE total_amount = 295976.75 WHERE settlement_month = '2024-03'
SELECT settlement_month,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_amount,
       COUNT(DISTINCT account_id) AS accounts,
       COUNT(DISTINCT counterparty) AS counterparties
FROM {{zone_name}}.delta_demos.monthly_settlements
GROUP BY settlement_month
ORDER BY settlement_month;


-- ============================================================================
-- EXPLORE: January Details — Spot the Duplicates
-- ============================================================================
-- January has 20 settlements. Two are duplicates that slipped through:
--   id=19 is a double-booking of id=1 (same $12,500 payment to Meridian Capital)
--   id=20 is a double-booking of id=5 (same $34,200 payment to Sterling Settlements)
-- Two adjustments also have incorrect amounts (id=6 and id=14).

ASSERT ROW_COUNT = 20
ASSERT VALUE amount = 12500.00 WHERE id = 1
ASSERT VALUE amount = 12500.00 WHERE id = 19
ASSERT VALUE amount = 34200.00 WHERE id = 5
ASSERT VALUE amount = 34200.00 WHERE id = 20
SELECT id, account_id, transaction_type, amount, currency,
       counterparty, settled_at
FROM {{zone_name}}.delta_demos.monthly_settlements
WHERE settlement_month = '2024-01'
ORDER BY id;


-- ============================================================================
-- STEP 1: DELETE — Remove Entire January Partition (Creates DVs)
-- ============================================================================
-- This DELETE targets all rows where settlement_month = '2024-01'. Delta
-- writes deletion vector (.bin) sidecar files that mark each of the 20 row
-- positions as deleted. The original Parquet data files remain physically
-- on disk — only lightweight bitmaps are written alongside them. February
-- and March partition directories are completely untouched.

ASSERT ROW_COUNT = 20
DELETE FROM {{zone_name}}.delta_demos.monthly_settlements
WHERE settlement_month = '2024-01';


-- ============================================================================
-- LEARN: Verify January Is Empty, Feb/Mar Untouched
-- ============================================================================
-- After the DELETE, January's data is logically gone (DVs filter it out),
-- but the original Parquet files still exist on disk. Total drops from
-- 60 to 40. February and March totals are byte-identical to baseline.

ASSERT VALUE jan_count = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS jan_count
FROM {{zone_name}}.delta_demos.monthly_settlements
WHERE settlement_month = '2024-01';

ASSERT ROW_COUNT = 2
ASSERT VALUE txn_count = 20 WHERE settlement_month = '2024-02'
ASSERT VALUE txn_count = 20 WHERE settlement_month = '2024-03'
ASSERT VALUE total_amount = 277201.50 WHERE settlement_month = '2024-02'
ASSERT VALUE total_amount = 295976.75 WHERE settlement_month = '2024-03'
SELECT settlement_month,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_amount
FROM {{zone_name}}.delta_demos.monthly_settlements
GROUP BY settlement_month
ORDER BY settlement_month;

ASSERT VALUE cnt = 40
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.monthly_settlements;


-- ============================================================================
-- STEP 2: INSERT — Load Corrected January Data (18 Rows)
-- ============================================================================
-- The corrected January dataset has 18 rows instead of 20:
--   - id=19 REMOVED (duplicate of id=1: $12,500 to Meridian Capital)
--   - id=20 REMOVED (duplicate of id=5: $34,200 to Sterling Settlements)
--   - id=6  CORRECTED: adjustment amount $1,500.75 -> $1,475.50
--   - id=14 CORRECTED: adjustment amount $950.50 -> $1,125.00
-- This INSERT creates new Parquet files only in the January partition
-- directory. February and March are never touched.

ASSERT ROW_COUNT = 18
INSERT INTO {{zone_name}}.delta_demos.monthly_settlements VALUES
    (1,  'ACC-1001', '2024-01', 'payment',    12500.00, 'USD', 'Meridian Capital LLC',       '2024-01-03 09:15:00'),
    (2,  'ACC-1002', '2024-01', 'payment',    8750.50,  'USD', 'Crossbridge Partners',       '2024-01-04 11:30:00'),
    (3,  'ACC-1003', '2024-01', 'refund',     2100.00,  'EUR', 'Nordic Trade Finance',       '2024-01-05 14:22:00'),
    (4,  'ACC-1001', '2024-01', 'fee',        875.00,   'USD', 'Clearstream Services',       '2024-01-07 08:45:00'),
    (5,  'ACC-1004', '2024-01', 'payment',    34200.00, 'GBP', 'Sterling Settlements Ltd',   '2024-01-08 10:00:00'),
    (6,  'ACC-1002', '2024-01', 'adjustment', 1475.50,  'USD', 'Meridian Capital LLC',       '2024-01-09 13:10:00'),
    (7,  'ACC-1005', '2024-01', 'payment',    19800.00, 'EUR', 'Deutsche Handelsbank AG',    '2024-01-10 09:30:00'),
    (8,  'ACC-1003', '2024-01', 'payment',    6300.25,  'USD', 'Pacific Rim Holdings',       '2024-01-11 15:45:00'),
    (9,  'ACC-1001', '2024-01', 'refund',     3450.00,  'USD', 'Crossbridge Partners',       '2024-01-14 11:00:00'),
    (10, 'ACC-1004', '2024-01', 'payment',    27650.00, 'GBP', 'London Clearing House',      '2024-01-15 08:20:00'),
    (11, 'ACC-1005', '2024-01', 'fee',        1250.00,  'EUR', 'Euroclear Operations',       '2024-01-16 10:15:00'),
    (12, 'ACC-1002', '2024-01', 'payment',    15900.00, 'USD', 'Apex Financial Group',       '2024-01-17 14:30:00'),
    (13, 'ACC-1003', '2024-01', 'payment',    42100.00, 'USD', 'Meridian Capital LLC',       '2024-01-18 09:00:00'),
    (14, 'ACC-1001', '2024-01', 'adjustment', 1125.00,  'USD', 'Clearstream Services',       '2024-01-21 11:45:00'),
    (15, 'ACC-1004', '2024-01', 'payment',    8200.00,  'GBP', 'Sterling Settlements Ltd',   '2024-01-22 13:20:00'),
    (16, 'ACC-1005', '2024-01', 'refund',     4800.00,  'EUR', 'Nordic Trade Finance',       '2024-01-23 08:55:00'),
    (17, 'ACC-1002', '2024-01', 'payment',    11350.00, 'USD', 'Pacific Rim Holdings',       '2024-01-24 15:10:00'),
    (18, 'ACC-1003', '2024-01', 'fee',        625.00,   'USD', 'Apex Financial Group',       '2024-01-25 10:30:00');


-- ============================================================================
-- LEARN: Verify Corrected January Data
-- ============================================================================
-- January now has 18 rows with corrected amounts. The duplicates (id=19,
-- id=20) are gone, and the two adjustment amounts are fixed.

ASSERT ROW_COUNT = 18
SELECT id, account_id, transaction_type, amount, currency,
       counterparty
FROM {{zone_name}}.delta_demos.monthly_settlements
WHERE settlement_month = '2024-01'
ORDER BY id;

-- Verify the specific corrections
ASSERT VALUE amount = 1475.50
SELECT amount FROM {{zone_name}}.delta_demos.monthly_settlements WHERE id = 6;

ASSERT VALUE amount = 1125.00
SELECT amount FROM {{zone_name}}.delta_demos.monthly_settlements WHERE id = 14;

-- Verify duplicates are gone
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.monthly_settlements WHERE id IN (19, 20);

-- Corrected January total: $202,451.25 (was $249,002.00)
ASSERT VALUE total_amount = 202451.25
ASSERT ROW_COUNT = 1
SELECT SUM(amount) AS total_amount
FROM {{zone_name}}.delta_demos.monthly_settlements
WHERE settlement_month = '2024-01';


-- ============================================================================
-- STEP 3: OPTIMIZE — Clean Up Deletion Vectors
-- ============================================================================
-- OPTIMIZE does two things:
--   1. Merges small data files into larger, optimally-sized files
--   2. Applies pending deletion vectors by physically removing deleted
--      rows from the compacted files
--
-- The January partition still has the original Parquet files (with DV
-- sidecar .bin files marking all 20 rows as deleted) alongside the new
-- Parquet files containing the 18 corrected rows. After OPTIMIZE, only
-- clean compacted files remain — no more DV overhead on reads.

OPTIMIZE {{zone_name}}.delta_demos.monthly_settlements;


-- ============================================================================
-- EXPLORE: Final Per-Month Summary
-- ============================================================================
-- After partition replacement and OPTIMIZE:
--   January:  18 rows, $202,451.25 (was 20 rows, $249,002.00)
--   February: 20 rows, $277,201.50 (unchanged)
--   March:    20 rows, $295,976.75 (unchanged)

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 18 WHERE settlement_month = '2024-01'
ASSERT VALUE txn_count = 20 WHERE settlement_month = '2024-02'
ASSERT VALUE txn_count = 20 WHERE settlement_month = '2024-03'
ASSERT VALUE total_amount = 202451.25 WHERE settlement_month = '2024-01'
ASSERT VALUE total_amount = 277201.50 WHERE settlement_month = '2024-02'
ASSERT VALUE total_amount = 295976.75 WHERE settlement_month = '2024-03'
SELECT settlement_month,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_amount,
       COUNT(DISTINCT account_id) AS accounts,
       COUNT(DISTINCT counterparty) AS counterparties
FROM {{zone_name}}.delta_demos.monthly_settlements
GROUP BY settlement_month
ORDER BY settlement_month;


-- ============================================================================
-- EXPLORE: Settlement Breakdown by Transaction Type
-- ============================================================================
-- Payments dominate the settlement volume. The corrected dataset has 36
-- payments (was 38 — two duplicates removed), with unchanged refund,
-- adjustment, and fee counts.

ASSERT ROW_COUNT = 4
ASSERT VALUE txn_count = 36 WHERE transaction_type = 'payment'
ASSERT VALUE total_amount = 721602.00 WHERE transaction_type = 'payment'
SELECT transaction_type,
       COUNT(*) AS txn_count,
       SUM(amount) AS total_amount,
       COUNT(DISTINCT account_id) AS accounts
FROM {{zone_name}}.delta_demos.monthly_settlements
GROUP BY transaction_type
ORDER BY total_amount DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 58 (18 + 20 + 20)
ASSERT VALUE cnt = 58
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.monthly_settlements;

-- Verify jan_corrected_count: 18 rows after dedup
ASSERT VALUE cnt = 18
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.monthly_settlements WHERE settlement_month = '2024-01';

-- Verify feb_unchanged: 20 rows, same total
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.monthly_settlements WHERE settlement_month = '2024-02';

-- Verify mar_unchanged: 20 rows, same total
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.monthly_settlements WHERE settlement_month = '2024-03';

-- Verify jan_corrected_total: $202,451.25
ASSERT VALUE total_amount = 202451.25
SELECT SUM(amount) AS total_amount FROM {{zone_name}}.delta_demos.monthly_settlements WHERE settlement_month = '2024-01';

-- Verify feb_total_unchanged: $277,201.50
ASSERT VALUE total_amount = 277201.50
SELECT SUM(amount) AS total_amount FROM {{zone_name}}.delta_demos.monthly_settlements WHERE settlement_month = '2024-02';

-- Verify mar_total_unchanged: $295,976.75
ASSERT VALUE total_amount = 295976.75
SELECT SUM(amount) AS total_amount FROM {{zone_name}}.delta_demos.monthly_settlements WHERE settlement_month = '2024-03';

-- Verify duplicates_removed: id=19 and id=20 no longer exist
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.monthly_settlements WHERE id IN (19, 20);

-- Verify adjustment_correction_id6: $1,500.75 -> $1,475.50
ASSERT VALUE amount = 1475.50
SELECT amount FROM {{zone_name}}.delta_demos.monthly_settlements WHERE id = 6;

-- Verify adjustment_correction_id14: $950.50 -> $1,125.00
ASSERT VALUE amount = 1125.00
SELECT amount FROM {{zone_name}}.delta_demos.monthly_settlements WHERE id = 14;

-- Verify grand_total: $775,629.50 (was $822,180.25)
ASSERT VALUE total_amount = 775629.50
SELECT SUM(amount) AS total_amount FROM {{zone_name}}.delta_demos.monthly_settlements;

-- Verify payment_count: 36 (was 38, removed 2 duplicate payments)
ASSERT VALUE cnt = 36
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.monthly_settlements WHERE transaction_type = 'payment';
