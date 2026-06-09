-- ============================================================================
-- Delta Recovery Strategy — RESTORE vs Partition Replace — Educational Queries
-- ============================================================================
-- WHAT: RESTORE rewinds the entire table to a prior version. But when only
--       one partition is corrupted and other partitions have legitimate
--       changes, RESTORE causes collateral damage. The partition-scoped
--       replacement pattern (DELETE + INSERT FROM VERSION AS OF) lets you
--       surgically fix one partition while preserving all others.
-- WHY:  A retail chain discovers Q1 tax rates were corrupted. But Q2 already
--       has a price correction and Q3 has late-arriving transactions. Full
--       RESTORE would undo all of that.
-- HOW:  1. Build up a realistic version history with good and bad changes
--       2. Show that RESTORE would destroy wanted changes
--       3. Use DELETE Q1 + INSERT FROM VERSION AS OF to fix only Q1
--       4. Verify all other partitions are untouched
--
-- Version history we will build:
--   V0: CREATE empty delta table partitioned by quarter (done in setup.sql)
--   V1: INSERT 40 rows — 10 per quarter, Q1–Q4 (done in setup.sql)
--   V2: UPDATE — Correct Q2 monitor price ($349.99 → $379.99)
--   V3: INSERT — 5 late-arriving Q3 transactions
--   V4: UPDATE — ACCIDENT: wrong tax rate (0.15) applied to all Q1 rows
--   V5: DELETE Q1 + INSERT Q1 FROM VERSION AS OF 1 — partition-scoped fix
-- ============================================================================


-- ============================================================================
-- Query 1: V1 Baseline — Quarterly Revenue Summary
-- ============================================================================
-- The setup script inserted 40 rows across 4 quarters with uniform 8% tax.
-- Each quarter has exactly 10 transactions from 5 stores.

ASSERT ROW_COUNT = 4
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q1'
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q2'
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q3'
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q4'
ASSERT VALUE total_revenue = 36176.15 WHERE quarter = 'Q1'
ASSERT VALUE total_revenue = 44761.56 WHERE quarter = 'Q2'
ASSERT VALUE total_revenue = 51424.68 WHERE quarter = 'Q3'
ASSERT VALUE total_revenue = 57612.61 WHERE quarter = 'Q4'
SELECT quarter,
       COUNT(*)          AS txn_count,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.quarterly_revenue
GROUP BY quarter
ORDER BY quarter;


-- ============================================================================
-- Query 2: V2 — Correct Q2 Monitor Price ($349.99 → $379.99)
-- ============================================================================
-- The purchasing team discovers the Q2 monitor was invoiced at $349.99 but
-- the correct negotiated price is $379.99. This is a legitimate correction
-- that must survive any recovery operation.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.quarterly_revenue
SET unit_price = 379.99,
    total      = ROUND(15 * 379.99 * 1.08, 2)
WHERE id = 12;


-- ============================================================================
-- Query 3: Verify V2 — Q2 Monitor Correction Applied
-- ============================================================================
-- Row id=12 now has unit_price=379.99 and total=6155.84.
-- Q2 total revenue increases from 44761.56 to 45247.56.

ASSERT ROW_COUNT = 1
ASSERT VALUE unit_price = 379.99
ASSERT VALUE total = 6155.84
SELECT id, product, unit_price, total
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE id = 12;

ASSERT ROW_COUNT = 1
ASSERT VALUE total_revenue = 45247.56
SELECT quarter,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q2'
GROUP BY quarter;


-- ============================================================================
-- Query 4: V3 — Late-Arriving Q3 Transactions (5 New Rows)
-- ============================================================================
-- Five additional Q3 transactions from remote stores arrive after the
-- quarter closed. These are valid sales that must be included.

ASSERT ROW_COUNT = 5
INSERT INTO {{zone_name}}.delta_demos.quarterly_revenue VALUES
    (41, 'Q3', 'STR01', 'Dock Station', 8,  189.99, 0.08, 1641.51),
    (42, 'Q3', 'STR02', 'SSD Drive',   15,  129.99, 0.08, 2105.84),
    (43, 'Q3', 'STR03', 'RAM Kit',     10,   89.99, 0.08, 971.89),
    (44, 'Q3', 'STR04', 'Power Strip', 20,   34.99, 0.08, 755.78),
    (45, 'Q3', 'STR05', 'Mouse Pad',   25,   24.99, 0.08, 674.73);


-- ============================================================================
-- Query 5: Verify V3 — Q3 Now Has 15 Rows
-- ============================================================================
-- Q3 grows from 10 to 15 rows. Q3 total revenue increases from
-- 51424.68 to 57574.43. Overall table now has 45 rows.

ASSERT ROW_COUNT = 1
ASSERT VALUE txn_count = 15
ASSERT VALUE total_revenue = 57574.43
SELECT quarter,
       COUNT(*)          AS txn_count,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q3'
GROUP BY quarter;

ASSERT ROW_COUNT = 1
ASSERT VALUE cnt = 45
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.quarterly_revenue;


-- ============================================================================
-- Query 6: V4 — ACCIDENT: Wrong Tax Rate Applied to All Q1 Rows
-- ============================================================================
-- A data engineer runs a tax rate correction script but uses 15% (the rate
-- for a different jurisdiction) instead of the correct 8%. This corrupts
-- every Q1 row — the tax_rate column AND the precomputed total column.

ASSERT ROW_COUNT = 10
UPDATE {{zone_name}}.delta_demos.quarterly_revenue
SET tax_rate = 0.15,
    total    = ROUND(units * unit_price * 1.15, 2)
WHERE quarter = 'Q1';


-- ============================================================================
-- Query 7: Verify V4 — Q1 Is Now Corrupted
-- ============================================================================
-- Q1 total revenue is inflated from 36176.15 to 38520.9 due to the wrong
-- tax rate. All Q1 rows now show tax_rate = 0.15 instead of 0.08.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_revenue = 38520.9
SELECT quarter,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q1'
GROUP BY quarter;

ASSERT ROW_COUNT = 10
SELECT id, product, tax_rate, total
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q1'
ORDER BY id;


-- ============================================================================
-- Query 8: THE PROBLEM — Why Full RESTORE Would Cause Collateral Damage
-- ============================================================================
-- RESTORE to V1 (the last version with correct Q1 data) would also:
--   1. Undo the Q2 monitor price correction (V2) — $379.99 back to $349.99
--   2. Remove the 5 late-arriving Q3 transactions (V3) — 15 rows back to 10
-- Let us prove this by comparing VERSION AS OF 1 with current data.

-- V1 had the OLD Q2 monitor price — RESTORE would revert this correction
ASSERT ROW_COUNT = 1
ASSERT VALUE unit_price = 349.99
SELECT id, product, unit_price
FROM {{zone_name}}.delta_demos.quarterly_revenue VERSION AS OF 1
WHERE id = 12;

-- V1 had only 10 Q3 rows — RESTORE would lose the 5 late arrivals
ASSERT ROW_COUNT = 1
ASSERT VALUE q3_count = 10
SELECT COUNT(*) AS q3_count
FROM {{zone_name}}.delta_demos.quarterly_revenue VERSION AS OF 1
WHERE quarter = 'Q3';

-- Current table has the corrected Q2 monitor price we want to KEEP
ASSERT ROW_COUNT = 1
ASSERT VALUE unit_price = 379.99
SELECT id, product, unit_price
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE id = 12;

-- Current table has 15 Q3 rows we want to KEEP
ASSERT ROW_COUNT = 1
ASSERT VALUE q3_count = 15
SELECT COUNT(*) AS q3_count
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q3';


-- ============================================================================
-- Query 9: V5a — Partition-Scoped Fix Step 1: DELETE Corrupted Q1
-- ============================================================================
-- Instead of RESTORE (which is all-or-nothing), we surgically remove only
-- the corrupted Q1 partition. This leaves Q2, Q3, and Q4 untouched.

DELETE FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q1';

-- Confirm Q1 is gone but Q2-Q4 are intact
ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q2'
ASSERT VALUE txn_count = 15 WHERE quarter = 'Q3'
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q4'
SELECT quarter,
       COUNT(*) AS txn_count
FROM {{zone_name}}.delta_demos.quarterly_revenue
GROUP BY quarter
ORDER BY quarter;


-- ============================================================================
-- Query 10: V5b — Partition-Scoped Fix Step 2: Re-INSERT Clean Q1 from V1
-- ============================================================================
-- Pull the original Q1 data from VERSION AS OF 1 (before any corruption)
-- and insert it back. This is the key technique: combining time travel
-- with partition-scoped operations.

ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.quarterly_revenue
SELECT * FROM {{zone_name}}.delta_demos.quarterly_revenue VERSION AS OF 1
WHERE quarter = 'Q1';


-- ============================================================================
-- Query 11: Verify Fix — Q1 Restored to Correct Tax Rate
-- ============================================================================
-- Q1 is back to tax_rate = 0.08 and the original correct totals.
-- Total Q1 revenue is 36176.15 again (not the inflated 38520.9).

ASSERT ROW_COUNT = 1
ASSERT VALUE total_revenue = 36176.15
SELECT quarter,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q1'
GROUP BY quarter;

ASSERT ROW_COUNT = 10
SELECT id, product, tax_rate, total
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q1'
ORDER BY id;


-- ============================================================================
-- Query 12: Verify Fix — Q2 Correction Preserved
-- ============================================================================
-- The Q2 monitor price correction from V2 is still intact.
-- unit_price = 379.99 (not the original 349.99).

ASSERT ROW_COUNT = 1
ASSERT VALUE unit_price = 379.99
ASSERT VALUE total = 6155.84
SELECT id, product, unit_price, total
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE id = 12;

ASSERT ROW_COUNT = 1
ASSERT VALUE total_revenue = 45247.56
SELECT quarter,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q2'
GROUP BY quarter;


-- ============================================================================
-- Query 13: Verify Fix — Q3 Late Inserts Preserved
-- ============================================================================
-- All 15 Q3 rows (original 10 + 5 late arrivals) are still present.
-- The late-arriving transactions from V3 survived the partition fix.

ASSERT ROW_COUNT = 1
ASSERT VALUE txn_count = 15
ASSERT VALUE total_revenue = 57574.43
SELECT quarter,
       COUNT(*)          AS txn_count,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q3'
GROUP BY quarter;

-- Confirm the 5 late-arriving rows specifically
ASSERT ROW_COUNT = 5
SELECT id, store_id, product, total
FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE id >= 41
ORDER BY id;


-- ============================================================================
-- Query 14: Full Summary — All Quarters Correct
-- ============================================================================
-- Final state: 45 rows across 4 quarters with all corrections preserved
-- and the Q1 corruption fully repaired.

ASSERT ROW_COUNT = 4
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q1'
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q2'
ASSERT VALUE txn_count = 15 WHERE quarter = 'Q3'
ASSERT VALUE txn_count = 10 WHERE quarter = 'Q4'
ASSERT VALUE total_revenue = 36176.15 WHERE quarter = 'Q1'
ASSERT VALUE total_revenue = 45247.56 WHERE quarter = 'Q2'
ASSERT VALUE total_revenue = 57574.43 WHERE quarter = 'Q3'
ASSERT VALUE total_revenue = 57612.61 WHERE quarter = 'Q4'
SELECT quarter,
       COUNT(*)          AS txn_count,
       ROUND(SUM(total), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.quarterly_revenue
GROUP BY quarter
ORDER BY quarter;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 45 rows (40 original + 5 late Q3)
ASSERT VALUE cnt = 45
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.quarterly_revenue;

-- Verify q1_tax_rate: all Q1 rows back to 0.08 (not corrupted 0.15)
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q1' AND tax_rate = 0.08;

-- Verify q1_revenue: Q1 total restored to correct value
ASSERT VALUE total_revenue = 36176.15
SELECT ROUND(SUM(total), 2) AS total_revenue FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q1';

-- Verify q2_monitor_price: correction from V2 preserved
ASSERT VALUE unit_price = 379.99
SELECT unit_price FROM {{zone_name}}.delta_demos.quarterly_revenue WHERE id = 12;

-- Verify q2_revenue: Q2 total includes corrected monitor price
ASSERT VALUE total_revenue = 45247.56
SELECT ROUND(SUM(total), 2) AS total_revenue FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q2';

-- Verify q3_row_count: late-arriving transactions preserved
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q3';

-- Verify q3_revenue: Q3 total includes late arrivals
ASSERT VALUE total_revenue = 57574.43
SELECT ROUND(SUM(total), 2) AS total_revenue FROM {{zone_name}}.delta_demos.quarterly_revenue
WHERE quarter = 'Q3';

-- Verify overall_revenue: sum of all corrected quarters
ASSERT VALUE total_revenue = 196610.75
SELECT ROUND(SUM(total), 2) AS total_revenue FROM {{zone_name}}.delta_demos.quarterly_revenue;
