-- ============================================================================
-- Delta Partition Replace — Educational Queries
-- ============================================================================
-- WHAT: The partition-replace pattern uses DELETE + INSERT to atomically swap
--       an entire partition's data with a corrected dataset, while leaving
--       every other partition byte-identical.
-- WHY:  Production ETL pipelines frequently receive late corrections — a
--       restated month of revenue, a vendor's updated price file, or a
--       compliance-driven data fix. Reloading the entire table is wasteful
--       and dangerous. Replacing just the affected partition is surgical.
-- HOW:  DELETE WHERE sale_month = '2024-02' removes all February files, then
--       INSERT loads the corrected rows into a fresh February partition
--       directory. January and March files are never touched.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Monthly Revenue Before Corrections
-- ============================================================================
-- Three months of sales data, 20 transactions each. February contains pricing
-- errors (Tool C priced at $45.00 instead of the correct $48.50):

ASSERT ROW_COUNT = 3
ASSERT VALUE total_revenue = 3063.64 WHERE sale_month = '2024-01'
ASSERT VALUE total_revenue = 3692.15 WHERE sale_month = '2024-02'
ASSERT VALUE total_revenue = 4368.62 WHERE sale_month = '2024-03'
SELECT sale_month,
       COUNT(*) AS txn_count,
       ROUND(SUM(unit_price * qty), 2) AS total_revenue,
       SUM(qty) AS total_units
FROM {{zone_name}}.delta_demos.monthly_sales
GROUP BY sale_month
ORDER BY sale_month;


-- ============================================================================
-- Query 2: Identify February Errors — Tool C Pricing
-- ============================================================================
-- These 5 February Tool C transactions all show $45.00. The correct price
-- (confirmed by the vendor) is $48.50. This affects ids 23, 27, 31, 35, 39:

ASSERT ROW_COUNT = 5
ASSERT VALUE unit_price = 45.0 WHERE id = 23
SELECT id, store_id, product, unit_price, qty,
       ROUND(unit_price * qty, 2) AS line_total
FROM {{zone_name}}.delta_demos.monthly_sales
WHERE sale_month = '2024-02' AND product = 'Tool C'
ORDER BY id;


-- ============================================================================
-- LEARN: Step 1 — DELETE the Entire February Partition
-- ============================================================================
-- This DELETE targets only sale_month = '2024-02'. Delta removes all Parquet
-- files in the February partition directory. January and March directories
-- are completely untouched — their files are not read or modified.

ASSERT ROW_COUNT = 20
DELETE FROM {{zone_name}}.delta_demos.monthly_sales
WHERE sale_month = '2024-02';


-- ============================================================================
-- EXPLORE: Verify February Is Gone
-- ============================================================================
-- After the DELETE, only January and March remain. The table dropped from
-- 60 to 40 rows:

ASSERT ROW_COUNT = 2
ASSERT VALUE txn_count = 20 WHERE sale_month = '2024-01'
ASSERT VALUE txn_count = 20 WHERE sale_month = '2024-03'
SELECT sale_month,
       COUNT(*) AS txn_count,
       ROUND(SUM(unit_price * qty), 2) AS total_revenue
FROM {{zone_name}}.delta_demos.monthly_sales
GROUP BY sale_month
ORDER BY sale_month;


-- ============================================================================
-- LEARN: Step 2 — INSERT Corrected February Data
-- ============================================================================
-- Now we reload February with the corrected prices. Tool C is fixed to $48.50,
-- and two quantity adjustments (id=28: qty 7→8, id=32: qty 9→7). All other
-- February rows are unchanged. This INSERT creates new Parquet files only in
-- the February partition directory.

ASSERT ROW_COUNT = 20
INSERT INTO {{zone_name}}.delta_demos.monthly_sales VALUES
    (21, 'STORE-01', 'Widget A',    120.50, 4, '2024-02-02', '2024-02'),
    (22, 'STORE-02', 'Gadget B',    89.99,  2, '2024-02-03', '2024-02'),
    (23, 'STORE-03', 'Tool C',      48.50,  6, '2024-02-05', '2024-02'),
    (24, 'STORE-01', 'Accessory D', 15.99,  4, '2024-02-06', '2024-02'),
    (25, 'STORE-02', 'Widget A',    120.50, 3, '2024-02-08', '2024-02'),
    (26, 'STORE-03', 'Gadget B',    89.99,  1, '2024-02-09', '2024-02'),
    (27, 'STORE-01', 'Tool C',      48.50,  5, '2024-02-10', '2024-02'),
    (28, 'STORE-02', 'Accessory D', 15.99,  8, '2024-02-12', '2024-02'),
    (29, 'STORE-03', 'Widget A',    120.50, 2, '2024-02-13', '2024-02'),
    (30, 'STORE-01', 'Gadget B',    89.99,  3, '2024-02-15', '2024-02'),
    (31, 'STORE-02', 'Tool C',      48.50,  2, '2024-02-16', '2024-02'),
    (32, 'STORE-03', 'Accessory D', 15.99,  7, '2024-02-17', '2024-02'),
    (33, 'STORE-01', 'Widget A',    120.50, 1, '2024-02-19', '2024-02'),
    (34, 'STORE-02', 'Gadget B',    89.99,  2, '2024-02-20', '2024-02'),
    (35, 'STORE-03', 'Tool C',      48.50,  3, '2024-02-22', '2024-02'),
    (36, 'STORE-01', 'Accessory D', 15.99,  6, '2024-02-23', '2024-02'),
    (37, 'STORE-02', 'Widget A',    120.50, 2, '2024-02-24', '2024-02'),
    (38, 'STORE-03', 'Gadget B',    89.99,  1, '2024-02-25', '2024-02'),
    (39, 'STORE-01', 'Tool C',      48.50,  4, '2024-02-27', '2024-02'),
    (40, 'STORE-02', 'Widget A',    120.50, 1, '2024-02-28', '2024-02');


-- ============================================================================
-- Query 3: Verify Corrected February — Tool C at $48.50
-- ============================================================================
-- All 5 Tool C transactions now show the correct $48.50 price:

ASSERT ROW_COUNT = 5
ASSERT VALUE unit_price = 48.5 WHERE id = 23
ASSERT VALUE unit_price = 48.5 WHERE id = 27
SELECT id, store_id, product, unit_price, qty,
       ROUND(unit_price * qty, 2) AS line_total
FROM {{zone_name}}.delta_demos.monthly_sales
WHERE sale_month = '2024-02' AND product = 'Tool C'
ORDER BY id;


-- ============================================================================
-- Query 4: Revenue Impact — Before vs After Correction
-- ============================================================================
-- February revenue changed from $3,692.15 to $3,746.16 after the price and
-- quantity corrections. January and March are unchanged:

ASSERT ROW_COUNT = 3
ASSERT VALUE total_revenue = 3063.64 WHERE sale_month = '2024-01'
ASSERT VALUE total_revenue = 3746.16 WHERE sale_month = '2024-02'
ASSERT VALUE total_revenue = 4368.62 WHERE sale_month = '2024-03'
SELECT sale_month,
       COUNT(*) AS txn_count,
       ROUND(SUM(unit_price * qty), 2) AS total_revenue,
       SUM(qty) AS total_units
FROM {{zone_name}}.delta_demos.monthly_sales
GROUP BY sale_month
ORDER BY sale_month;


-- ============================================================================
-- LEARN: Incremental Append — New April Partition
-- ============================================================================
-- The other half of the ETL pattern: appending a brand-new month. This INSERT
-- creates a new partition directory (sale_month=2024-04/) without touching
-- any existing partitions. April uses the corrected Tool C price ($48.50).

ASSERT ROW_COUNT = 20
INSERT INTO {{zone_name}}.delta_demos.monthly_sales VALUES
    (61, 'STORE-01', 'Widget A',    120.50, 6, '2024-04-01', '2024-04'),
    (62, 'STORE-02', 'Gadget B',    89.99,  3, '2024-04-03', '2024-04'),
    (63, 'STORE-03', 'Tool C',      48.50,  5, '2024-04-04', '2024-04'),
    (64, 'STORE-01', 'Accessory D', 15.99,  4, '2024-04-05', '2024-04'),
    (65, 'STORE-02', 'Widget A',    120.50, 2, '2024-04-07', '2024-04'),
    (66, 'STORE-03', 'Gadget B',    89.99,  2, '2024-04-08', '2024-04'),
    (67, 'STORE-01', 'Tool C',      48.50,  3, '2024-04-10', '2024-04'),
    (68, 'STORE-02', 'Accessory D', 15.99,  6, '2024-04-11', '2024-04'),
    (69, 'STORE-03', 'Widget A',    120.50, 4, '2024-04-13', '2024-04'),
    (70, 'STORE-01', 'Gadget B',    89.99,  2, '2024-04-14', '2024-04'),
    (71, 'STORE-02', 'Tool C',      48.50,  4, '2024-04-15', '2024-04'),
    (72, 'STORE-03', 'Accessory D', 15.99,  5, '2024-04-17', '2024-04'),
    (73, 'STORE-01', 'Widget A',    120.50, 3, '2024-04-18', '2024-04'),
    (74, 'STORE-02', 'Gadget B',    89.99,  1, '2024-04-20', '2024-04'),
    (75, 'STORE-03', 'Tool C',      48.50,  6, '2024-04-21', '2024-04'),
    (76, 'STORE-01', 'Accessory D', 15.99,  8, '2024-04-22', '2024-04'),
    (77, 'STORE-02', 'Widget A',    120.50, 2, '2024-04-24', '2024-04'),
    (78, 'STORE-03', 'Gadget B',    89.99,  3, '2024-04-25', '2024-04'),
    (79, 'STORE-01', 'Tool C',      48.50,  2, '2024-04-27', '2024-04'),
    (80, 'STORE-02', 'Widget A',    120.50, 1, '2024-04-29', '2024-04');


-- ============================================================================
-- Query 5: Four-Month Overview After Append
-- ============================================================================
-- The table now has 4 partitions. January and March are the original data,
-- February is the corrected replacement, April is the fresh append:

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 4496.66 WHERE sale_month = '2024-04'
SELECT sale_month,
       COUNT(*) AS txn_count,
       ROUND(SUM(unit_price * qty), 2) AS total_revenue,
       SUM(qty) AS total_units
FROM {{zone_name}}.delta_demos.monthly_sales
GROUP BY sale_month
ORDER BY sale_month;


-- ============================================================================
-- Query 6: Store Performance Across All Months
-- ============================================================================
-- Cross-partition aggregation by store shows contribution across the full
-- 4-month window. All partition directories are read for this query:

ASSERT ROW_COUNT = 3
SELECT store_id,
       COUNT(*) AS txn_count,
       ROUND(SUM(unit_price * qty), 2) AS total_revenue,
       SUM(qty) AS total_units
FROM {{zone_name}}.delta_demos.monthly_sales
GROUP BY store_id
ORDER BY total_revenue DESC;


-- ============================================================================
-- Query 7: Product Revenue Breakdown
-- ============================================================================
-- Widget A dominates revenue thanks to its higher unit price and volume.
-- Tool C revenue reflects the corrected $48.50 price in Feb and Apr:

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 6868.5 WHERE product = 'Widget A'
SELECT product,
       COUNT(*) AS txn_count,
       ROUND(SUM(unit_price * qty), 2) AS total_revenue,
       SUM(qty) AS total_units
FROM {{zone_name}}.delta_demos.monthly_sales
GROUP BY product
ORDER BY total_revenue DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 80 (20 per month × 4 months)
ASSERT VALUE cnt = 80
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.monthly_sales;

-- Verify jan_unchanged: January revenue identical to baseline
ASSERT VALUE revenue = 3063.64
SELECT ROUND(SUM(unit_price * qty), 2) AS revenue FROM {{zone_name}}.delta_demos.monthly_sales WHERE sale_month = '2024-01';

-- Verify feb_corrected: February revenue reflects corrected prices
ASSERT VALUE revenue = 3746.16
SELECT ROUND(SUM(unit_price * qty), 2) AS revenue FROM {{zone_name}}.delta_demos.monthly_sales WHERE sale_month = '2024-02';

-- Verify mar_unchanged: March revenue identical to baseline
ASSERT VALUE revenue = 4368.62
SELECT ROUND(SUM(unit_price * qty), 2) AS revenue FROM {{zone_name}}.delta_demos.monthly_sales WHERE sale_month = '2024-03';

-- Verify apr_appended: April revenue
ASSERT VALUE revenue = 4496.66
SELECT ROUND(SUM(unit_price * qty), 2) AS revenue FROM {{zone_name}}.delta_demos.monthly_sales WHERE sale_month = '2024-04';

-- Verify feb_tool_c_fixed: id=23 now at $48.50
ASSERT VALUE unit_price = 48.5
SELECT unit_price FROM {{zone_name}}.delta_demos.monthly_sales WHERE id = 23;

-- Verify grand_total: sum across all 4 months
ASSERT VALUE revenue = 15675.08
SELECT ROUND(SUM(unit_price * qty), 2) AS revenue FROM {{zone_name}}.delta_demos.monthly_sales;
