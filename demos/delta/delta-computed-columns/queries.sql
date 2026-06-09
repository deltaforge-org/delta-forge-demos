-- ============================================================================
-- Delta Computed Fields — Educational Queries
-- ============================================================================
-- WHAT: Computed columns are derived values calculated via SQL expressions
--       (e.g., subtotal = qty * unit_price) rather than stored independently.
-- WHY:  Keeping formulas in CTEs ensures every INSERT applies the same logic,
--       preventing drift between manually entered values across batches.
-- HOW:  Delta stores the final computed values in Parquet data files — the
--       formulas exist only in your SQL, not in the Delta transaction log.
-- ============================================================================


-- ============================================================================
-- STEP 1: Insert 40 invoices with CTE-computed values
-- ============================================================================
-- The CTE pattern below separates raw data (base) from formula logic (computed).
-- This makes the formulas reusable across batches and easy to audit:
--
--   subtotal     = ROUND(qty * unit_price, 2)
--   discount_amt = ROUND(subtotal * discount_pct / 100, 2)
--   total        = subtotal - discount_amt
--   commission   = ROUND(total * 0.05, 2)

ASSERT ROW_COUNT = 40
INSERT INTO {{zone_name}}.delta_demos.sales_invoices
WITH base AS (
    SELECT * FROM (VALUES
        (1,  'Laptop Pro',      2,  999.99, 5.0,  'Alice',  '2024-01-05'),
        (2,  'Monitor 27"',     3,  449.99, 5.0,  'Bob',    '2024-01-06'),
        (3,  'Keyboard MX',     10, 99.99,  10.0, 'Alice',  '2024-01-07'),
        (4,  'Mouse Wireless',  20, 29.99,  10.0, 'Carol',  '2024-01-08'),
        (5,  'USB-C Dock',      5,  89.99,  5.0,  'Bob',    '2024-01-09'),
        (6,  'Headphones',      4,  199.99, 5.0,  'Sarah',  '2024-01-10'),
        (7,  'Webcam HD',       8,  69.99,  10.0, 'Alice',  '2024-01-11'),
        (8,  'Desk Lamp',       15, 45.99,  0.0,  'Carol',  '2024-01-12'),
        (9,  'Office Chair',    2,  349.99, 5.0,  'Sarah',  '2024-01-13'),
        (10, 'Standing Desk',   1,  599.99, 0.0,  'Bob',    '2024-01-14'),
        (11, 'Cable Kit',       25, 12.99,  10.0, 'Carol',  '2024-01-15'),
        (12, 'Power Strip',     10, 19.99,  0.0,  'Alice',  '2024-01-16'),
        (13, 'Desk Mat',        12, 24.99,  5.0,  'Bob',    '2024-01-17'),
        (14, 'Footrest',        6,  44.99,  5.0,  'Sarah',  '2024-01-18'),
        (15, 'Whiteboard',      3,  89.99,  0.0,  'Carol',  '2024-01-19'),
        (16, 'Printer',         2,  299.99, 5.0,  'Alice',  '2024-01-20'),
        (17, 'Scanner',         2,  199.99, 5.0,  'Bob',    '2024-01-21'),
        (18, 'Projector',       1,  799.99, 5.0,  'Sarah',  '2024-01-22'),
        (19, 'Speaker Set',     4,  79.99,  10.0, 'Carol',  '2024-01-23'),
        (20, 'Microphone',      3,  149.99, 5.0,  'Alice',  '2024-01-24'),
        (21, 'Tablet Stand',    8,  34.99,  0.0,  'Bob',    '2024-01-25'),
        (22, 'Phone Mount',     10, 15.99,  10.0, 'Carol',  '2024-01-26'),
        (23, 'HDMI Cable',      20, 9.99,   0.0,  'Alice',  '2024-01-27'),
        (24, 'Laptop Bag',      5,  59.99,  5.0,  'Sarah',  '2024-01-28'),
        (25, 'Screen Cleaner',  30, 7.99,   10.0, 'Bob',    '2024-01-29'),
        (26, 'Surge Protector', 4,  29.99,  0.0,  'Carol',  '2024-01-30'),
        (27, 'Desk Fan',        6,  39.99,  5.0,  'Alice',  '2024-01-31'),
        (28, 'Air Purifier',    2,  249.99, 5.0,  'Sarah',  '2024-02-01'),
        (29, 'Book Stand',      10, 19.99,  0.0,  'Bob',    '2024-02-02'),
        (30, 'Pen Set',         15, 14.99,  10.0, 'Carol',  '2024-02-03'),
        (31, 'Stapler',         20, 8.99,   0.0,  'Alice',  '2024-02-04'),
        (32, 'Paper Shredder',  1,  149.99, 5.0,  'Bob',    '2024-02-05'),
        (33, 'Label Maker',     3,  49.99,  5.0,  'Sarah',  '2024-02-06'),
        (34, 'Calculator',      8,  24.99,  0.0,  'Carol',  '2024-02-07'),
        (35, 'Tape Dispenser',  12, 6.99,   0.0,  'Alice',  '2024-02-08'),
        (36, 'Clipboard',       20, 4.99,   10.0, 'Bob',    '2024-02-09'),
        (37, 'Hole Punch',      5,  12.99,  0.0,  'Carol',  '2024-02-10'),
        (38, 'Ruler Set',       10, 3.99,   0.0,  'Alice',  '2024-02-11'),
        (39, 'Correction Tape', 25, 2.99,   10.0, 'Bob',    '2024-02-12'),
        (40, 'Notebook Pack',   8,  11.99,  5.0,  'Sarah',  '2024-02-13')
    ) AS t(id, item, qty, unit_price, discount_pct, sales_rep, invoice_date)
),
computed AS (
    SELECT
        id, item, qty, unit_price, discount_pct,
        ROUND(qty * unit_price, 2) AS subtotal,
        ROUND(ROUND(qty * unit_price, 2) * discount_pct / 100.0, 2) AS discount_amt,
        ROUND(qty * unit_price, 2) - ROUND(ROUND(qty * unit_price, 2) * discount_pct / 100.0, 2) AS total,
        sales_rep,
        ROUND((ROUND(qty * unit_price, 2) - ROUND(ROUND(qty * unit_price, 2) * discount_pct / 100.0, 2)) * 0.05, 2) AS commission,
        invoice_date
    FROM base
)
SELECT * FROM computed;


-- ============================================================================
-- EXPLORE: Inspect the first batch of invoices
-- ============================================================================
-- Let's look at a sample of invoices to understand the column relationships.
-- Notice how subtotal, discount_amt, total, and commission are all derived
-- from the base columns (qty, unit_price, discount_pct).

ASSERT ROW_COUNT = 5
SELECT id, item, qty, unit_price, discount_pct,
       subtotal, discount_amt, total, commission, sales_rep
FROM {{zone_name}}.delta_demos.sales_invoices
WHERE id <= 5
ORDER BY id;


-- ============================================================================
-- STEP 2: Insert 10 more invoices with higher discount tier (15-25%)
-- ============================================================================
-- The same CTE formula pattern is reused for a second batch. Bulk orders
-- get higher discounts, but the computed column logic stays identical.
-- This is the key benefit: one formula, many batches, zero drift.

ASSERT ROW_COUNT = 10
INSERT INTO {{zone_name}}.delta_demos.sales_invoices
WITH base AS (
    SELECT * FROM (VALUES
        (41, 'Bulk Laptops',    10, 899.99, 15.0, 'Sarah',  '2024-02-14'),
        (42, 'Bulk Monitors',   20, 399.99, 20.0, 'Alice',  '2024-02-15'),
        (43, 'Bulk Keyboards',  50, 79.99,  25.0, 'Bob',    '2024-02-16'),
        (44, 'Bulk Mice',       100,24.99,  25.0, 'Carol',  '2024-02-17'),
        (45, 'Bulk Cables',     200,8.99,   20.0, 'Alice',  '2024-02-18'),
        (46, 'Bulk Webcams',    30, 59.99,  15.0, 'Sarah',  '2024-02-19'),
        (47, 'Bulk Headsets',   25, 149.99, 20.0, 'Bob',    '2024-02-20'),
        (48, 'Bulk Chairs',     10, 299.99, 15.0, 'Carol',  '2024-02-21'),
        (49, 'Bulk Desks',      5,  499.99, 15.0, 'Sarah',  '2024-02-22'),
        (50, 'Bulk Printers',   8,  249.99, 20.0, 'Alice',  '2024-02-23')
    ) AS t(id, item, qty, unit_price, discount_pct, sales_rep, invoice_date)
),
computed AS (
    SELECT
        id, item, qty, unit_price, discount_pct,
        ROUND(qty * unit_price, 2) AS subtotal,
        ROUND(ROUND(qty * unit_price, 2) * discount_pct / 100.0, 2) AS discount_amt,
        ROUND(qty * unit_price, 2) - ROUND(ROUND(qty * unit_price, 2) * discount_pct / 100.0, 2) AS total,
        sales_rep,
        ROUND((ROUND(qty * unit_price, 2) - ROUND(ROUND(qty * unit_price, 2) * discount_pct / 100.0, 2)) * 0.05, 2) AS commission,
        invoice_date
    FROM base
)
SELECT * FROM computed;


-- ============================================================================
-- LEARN: How CTE Formulas Maintain Consistency
-- ============================================================================
-- Both batches used the exact same CTE formula. Let's verify the formula
-- chain by recomputing values from scratch and comparing them to what is
-- stored. Zero mismatches means perfect consistency across all 50 rows.

ASSERT VALUE total_rows = 50
ASSERT VALUE subtotal_mismatches = 0
ASSERT VALUE discount_mismatches = 0
ASSERT VALUE total_mismatches = 0
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE ABS(subtotal - ROUND(qty * unit_price, 2)) > 0.01) AS subtotal_mismatches,
    COUNT(*) FILTER (WHERE ABS(discount_amt - ROUND(subtotal * discount_pct / 100.0, 2)) > 0.01) AS discount_mismatches,
    COUNT(*) FILTER (WHERE ABS(total - (subtotal - discount_amt)) > 0.01) AS total_mismatches
FROM {{zone_name}}.delta_demos.sales_invoices;


-- ============================================================================
-- STEP 3: UPDATE — bonus commission (8%) for Sarah on orders > $500 total
-- ============================================================================
-- After initial insertion, we selectively override the computed commission
-- for sales rep "Sarah" on high-value orders. This is a common pattern:
-- compute defaults at INSERT time, then selectively override via UPDATE.
--
-- In the Delta transaction log, this UPDATE creates a new version of the
-- table with the modified rows rewritten into new Parquet files, while
-- unchanged rows remain in their original files.

ASSERT ROW_COUNT = 6
UPDATE {{zone_name}}.delta_demos.sales_invoices
SET commission = ROUND(total * 0.08, 2)
WHERE sales_rep = 'Sarah' AND total > 500;


-- ============================================================================
-- EXPLORE: See the Effect of the Commission Override
-- ============================================================================
-- Compare Sarah's actual commission against both the standard 5% and the
-- bonus 8% rate. High-value orders should now show the bonus tier.

ASSERT ROW_COUNT = 11
SELECT id, item, sales_rep, total, commission,
       ROUND(total * 0.05, 2) AS standard_5pct,
       ROUND(total * 0.08, 2) AS bonus_8pct,
       CASE
           WHEN ABS(commission - ROUND(total * 0.08, 2)) < 0.01 THEN 'Bonus (8%)'
           WHEN ABS(commission - ROUND(total * 0.05, 2)) < 0.01 THEN 'Standard (5%)'
           ELSE 'Unknown'
       END AS commission_tier
FROM {{zone_name}}.delta_demos.sales_invoices
WHERE sales_rep = 'Sarah'
ORDER BY total DESC;


-- ============================================================================
-- EXPLORE: Revenue and Commission by Sales Rep
-- ============================================================================
-- Aggregating computed columns lets us see how the formula chain rolls up.
-- Sarah's commission total will be higher per dollar of revenue because her
-- high-value orders earn 8% instead of 5%.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 14739.5 WHERE sales_rep = 'Sarah'
ASSERT VALUE total_commission = 1141.67 WHERE sales_rep = 'Sarah'
ASSERT VALUE effective_commission_pct = 7.75 WHERE sales_rep = 'Sarah'
SELECT sales_rep,
       COUNT(*) AS invoice_count,
       ROUND(SUM(total), 2) AS total_revenue,
       ROUND(SUM(commission), 2) AS total_commission,
       ROUND(SUM(commission) / SUM(total) * 100, 2) AS effective_commission_pct
FROM {{zone_name}}.delta_demos.sales_invoices
GROUP BY sales_rep
ORDER BY total_revenue DESC;


-- ============================================================================
-- EXPLORE: Discount Tier Analysis
-- ============================================================================
-- The demo inserts invoices across multiple discount tiers. Bulk orders
-- (ids 41-50) use higher discount percentages (15-25%), showing how the
-- same CTE formula handles different discount levels consistently.

ASSERT ROW_COUNT = 4
ASSERT VALUE invoice_count = 10 WHERE discount_tier = 'Bulk (15-25%)'
ASSERT VALUE total_discounted = 7179.05 WHERE discount_tier = 'Bulk (15-25%)'
ASSERT VALUE avg_discount_pct = 19.0 WHERE discount_tier = 'Bulk (15-25%)'
SELECT
    CASE
        WHEN discount_pct = 0 THEN 'No Discount'
        WHEN discount_pct <= 5 THEN 'Standard (5%)'
        WHEN discount_pct <= 10 THEN 'Volume (10%)'
        ELSE 'Bulk (15-25%)'
    END AS discount_tier,
    COUNT(*) AS invoice_count,
    ROUND(SUM(discount_amt), 2) AS total_discounted,
    ROUND(AVG(discount_pct), 1) AS avg_discount_pct
FROM {{zone_name}}.delta_demos.sales_invoices
GROUP BY
    CASE
        WHEN discount_pct = 0 THEN 'No Discount'
        WHEN discount_pct <= 5 THEN 'Standard (5%)'
        WHEN discount_pct <= 10 THEN 'Volume (10%)'
        ELSE 'Bulk (15-25%)'
    END
ORDER BY avg_discount_pct;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.sales_invoices;

-- Verify subtotal formula consistency
ASSERT VALUE subtotal_mismatches = 0
SELECT COUNT(*) FILTER (WHERE ABS(subtotal - ROUND(qty * unit_price, 2)) > 0.01) AS subtotal_mismatches
FROM {{zone_name}}.delta_demos.sales_invoices;

-- Verify discount formula consistency
ASSERT VALUE discount_mismatches = 0
SELECT COUNT(*) FILTER (WHERE ABS(discount_amt - ROUND(subtotal * discount_pct / 100.0, 2)) > 0.01) AS discount_mismatches
FROM {{zone_name}}.delta_demos.sales_invoices;

-- Verify total formula consistency
ASSERT VALUE total_mismatches = 0
SELECT COUNT(*) FILTER (WHERE ABS(total - (subtotal - discount_amt)) > 0.01) AS total_mismatches
FROM {{zone_name}}.delta_demos.sales_invoices;

-- Verify Laptop Pro total
ASSERT VALUE laptop_pro_total = 1899.98
SELECT ROUND(total, 2) AS laptop_pro_total FROM {{zone_name}}.delta_demos.sales_invoices WHERE id = 1;

-- Verify Sarah's bonus commission (8% on orders > $500)
ASSERT VALUE sarah_commission_errors = 0
SELECT COUNT(*) AS sarah_commission_errors FROM {{zone_name}}.delta_demos.sales_invoices
WHERE sales_rep = 'Sarah' AND total > 500
  AND ABS(commission - ROUND(total * 0.08, 2)) > 0.01;

-- Verify standard commission (5% for non-Sarah or <= $500)
ASSERT VALUE standard_commission_errors = 0
SELECT COUNT(*) AS standard_commission_errors FROM {{zone_name}}.delta_demos.sales_invoices
WHERE NOT (sales_rep = 'Sarah' AND total > 500)
  AND ABS(commission - ROUND(total * 0.05, 2)) > 0.01;
