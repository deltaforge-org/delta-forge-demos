-- ============================================================================
-- Iceberg UniForm Partitioned — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH PARTITIONS
-- -----------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- When a Delta table is PARTITIONED BY (region), the Iceberg metadata
-- includes a matching partition spec. This means:
--   - Delta reads use Delta's own partition pruning
--   - External Iceberg engines use the Iceberg partition spec for pruning
--   - Both see the same directory layout: region=us-east/, region=us-west/, etc.
--
-- Cross-partition operations (UPDATE all Q4 rows) rewrite data files in
-- every affected partition. The Iceberg snapshot tracks which manifests
-- changed, so Iceberg engines only re-read affected partitions.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running, verify partition specs in the Iceberg metadata:
--   python3 verify_iceberg_metadata.py <table_data_path>/regional_sales -v
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline — All 24 Transactions
-- ============================================================================

ASSERT ROW_COUNT = 24
SELECT * FROM {{zone_name}}.iceberg_demos.regional_sales ORDER BY id;


-- ============================================================================
-- Query 1: Per-Region Summary
-- ============================================================================
-- Each region has 8 transactions. Iceberg partition pruning can skip
-- entire data files for single-region queries.

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 8 WHERE region = 'us-east'
ASSERT VALUE txn_count = 8 WHERE region = 'us-west'
ASSERT VALUE txn_count = 8 WHERE region = 'eu-west'
ASSERT VALUE total_amount = 8460.00 WHERE region = 'us-east'
ASSERT VALUE total_amount = 8730.00 WHERE region = 'us-west'
ASSERT VALUE total_amount = 8790.00 WHERE region = 'eu-west'
SELECT
    region,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount,
    SUM(quantity) AS total_qty
FROM {{zone_name}}.iceberg_demos.regional_sales
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 2: Per-Quarter Summary
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE total_amount = 6480.00 WHERE quarter = 'Q1-2024'
ASSERT VALUE total_amount = 6050.00 WHERE quarter = 'Q2-2024'
ASSERT VALUE total_amount = 7020.00 WHERE quarter = 'Q3-2024'
ASSERT VALUE total_amount = 6430.00 WHERE quarter = 'Q4-2024'
SELECT
    quarter,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount,
    SUM(quantity) AS total_qty
FROM {{zone_name}}.iceberg_demos.regional_sales
GROUP BY quarter
ORDER BY quarter;


-- ============================================================================
-- Query 3: Product Revenue by Region
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE revenue = 5400.00 WHERE region = 'us-east' AND product = 'Widget Pro'
ASSERT VALUE revenue = 3060.00 WHERE region = 'us-east' AND product = 'Gadget Max'
ASSERT VALUE revenue = 3800.00 WHERE region = 'us-west' AND product = 'Widget Pro'
ASSERT VALUE revenue = 4930.00 WHERE region = 'us-west' AND product = 'Gadget Max'
ASSERT VALUE revenue = 4200.00 WHERE region = 'eu-west' AND product = 'Widget Pro'
ASSERT VALUE revenue = 4590.00 WHERE region = 'eu-west' AND product = 'Gadget Max'
SELECT
    region,
    product,
    ROUND(SUM(amount), 2) AS revenue,
    SUM(quantity) AS units_sold
FROM {{zone_name}}.iceberg_demos.regional_sales
GROUP BY region, product
ORDER BY region, product;


-- ============================================================================
-- Query 4: Single-Partition Read — us-east Only
-- ============================================================================
-- With partition pruning, Iceberg engines only read us-east data files.

ASSERT ROW_COUNT = 8
SELECT
    id, product, quarter, amount, quantity, sales_rep
FROM {{zone_name}}.iceberg_demos.regional_sales
WHERE region = 'us-east'
ORDER BY id;


-- ============================================================================
-- Query 5: Top Sales Rep by Region
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE top_rep = 'Alice' WHERE region = 'us-east'
ASSERT VALUE top_rep = 'Carol' WHERE region = 'us-west'
ASSERT VALUE top_rep = 'Frank' WHERE region = 'eu-west'
SELECT
    region,
    sales_rep AS top_rep,
    ROUND(SUM(amount), 2) AS rep_revenue
FROM {{zone_name}}.iceberg_demos.regional_sales
GROUP BY region, sales_rep
HAVING SUM(amount) = (
    SELECT MAX(rep_total) FROM (
        SELECT SUM(amount) AS rep_total
        FROM {{zone_name}}.iceberg_demos.regional_sales s2
        WHERE s2.region = {{zone_name}}.iceberg_demos.regional_sales.region
        GROUP BY s2.sales_rep
    )
)
ORDER BY region;


-- ============================================================================
-- LEARN: Cross-Partition UPDATE (Version 2 / Snapshot 2)
-- ============================================================================
-- Apply a 5% Q4 year-end bonus to all Q4 transactions across all regions.
-- This touches data files in every partition.

UPDATE {{zone_name}}.iceberg_demos.regional_sales
SET amount = ROUND(amount * 1.05, 2)
WHERE quarter = 'Q4-2024';


-- ============================================================================
-- Query 6: Post-Update Q4 Amounts
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE new_amount = 1890.00 WHERE id = 7
ASSERT VALUE new_amount = 535.50 WHERE id = 8
ASSERT VALUE new_amount = 630.00 WHERE id = 15
ASSERT VALUE new_amount = 1606.50 WHERE id = 16
ASSERT VALUE new_amount = 840.00 WHERE id = 23
ASSERT VALUE new_amount = 1249.50 WHERE id = 24
SELECT
    id,
    region,
    product,
    ROUND(amount, 2) AS new_amount
FROM {{zone_name}}.iceberg_demos.regional_sales
WHERE quarter = 'Q4-2024'
ORDER BY id;


-- ============================================================================
-- Query 7: Time Travel — Pre-Bonus vs Post-Bonus Totals
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE pre_bonus_total = 25980.00
ASSERT VALUE post_bonus_total = 26301.50
SELECT
    ROUND((SELECT SUM(amount) FROM {{zone_name}}.iceberg_demos.regional_sales VERSION AS OF 1), 2) AS pre_bonus_total,
    ROUND(SUM(amount), 2) AS post_bonus_total
FROM {{zone_name}}.iceberg_demos.regional_sales;


-- ============================================================================
-- LEARN: Partition-Scoped DELETE (Version 3 / Snapshot 3)
-- ============================================================================
-- Remove low-performing transactions (amount < 700) from eu-west only.
-- Only the eu-west partition's data files are rewritten.

DELETE FROM {{zone_name}}.iceberg_demos.regional_sales
WHERE region = 'eu-west' AND amount < 700;


-- ============================================================================
-- Query 8: Post-Delete Region Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 8 WHERE region = 'us-east'
ASSERT VALUE txn_count = 8 WHERE region = 'us-west'
ASSERT VALUE txn_count = 7 WHERE region = 'eu-west'
SELECT
    region,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.iceberg_demos.regional_sales
GROUP BY region
ORDER BY region;


-- ============================================================================
-- LEARN: INSERT Into Specific Partition (Version 4 / Snapshot 4)
-- ============================================================================
-- Add new Q1-2025 transactions. Only the target partitions' manifests update.

INSERT INTO {{zone_name}}.iceberg_demos.regional_sales VALUES
    (25, 'Widget Pro',  'us-east', 'Q1-2025', 2100.00, 21, 'Alice'),
    (26, 'Gadget Max',  'us-west', 'Q1-2025', 1800.00, 10, 'Carol'),
    (27, 'Widget Pro',  'eu-west', 'Q1-2025', 1600.00, 16, 'Eve');


-- ============================================================================
-- Query 9: Final Region Summary
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE txn_count = 9 WHERE region = 'us-east'
ASSERT VALUE txn_count = 9 WHERE region = 'us-west'
ASSERT VALUE txn_count = 8 WHERE region = 'eu-west'
SELECT
    region,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount,
    SUM(quantity) AS total_qty
FROM {{zone_name}}.iceberg_demos.regional_sales
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 10: Version History
-- ============================================================================
-- 4 versions: seed, Q4 bonus update, eu-west delete, Q1-2025 insert.

ASSERT WARNING ROW_COUNT >= 4
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.regional_sales;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Grand totals across all partitions after all mutations.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_transactions = 26
ASSERT VALUE total_revenue = 31121.50
ASSERT VALUE region_count = 3
ASSERT VALUE q4_bonus_total = 6751.50
ASSERT VALUE q1_2025_total = 5500.00
SELECT
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_revenue,
    COUNT(DISTINCT region) AS region_count,
    ROUND(SUM(CASE WHEN quarter = 'Q4-2024' THEN amount ELSE 0 END), 2) AS q4_bonus_total,
    ROUND(SUM(CASE WHEN quarter = 'Q1-2025' THEN amount ELSE 0 END), 2) AS q1_2025_total
FROM {{zone_name}}.iceberg_demos.regional_sales;
