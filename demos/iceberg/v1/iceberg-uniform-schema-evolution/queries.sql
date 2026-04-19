-- ============================================================================
-- Iceberg UniForm Schema Evolution — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH SCHEMA EVOLUTION
-- ----------------------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- When ALTER TABLE ADD COLUMN runs, Delta Forge:
--   1. Updates the Delta schema in _delta_log/ (what these queries read)
--   2. Adds a new schema entry to metadata.json's "schemas" array
--      (Iceberg V2/V3 track multiple schema versions)
--
-- Older Iceberg snapshots reference the original schema; newer snapshots
-- reference the evolved schema. This means an Iceberg engine reading an
-- old snapshot will see the original columns, while the latest snapshot
-- includes the new columns — standard Iceberg schema evolution semantics.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify schema evolution in metadata with:
--   python3 verify_iceberg_metadata.py <table_data_path>/customer_orders -v
-- The --verbose flag shows the schema versions in the Iceberg metadata.
-- ============================================================================
-- ============================================================================
-- EXPLORE: Baseline Schema — 6 Columns (Version 1)
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.customer_orders ORDER BY id;
-- ============================================================================
-- Query 1: Baseline Aggregation — Per-Customer Revenue
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE total_revenue = 1435.00 WHERE customer_name = 'Acme Corp'
ASSERT VALUE total_revenue = 2275.00 WHERE customer_name = 'TechStart Inc'
ASSERT VALUE total_revenue = 2675.00 WHERE customer_name = 'Global Foods'
ASSERT VALUE total_revenue = 2710.00 WHERE customer_name = 'DataFlow LLC'
SELECT
    customer_name,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.customer_orders
GROUP BY customer_name
ORDER BY customer_name;
-- ============================================================================
-- LEARN: Schema Evolution Step 1 — Add loyalty_tier (Version 2)
-- ============================================================================
-- ADD COLUMN is a metadata-only operation. The Iceberg metadata.json gets
-- a new schema entry with schema-id incremented. Existing data files
-- return NULL for the new column (standard Iceberg schema evolution).

ALTER TABLE {{zone_name}}.iceberg_demos.customer_orders ADD COLUMN loyalty_tier VARCHAR;
-- ============================================================================
-- Query 2: Verify NULL Backfill on New Column
-- ============================================================================
-- All 20 existing rows should have loyalty_tier = NULL.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 20
ASSERT VALUE has_tier = 0
ASSERT VALUE missing_tier = 20
SELECT
    COUNT(*) AS total_rows,
    COUNT(loyalty_tier) AS has_tier,
    COUNT(*) - COUNT(loyalty_tier) AS missing_tier
FROM {{zone_name}}.iceberg_demos.customer_orders;
-- ============================================================================
-- LEARN: Backfill loyalty_tier (Version 3)
-- ============================================================================
-- Assign tiers based on cumulative spend. This UPDATE produces a new
-- Delta version and Iceberg snapshot, both using the evolved schema.

UPDATE {{zone_name}}.iceberg_demos.customer_orders
SET loyalty_tier = CASE
    WHEN customer_name = 'DataFlow LLC' THEN 'Platinum'
    WHEN customer_name = 'Global Foods' THEN 'Platinum'
    WHEN customer_name = 'TechStart Inc' THEN 'Gold'
    WHEN customer_name = 'Acme Corp' THEN 'Silver'
    ELSE 'Bronze'
END;
-- ============================================================================
-- Query 3: Verify Tier Assignment
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE customer_count = 2 WHERE loyalty_tier = 'Platinum'
ASSERT VALUE customer_count = 1 WHERE loyalty_tier = 'Gold'
ASSERT VALUE customer_count = 1 WHERE loyalty_tier = 'Silver'
SELECT
    loyalty_tier,
    COUNT(DISTINCT customer_name) AS customer_count,
    ROUND(SUM(quantity * unit_price), 2) AS tier_revenue
FROM {{zone_name}}.iceberg_demos.customer_orders
GROUP BY loyalty_tier
ORDER BY loyalty_tier;
-- ============================================================================
-- LEARN: Schema Evolution Step 2 — Add discount_pct and notes (Version 4)
-- ============================================================================
-- Two columns added in one ALTER. The Iceberg schema-id increments again.

ALTER TABLE {{zone_name}}.iceberg_demos.customer_orders ADD COLUMN discount_pct DOUBLE;
ALTER TABLE {{zone_name}}.iceberg_demos.customer_orders ADD COLUMN notes VARCHAR;
-- ============================================================================
-- LEARN: Populate New Columns for Platinum Customers (Version 6)
-- ============================================================================
-- Platinum customers get a 10% discount. Other tiers get 5%.

UPDATE {{zone_name}}.iceberg_demos.customer_orders
SET discount_pct = CASE
    WHEN loyalty_tier = 'Platinum' THEN 10.0
    WHEN loyalty_tier = 'Gold' THEN 7.5
    ELSE 5.0
END,
notes = CASE
    WHEN loyalty_tier = 'Platinum' THEN 'VIP discount applied'
    WHEN loyalty_tier = 'Gold' THEN 'Preferred customer discount'
    ELSE 'Standard discount'
END;
-- ============================================================================
-- Query 4: Discounted Revenue by Customer
-- ============================================================================
-- Revenue after applying discount_pct.

ASSERT ROW_COUNT = 4
ASSERT VALUE discounted_revenue = 1363.25 WHERE customer_name = 'Acme Corp'
ASSERT VALUE discounted_revenue = 2104.38 WHERE customer_name = 'TechStart Inc'
ASSERT VALUE discounted_revenue = 2407.50 WHERE customer_name = 'Global Foods'
ASSERT VALUE discounted_revenue = 2439.00 WHERE customer_name = 'DataFlow LLC'
SELECT
    customer_name,
    loyalty_tier,
    ROUND(SUM(quantity * unit_price), 2) AS gross_revenue,
    ROUND(SUM(quantity * unit_price * (1 - discount_pct / 100)), 2) AS discounted_revenue
FROM {{zone_name}}.iceberg_demos.customer_orders
GROUP BY customer_name, loyalty_tier
ORDER BY customer_name;
-- ============================================================================
-- Query 5: New Inserts With Full Schema (Version 7)
-- ============================================================================
-- Orders inserted after schema evolution have all 9 columns populated.

INSERT INTO {{zone_name}}.iceberg_demos.customer_orders
SELECT * FROM (VALUES
    (21, 'Acme Corp',     'Gadget Z',  5,  150.00, '2024-04-01', 'Silver',   5.0,  'Standard discount'),
    (22, 'TechStart Inc', 'Gadget Z',  8,  150.00, '2024-04-05', 'Gold',     7.5,  'Preferred customer discount'),
    (23, 'Global Foods',  'Gadget Z',  10, 150.00, '2024-04-10', 'Platinum', 10.0, 'VIP discount applied'),
    (24, 'DataFlow LLC',  'Gadget Z',  6,  150.00, '2024-04-15', 'Platinum', 10.0, 'VIP discount applied')
) AS t(id, customer_name, product, quantity, unit_price, order_date, loyalty_tier, discount_pct, notes);
-- ============================================================================
-- Query 6: Three Row Groups — Schema Evolution Shape
-- ============================================================================
-- Group 1 (ids 1-20 at V1):  Had 6 columns, then evolved to 9 via updates
-- Group 2 (ids 21-24):       Inserted with full 9-column schema
-- All rows should now have all 9 columns populated.

ASSERT ROW_COUNT = 2
ASSERT VALUE row_count = 20 WHERE row_group = 'Original rows (evolved)'
ASSERT VALUE has_tier = 20 WHERE row_group = 'Original rows (evolved)'
ASSERT VALUE has_discount = 20 WHERE row_group = 'Original rows (evolved)'
ASSERT VALUE has_notes = 20 WHERE row_group = 'Original rows (evolved)'
ASSERT VALUE row_count = 4 WHERE row_group = 'Post-evolution inserts'
ASSERT VALUE has_tier = 4 WHERE row_group = 'Post-evolution inserts'
ASSERT VALUE has_discount = 4 WHERE row_group = 'Post-evolution inserts'
ASSERT VALUE has_notes = 4 WHERE row_group = 'Post-evolution inserts'
SELECT
    CASE
        WHEN id <= 20 THEN 'Original rows (evolved)'
        ELSE 'Post-evolution inserts'
    END AS row_group,
    COUNT(*) AS row_count,
    COUNT(loyalty_tier) AS has_tier,
    COUNT(discount_pct) AS has_discount,
    COUNT(notes) AS has_notes
FROM {{zone_name}}.iceberg_demos.customer_orders
GROUP BY row_group
ORDER BY row_group;
-- ============================================================================
-- Query 7: Time Travel — Read Version 1 (Original Schema)
-- ============================================================================
-- Reading the pre-evolution version should return only the original 6 columns.
-- The loyalty_tier, discount_pct, and notes columns did not exist yet.

ASSERT ROW_COUNT = 20
SELECT
    id, customer_name, product, quantity, unit_price, order_date
FROM {{zone_name}}.iceberg_demos.customer_orders VERSION AS OF 1
ORDER BY id;
-- ============================================================================
-- Query 8: Version History — Schema Evolution Trail
-- ============================================================================
-- The history shows the progression of schema changes and data mutations.

ASSERT WARNING ROW_COUNT >= 7
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.customer_orders;
-- ============================================================================
-- Query 9: Grand Total Revenue (All 24 Orders)
-- ============================================================================

-- Non-deterministic: discounted_total sum lands on exact 12296.625 midpoint;
-- float ROUND rule (half-even vs half-up) may produce 12296.62 or 12296.63.
ASSERT ROW_COUNT = 1
ASSERT VALUE gross_total = 13445.00
ASSERT WARNING VALUE discounted_total BETWEEN 12296.60 AND 12296.65
SELECT
    ROUND(SUM(quantity * unit_price), 2) AS gross_total,
    ROUND(SUM(quantity * unit_price * (1 - discount_pct / 100)), 2) AS discounted_total
FROM {{zone_name}}.iceberg_demos.customer_orders;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Comprehensive validation of the final evolved state.

-- Non-deterministic: discounted_revenue sum lands on exact 12296.625 midpoint;
-- float ROUND rule (half-even vs half-up) may produce 12296.62 or 12296.63.
ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 24
ASSERT VALUE total_columns_populated = 24
ASSERT VALUE distinct_tiers = 3
ASSERT VALUE gross_revenue = 13445.00
ASSERT WARNING VALUE discounted_revenue BETWEEN 12296.60 AND 12296.65
ASSERT VALUE platinum_orders = 12
SELECT
    COUNT(*) AS total_orders,
    COUNT(loyalty_tier) AS total_columns_populated,
    COUNT(DISTINCT loyalty_tier) AS distinct_tiers,
    ROUND(SUM(quantity * unit_price), 2) AS gross_revenue,
    ROUND(SUM(quantity * unit_price * (1 - discount_pct / 100)), 2) AS discounted_revenue,
    COUNT(*) FILTER (WHERE loyalty_tier = 'Platinum') AS platinum_orders
FROM {{zone_name}}.iceberg_demos.customer_orders;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents the evolved 9-column schema
-- (original 6 columns + loyalty_tier, discount_pct, notes).
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.customer_orders_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.customer_orders_iceberg
USING ICEBERG
LOCATION '{{data_subdir}}/customer_orders';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.customer_orders_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Verify 1: Row Count — 24 Orders (20 Original + 4 Post-Evolution)
-- ============================================================================

ASSERT ROW_COUNT = 24
SELECT * FROM {{zone_name}}.iceberg_demos.customer_orders_iceberg ORDER BY id;
-- ============================================================================
-- Iceberg Verify 2: Evolved Columns Are Populated
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 24
ASSERT VALUE has_tier = 24
ASSERT VALUE has_discount = 24
ASSERT VALUE has_notes = 24
SELECT
    COUNT(*) AS total_orders,
    COUNT(loyalty_tier) AS has_tier,
    COUNT(discount_pct) AS has_discount,
    COUNT(notes) AS has_notes
FROM {{zone_name}}.iceberg_demos.customer_orders_iceberg;
-- ============================================================================
-- Iceberg Verify 3: Revenue Totals — Must Match Delta Final State
-- ============================================================================

-- Non-deterministic: discounted_revenue sum lands on exact 12296.625 midpoint;
-- float ROUND rule (half-even vs half-up) may produce 12296.62 or 12296.63.
ASSERT ROW_COUNT = 1
ASSERT VALUE gross_revenue = 13445.00
ASSERT WARNING VALUE discounted_revenue BETWEEN 12296.60 AND 12296.65
SELECT
    ROUND(SUM(quantity * unit_price), 2) AS gross_revenue,
    ROUND(SUM(quantity * unit_price * (1 - discount_pct / 100)), 2) AS discounted_revenue
FROM {{zone_name}}.iceberg_demos.customer_orders_iceberg;
