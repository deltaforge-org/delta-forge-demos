-- ============================================================================
-- Iceberg UniForm MERGE INTO (CDC Upsert) — Queries
-- ============================================================================
-- HOW UNIFORM WORKS WITH MERGE
-- -----------------------------
-- All queries below read through the Delta transaction log. The Iceberg
-- metadata in metadata/ is generated automatically as a shadow.
--
-- Each MERGE INTO operation creates:
--   1. A new Delta version in _delta_log/  (what these queries read)
--   2. A new Iceberg snapshot in metadata/ (for external Iceberg engines)
--
-- MERGE is the most complex DML operation for UniForm because a single
-- statement can INSERT, UPDATE, and DELETE rows simultaneously. The
-- resulting Iceberg snapshot must correctly reflect all three mutation
-- types in its manifest entries.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify each Iceberg snapshot with:
--   python3 verify_iceberg_metadata.py <table_data_path>/order_fulfillment -v
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline State (Version 1 / Snapshot 1)
-- ============================================================================
-- 30 orders seeded across 3 regions with mixed statuses.

ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.iceberg_demos.order_fulfillment ORDER BY order_id;


-- ============================================================================
-- Query 1: Per-Region Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 10 WHERE region = 'eu-west'
ASSERT VALUE order_count = 10 WHERE region = 'us-east'
ASSERT VALUE order_count = 10 WHERE region = 'us-west'
ASSERT VALUE total_revenue = 2190.90 WHERE region = 'eu-west'
ASSERT VALUE total_revenue = 1472.43 WHERE region = 'us-east'
ASSERT VALUE total_revenue = 1709.87 WHERE region = 'us-west'
SELECT
    region,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.order_fulfillment
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 2: Per-Status Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 9 WHERE status = 'delivered'
ASSERT VALUE order_count = 12 WHERE status = 'pending'
ASSERT VALUE order_count = 9 WHERE status = 'shipped'
SELECT
    status,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS status_revenue
FROM {{zone_name}}.iceberg_demos.order_fulfillment
GROUP BY status
ORDER BY status;


-- ============================================================================
-- LEARN: MERGE 1 — CDC Upsert (Version 2 / Snapshot 2)
-- ============================================================================
-- Daily sync from source system: 15 rows total.
--   - 10 match existing order_ids  -> UPDATE status and order_date
--   - 5 are new orders             -> INSERT
-- Orders 1,4,7,10 move pending->shipped; 2,5,8 move shipped->delivered;
-- 11,14,17 move pending->shipped. Five new orders (31-35) are inserted.

MERGE INTO {{zone_name}}.iceberg_demos.order_fulfillment t
USING (VALUES
    (1,  'alice@example.com',   'SKU-1001', 2,  29.99,  'shipped',   'us-east', '2024-02-10'),
    (4,  'dave@example.com',    'SKU-1004', 3,  89.99,  'shipped',   'us-east', '2024-02-10'),
    (7,  'grace@example.com',   'SKU-1007', 2,  75.00,  'shipped',   'us-east', '2024-02-10'),
    (10, 'jack@example.com',    'SKU-1010', 3,  45.00,  'shipped',   'us-east', '2024-02-10'),
    (2,  'bob@example.com',     'SKU-1002', 1,  49.99,  'delivered', 'us-east', '2024-02-10'),
    (5,  'eve@example.com',     'SKU-1005', 1,  199.99, 'delivered', 'us-east', '2024-02-10'),
    (8,  'hank@example.com',    'SKU-1008', 6,  22.50,  'delivered', 'us-east', '2024-02-10'),
    (11, 'karen@example.com',   'SKU-2001', 2,  59.99,  'shipped',   'us-west', '2024-02-10'),
    (14, 'nick@example.com',    'SKU-2004', 2,  67.50,  'shipped',   'us-west', '2024-02-10'),
    (17, 'quinn@example.com',   'SKU-2007', 5,  18.00,  'shipped',   'us-west', '2024-02-10'),
    (31, 'liam@example.com',    'SKU-4001', 2,  79.99,  'pending',   'us-east', '2024-02-11'),
    (32, 'nina@example.com',    'SKU-4002', 1,  149.99, 'pending',   'us-west', '2024-02-11'),
    (33, 'oscar@example.com',   'SKU-4003', 3,  55.00,  'pending',   'eu-west', '2024-02-11'),
    (34, 'petra@example.com',   'SKU-4004', 4,  32.50,  'pending',   'us-east', '2024-02-11'),
    (35, 'roger@example.com',   'SKU-4005', 2,  210.00, 'pending',   'eu-west', '2024-02-11')
) AS s(order_id, customer_email, product_sku, quantity, unit_price, status, region, order_date)
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET t.status = s.status, t.order_date = s.order_date
WHEN NOT MATCHED THEN INSERT (order_id, customer_email, product_sku, quantity, unit_price, status, region, order_date)
    VALUES (s.order_id, s.customer_email, s.product_sku, s.quantity, s.unit_price, s.status, s.region, s.order_date);


-- ============================================================================
-- Query 3: Post-MERGE 1 Row Count
-- ============================================================================

ASSERT ROW_COUNT = 35
SELECT * FROM {{zone_name}}.iceberg_demos.order_fulfillment ORDER BY order_id;


-- ============================================================================
-- Query 4: Post-MERGE 1 — Verify Updated Statuses
-- ============================================================================
-- Orders that were pending should now be shipped; shipped should be delivered.

ASSERT ROW_COUNT = 10
ASSERT VALUE status = 'shipped' WHERE order_id = 1
ASSERT VALUE status = 'shipped' WHERE order_id = 4
ASSERT VALUE status = 'shipped' WHERE order_id = 7
ASSERT VALUE status = 'shipped' WHERE order_id = 10
ASSERT VALUE status = 'delivered' WHERE order_id = 2
ASSERT VALUE status = 'delivered' WHERE order_id = 5
ASSERT VALUE status = 'delivered' WHERE order_id = 8
ASSERT VALUE status = 'shipped' WHERE order_id = 11
ASSERT VALUE status = 'shipped' WHERE order_id = 14
ASSERT VALUE status = 'shipped' WHERE order_id = 17
SELECT
    order_id,
    customer_email,
    status,
    order_date
FROM {{zone_name}}.iceberg_demos.order_fulfillment
WHERE order_id IN (1, 2, 4, 5, 7, 8, 10, 11, 14, 17)
ORDER BY order_id;


-- ============================================================================
-- Query 5: Post-MERGE 1 — Per-Region Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 12 WHERE region = 'eu-west'
ASSERT VALUE order_count = 12 WHERE region = 'us-east'
ASSERT VALUE order_count = 11 WHERE region = 'us-west'
SELECT
    region,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.order_fulfillment
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 6: Post-MERGE 1 — Per-Status Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 12 WHERE status = 'delivered'
ASSERT VALUE order_count = 10 WHERE status = 'pending'
ASSERT VALUE order_count = 13 WHERE status = 'shipped'
SELECT
    status,
    COUNT(*) AS order_count
FROM {{zone_name}}.iceberg_demos.order_fulfillment
GROUP BY status
ORDER BY status;


-- ============================================================================
-- LEARN: MERGE 2 — CDC with Cancellations (Version 3 / Snapshot 3)
-- ============================================================================
-- Second daily sync: 10 rows.
--   - 3 match with status='cancelled'  -> DELETE (orders 3, 6, 9)
--   - 3 match with status updates      -> UPDATE (orders 21, 24, 27: pending->shipped)
--   - 4 are new orders                 -> INSERT (orders 36-39)

MERGE INTO {{zone_name}}.iceberg_demos.order_fulfillment t
USING (VALUES
    (3,  'carol@example.com',   'SKU-1003', 5,  12.50,  'cancelled', 'us-east', '2024-03-01'),
    (6,  'frank@example.com',   'SKU-1006', 4,  15.00,  'cancelled', 'us-east', '2024-03-01'),
    (9,  'iris@example.com',    'SKU-1009', 1,  350.00, 'cancelled', 'us-east', '2024-03-01'),
    (21, 'uma@example.com',     'SKU-3001', 2,  55.00,  'shipped',   'eu-west', '2024-03-01'),
    (24, 'xavier@example.com',  'SKU-3004', 4,  62.00,  'shipped',   'eu-west', '2024-03-01'),
    (27, 'amy@example.com',     'SKU-3007', 6,  14.99,  'shipped',   'eu-west', '2024-03-01'),
    (36, 'sara@example.com',    'SKU-5001', 1,  399.99, 'pending',   'us-west', '2024-03-02'),
    (37, 'tom@example.com',     'SKU-5002', 2,  65.00,  'pending',   'eu-west', '2024-03-02'),
    (38, 'ursula@example.com',  'SKU-5003', 3,  88.00,  'pending',   'us-east', '2024-03-02'),
    (39, 'vince@example.com',   'SKU-5004', 5,  27.50,  'pending',   'us-west', '2024-03-02')
) AS s(order_id, customer_email, product_sku, quantity, unit_price, status, region, order_date)
ON t.order_id = s.order_id
WHEN MATCHED AND s.status = 'cancelled' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.status = s.status, t.order_date = s.order_date
WHEN NOT MATCHED THEN INSERT (order_id, customer_email, product_sku, quantity, unit_price, status, region, order_date)
    VALUES (s.order_id, s.customer_email, s.product_sku, s.quantity, s.unit_price, s.status, s.region, s.order_date);


-- ============================================================================
-- Query 7: Post-MERGE 2 Row Count
-- ============================================================================
-- 35 - 3 deleted + 4 inserted = 36

ASSERT ROW_COUNT = 36
SELECT * FROM {{zone_name}}.iceberg_demos.order_fulfillment ORDER BY order_id;


-- ============================================================================
-- Query 8: Post-MERGE 2 — Cancelled Orders Removed
-- ============================================================================
-- Orders 3, 6, 9 should no longer exist.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.order_fulfillment
WHERE order_id IN (3, 6, 9);


-- ============================================================================
-- Query 9: Post-MERGE 2 — Per-Region Counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 13 WHERE region = 'eu-west'
ASSERT VALUE order_count = 10 WHERE region = 'us-east'
ASSERT VALUE order_count = 13 WHERE region = 'us-west'
ASSERT VALUE total_revenue = 2905.90 WHERE region = 'eu-west'
ASSERT VALUE total_revenue = 1553.91 WHERE region = 'us-east'
ASSERT VALUE total_revenue = 2397.35 WHERE region = 'us-west'
SELECT
    region,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.order_fulfillment
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Query 10: Post-MERGE 2 — Per-Status Breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 9 WHERE status = 'delivered'
ASSERT VALUE order_count = 11 WHERE status = 'pending'
ASSERT VALUE order_count = 16 WHERE status = 'shipped'
SELECT
    status,
    COUNT(*) AS order_count
FROM {{zone_name}}.iceberg_demos.order_fulfillment
GROUP BY status
ORDER BY status;


-- ============================================================================
-- Query 11: Time Travel — Row Counts Across All Versions
-- ============================================================================
-- V1: 30 (seed), V2: 35 (merge 1), V3: 36 (merge 2 with deletes)

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_count = 30
ASSERT VALUE v2_count = 35
ASSERT VALUE v3_count = 36
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.order_fulfillment VERSION AS OF 1) AS v1_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.order_fulfillment VERSION AS OF 2) AS v2_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.order_fulfillment) AS v3_count;


-- ============================================================================
-- Query 12: Time Travel — Revenue Comparison Across Versions
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_revenue = 5373.20
ASSERT VALUE v2_revenue = 6398.17
ASSERT VALUE v3_revenue = 6857.16
SELECT
    ROUND((SELECT SUM(quantity * unit_price) FROM {{zone_name}}.iceberg_demos.order_fulfillment VERSION AS OF 1), 2) AS v1_revenue,
    ROUND((SELECT SUM(quantity * unit_price) FROM {{zone_name}}.iceberg_demos.order_fulfillment VERSION AS OF 2), 2) AS v2_revenue,
    ROUND(SUM(quantity * unit_price), 2) AS v3_revenue
FROM {{zone_name}}.iceberg_demos.order_fulfillment;


-- ============================================================================
-- Query 13: Version History
-- ============================================================================
-- V1: Initial seed (30 rows)
-- V2: MERGE upsert (10 updates + 5 inserts)
-- V3: MERGE with deletes (3 deletes + 3 updates + 4 inserts)

ASSERT WARNING ROW_COUNT >= 3
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.order_fulfillment;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check after both MERGE operations.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 36
ASSERT VALUE pending_count = 11
ASSERT VALUE shipped_count = 16
ASSERT VALUE delivered_count = 9
ASSERT VALUE region_count = 3
ASSERT VALUE total_revenue = 6857.16
SELECT
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE status = 'pending') AS pending_count,
    COUNT(*) FILTER (WHERE status = 'shipped') AS shipped_count,
    COUNT(*) FILTER (WHERE status = 'delivered') AS delivered_count,
    COUNT(DISTINCT region) AS region_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.order_fulfillment;


-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata correctly represents the final state after two MERGE
-- operations (upsert + upsert-with-delete).
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.order_fulfillment_iceberg
USING ICEBERG
LOCATION '{{data_path}}/order_fulfillment';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.order_fulfillment_iceberg TO USER {{current_user}};
DETECT SCHEMA FOR TABLE {{zone_name}}.iceberg_demos.order_fulfillment_iceberg;


-- ============================================================================
-- Iceberg Verify 1: Row Count — 36 Orders After Both MERGEs
-- ============================================================================

ASSERT ROW_COUNT = 36
SELECT * FROM {{zone_name}}.iceberg_demos.order_fulfillment_iceberg ORDER BY order_id;


-- ============================================================================
-- Iceberg Verify 2: Per-Region Counts — Reflect Deletes + Inserts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE order_count = 13 WHERE region = 'eu-west'
ASSERT VALUE order_count = 10 WHERE region = 'us-east'
ASSERT VALUE order_count = 13 WHERE region = 'us-west'
SELECT
    region,
    COUNT(*) AS order_count
FROM {{zone_name}}.iceberg_demos.order_fulfillment_iceberg
GROUP BY region
ORDER BY region;


-- ============================================================================
-- Iceberg Verify 3: Grand Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 36
ASSERT VALUE total_revenue = 6857.16
ASSERT VALUE region_count = 3
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
    COUNT(DISTINCT region) AS region_count
FROM {{zone_name}}.iceberg_demos.order_fulfillment_iceberg;
