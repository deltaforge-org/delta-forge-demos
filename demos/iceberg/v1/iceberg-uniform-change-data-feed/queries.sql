-- ============================================================================
-- Demo: E-Commerce Order Lifecycle — Change Data Feed with UniForm
-- ============================================================================
-- Tests that Delta CDF mutations (INSERT, UPDATE, DELETE) are correctly
-- reflected in the shadow Iceberg metadata. Proves the UniForm post-commit
-- hook maintains metadata consistency through the full mutation lifecycle.

-- ============================================================================
-- Query 1: Baseline — All 30 Orders Present
-- ============================================================================
-- Verify the seed data loaded correctly with all 5 statuses represented.

ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.iceberg_demos.orders ORDER BY order_id;

-- ============================================================================
-- Query 2: Revenue by Status — Baseline Snapshot
-- ============================================================================
-- Establishes the revenue distribution before any mutations.

ASSERT ROW_COUNT = 5
ASSERT VALUE order_count = 10 WHERE status = 'pending'
ASSERT VALUE order_count = 7 WHERE status = 'processing'
ASSERT VALUE order_count = 6 WHERE status = 'shipped'
ASSERT VALUE order_count = 5 WHERE status = 'delivered'
ASSERT VALUE order_count = 2 WHERE status = 'cancelled'
SELECT
    status,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.orders
GROUP BY status
ORDER BY status;

-- ============================================================================
-- Query 3: Revenue by Customer — Baseline
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue = 3889.89 WHERE customer_name = 'Alice Johnson'
ASSERT VALUE total_revenue = 2509.92 WHERE customer_name = 'Bob Chen'
ASSERT VALUE total_revenue = 2169.89 WHERE customer_name = 'Carol Davis'
ASSERT VALUE total_revenue = 2399.88 WHERE customer_name = 'Dan Wilson'
ASSERT VALUE total_revenue = 4659.90 WHERE customer_name = 'Eve Martinez'
SELECT
    customer_name,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.orders
GROUP BY customer_name
ORDER BY customer_name;

-- ============================================================================
-- LEARN: CDF Mutation 1 — UPDATE pending orders to processing
-- ============================================================================
-- Moves the 5 oldest pending orders (id 1-5) to processing status.
-- With CDF enabled, this creates pre-image and post-image records in the
-- change feed that track exactly which rows changed and how.

UPDATE {{zone_name}}.iceberg_demos.orders
SET status = 'processing'
WHERE order_id IN (1, 2, 3, 4, 5);

-- ============================================================================
-- Query 4: Post-Update Status Distribution
-- ============================================================================
-- The 5 pending→processing updates shift the counts.

ASSERT ROW_COUNT = 5
ASSERT VALUE order_count = 5 WHERE status = 'pending'
ASSERT VALUE order_count = 12 WHERE status = 'processing'
ASSERT VALUE order_count = 6 WHERE status = 'shipped'
ASSERT VALUE order_count = 5 WHERE status = 'delivered'
ASSERT VALUE order_count = 2 WHERE status = 'cancelled'
SELECT
    status,
    COUNT(*) AS order_count
FROM {{zone_name}}.iceberg_demos.orders
GROUP BY status
ORDER BY status;

-- ============================================================================
-- Query 5: Spot-Check Updated Row
-- ============================================================================
-- Verify order 1 was updated from pending to processing.

ASSERT ROW_COUNT = 1
ASSERT VALUE status = 'processing' WHERE order_id = 1
ASSERT VALUE customer_name = 'Alice Johnson' WHERE order_id = 1
ASSERT VALUE unit_price = 1299.99 WHERE order_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.orders
WHERE order_id = 1;

-- ============================================================================
-- LEARN: CDF Mutation 2 — DELETE cancelled orders
-- ============================================================================
-- Remove the 2 cancelled orders (id 25, 26). With CDF, the delete event
-- is recorded so downstream consumers know which rows were removed.

DELETE FROM {{zone_name}}.iceberg_demos.orders
WHERE status = 'cancelled';

-- ============================================================================
-- Query 6: Post-Delete Row Count — 28 Orders Remain
-- ============================================================================

ASSERT ROW_COUNT = 28
SELECT * FROM {{zone_name}}.iceberg_demos.orders ORDER BY order_id;

-- ============================================================================
-- Query 7: Post-Delete Status Distribution — No Cancelled
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 5 WHERE status = 'pending'
ASSERT VALUE order_count = 12 WHERE status = 'processing'
ASSERT VALUE order_count = 6 WHERE status = 'shipped'
ASSERT VALUE order_count = 5 WHERE status = 'delivered'
SELECT
    status,
    COUNT(*) AS order_count
FROM {{zone_name}}.iceberg_demos.orders
GROUP BY status
ORDER BY status;

-- ============================================================================
-- Query 8: Final Revenue by Product
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue = 9099.93 WHERE product = 'Laptop Pro'
ASSERT VALUE total_revenue = 2799.93 WHERE product = 'Monitor 27in'
ASSERT VALUE total_revenue = 1349.91 WHERE product = 'Keyboard Mech'
ASSERT VALUE total_revenue = 599.88 WHERE product = 'USB-C Hub'
ASSERT VALUE total_revenue = 419.86 WHERE product = 'Wireless Mouse'
SELECT
    product,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.orders
GROUP BY product
ORDER BY total_revenue DESC;

-- ============================================================================
-- Query 9: Time Travel — Original State (Version 1)
-- ============================================================================
-- Read the table at version 1 (after initial INSERT, before any mutations).
-- All 30 rows should be present with original statuses.

ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.iceberg_demos.orders VERSION AS OF 1 ORDER BY order_id;

-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query through the Iceberg metadata chain. The final state (28 rows,
-- updated statuses, no cancelled) must match the Delta reader exactly.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.orders_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.orders_iceberg
USING ICEBERG
LOCATION '{{data_path}}/orders';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.orders_iceberg TO USER {{current_user}};

-- ============================================================================
-- Iceberg Verify 1: Row Count — 28 Orders After CDF Mutations
-- ============================================================================

ASSERT ROW_COUNT = 28
SELECT * FROM {{zone_name}}.iceberg_demos.orders_iceberg ORDER BY order_id;

-- ============================================================================
-- Iceberg Verify 2: No Cancelled Orders Visible
-- ============================================================================

ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.iceberg_demos.orders_iceberg WHERE status = 'cancelled';

-- ============================================================================
-- Iceberg Verify 3: Updated Row Spot-Check
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE status = 'processing' WHERE order_id = 1
ASSERT VALUE product = 'Laptop Pro' WHERE order_id = 1
SELECT *
FROM {{zone_name}}.iceberg_demos.orders_iceberg
WHERE order_id = 1;

-- ============================================================================
-- Iceberg Verify 4: Revenue by Product — Must Match Delta
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue = 9099.93 WHERE product = 'Laptop Pro'
ASSERT VALUE total_revenue = 2799.93 WHERE product = 'Monitor 27in'
ASSERT VALUE total_revenue = 599.88 WHERE product = 'USB-C Hub'
SELECT
    product,
    COUNT(*) AS order_count,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue
FROM {{zone_name}}.iceberg_demos.orders_iceberg
GROUP BY product
ORDER BY total_revenue DESC;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: final state after INSERT → UPDATE → DELETE.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 28
ASSERT VALUE total_revenue = 14269.51
ASSERT VALUE product_count = 5
ASSERT VALUE customer_count = 5
ASSERT VALUE avg_order_value = 509.63
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(quantity * unit_price), 2) AS total_revenue,
    COUNT(DISTINCT product) AS product_count,
    COUNT(DISTINCT customer_name) AS customer_count,
    ROUND(AVG(quantity * unit_price), 2) AS avg_order_value
FROM {{zone_name}}.iceberg_demos.orders;
