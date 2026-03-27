-- ============================================================================
-- Iceberg V2 Complex Types — Queries
-- ============================================================================
-- Demonstrates native Iceberg format-version 2 table reading with complex
-- nested column types: STRUCT and ARRAY<STRUCT>. Exercises struct field
-- access via dot notation, array explosion via UNNEST, and aggregations
-- over nested data.
-- All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Full Scan
-- ============================================================================
-- Verifies that Delta Forge discovered the Parquet data file via the
-- Iceberg v2 manifest chain and correctly reads all 100 orders including
-- nested STRUCT and ARRAY columns.

ASSERT ROW_COUNT = 100
ASSERT VALUE order_id IS NOT NULL WHERE order_id = 1
ASSERT VALUE order_id IS NOT NULL WHERE order_id = 50
ASSERT VALUE order_id IS NOT NULL WHERE order_id = 100
SELECT * FROM {{zone_name}}.iceberg.orders
ORDER BY order_id;


-- ============================================================================
-- Query 2: Nested Struct Field Access — City Breakdown
-- ============================================================================
-- Accesses fields within the shipping_address STRUCT using dot notation.
-- Proves correct Iceberg → Arrow struct type mapping.

ASSERT ROW_COUNT = 15
ASSERT VALUE order_count = 17 WHERE city = 'Phoenix'
ASSERT VALUE order_count = 9 WHERE city = 'Los Angeles'
ASSERT VALUE order_count = 8 WHERE city = 'New York'
SELECT
    shipping_address.city AS city,
    COUNT(*) AS order_count
FROM {{zone_name}}.iceberg.orders
GROUP BY shipping_address.city
ORDER BY order_count DESC, city;


-- ============================================================================
-- Query 3: Array Explosion — Total Line Items
-- ============================================================================
-- Uses UNNEST to expand the items ARRAY<STRUCT> column, producing one row
-- per order line item. Verifies correct array-of-struct reading.

ASSERT ROW_COUNT = 311
WITH exploded AS (
    SELECT order_id, unnest(items) AS item
    FROM {{zone_name}}.iceberg.orders
)
SELECT
    order_id,
    item['product_name'] AS product_name,
    item['quantity'] AS quantity,
    item['unit_price'] AS unit_price,
    item['quantity'] * item['unit_price'] AS line_total
FROM exploded
ORDER BY order_id, product_name;


-- ============================================================================
-- Query 4: Status Breakdown
-- ============================================================================
-- Groups orders by status with count and average total.

ASSERT ROW_COUNT = 4
ASSERT VALUE order_count = 6 WHERE status = 'Cancelled'
ASSERT VALUE order_count = 43 WHERE status = 'Delivered'
ASSERT VALUE order_count = 15 WHERE status = 'Processing'
ASSERT VALUE order_count = 36 WHERE status = 'Shipped'
ASSERT VALUE avg_total = 1758.66 WHERE status = 'Cancelled'
ASSERT VALUE avg_total = 1039.51 WHERE status = 'Delivered'
ASSERT VALUE avg_total = 1329.27 WHERE status = 'Processing'
ASSERT VALUE avg_total = 1009.94 WHERE status = 'Shipped'
SELECT
    status,
    COUNT(*) AS order_count,
    ROUND(AVG(order_total), 2) AS avg_total
FROM {{zone_name}}.iceberg.orders
GROUP BY status
ORDER BY status;


-- ============================================================================
-- Query 5: Top Products by Quantity
-- ============================================================================
-- After UNNEST, groups by product_name and sums quantities.
-- Exercises nested struct field access within exploded arrays.

ASSERT ROW_COUNT = 10
ASSERT VALUE total_qty = 71 WHERE product_name = 'Desk Lamp'
ASSERT VALUE total_qty = 70 WHERE product_name = 'Keyboard'
ASSERT VALUE total_qty = 50 WHERE product_name = 'Laptop'
WITH exploded AS (
    SELECT unnest(items) AS item
    FROM {{zone_name}}.iceberg.orders
)
SELECT
    item['product_name'] AS product_name,
    SUM(item['quantity']) AS total_qty
FROM exploded
GROUP BY item['product_name']
ORDER BY total_qty DESC;


-- ============================================================================
-- Query 6: State Analysis — Revenue by State
-- ============================================================================
-- Groups by shipping_address.state (nested struct field) with aggregation.

ASSERT ROW_COUNT = 12
ASSERT VALUE total_revenue = 23382.06 WHERE state = 'CA'
ASSERT VALUE total_revenue = 14812.55 WHERE state = 'TX'
ASSERT VALUE total_revenue = 13975.06 WHERE state = 'AZ'
SELECT
    shipping_address.state AS state,
    COUNT(*) AS order_count,
    ROUND(SUM(order_total), 2) AS total_revenue
FROM {{zone_name}}.iceberg.orders
GROUP BY shipping_address.state
ORDER BY total_revenue DESC;


-- ============================================================================
-- VERIFY: Grand Totals
-- ============================================================================
-- Cross-cutting sanity check: total orders, sum of order totals, distinct
-- cities, and total line items across all orders.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 100
ASSERT VALUE sum_order_total = 111547.7
ASSERT VALUE distinct_cities = 15
ASSERT VALUE total_items = 311
SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(order_total), 2) AS sum_order_total,
    COUNT(DISTINCT shipping_address.city) AS distinct_cities,
    (SELECT COUNT(*) FROM (SELECT unnest(items) AS item FROM {{zone_name}}.iceberg.orders) sub) AS total_items
FROM {{zone_name}}.iceberg.orders;
