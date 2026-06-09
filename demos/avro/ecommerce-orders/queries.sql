-- ============================================================================
-- Demo: Avro E-Commerce Orders — Logical Types & Nullable Unions
-- ============================================================================
-- Tests Avro logical type support: date (int), timestamp-millis (long),
-- nullable union fields (discount_pct, notes), integer monetary arithmetic,
-- mixed compression codecs (null + deflate), and multi-file reading.

-- ============================================================================
-- Query 1: Browse All Orders — Verify row count and column presence
-- ============================================================================
-- Reads both Q1 (null codec) and Q2 (deflate codec) files. Confirms all 80
-- orders load correctly with logical types intact.

ASSERT ROW_COUNT = 80
ASSERT VALUE order_id = 'ORD-10000' WHERE order_id = 'ORD-10000'
ASSERT VALUE customer_name = 'Alice Chen' WHERE order_id = 'ORD-10000'
ASSERT VALUE unit_price_cents = 500 WHERE order_id = 'ORD-10000'
SELECT
    order_id,
    customer_id,
    customer_name,
    order_date,
    order_timestamp_ms,
    product_category,
    product_name,
    quantity,
    unit_price_cents,
    discount_pct,
    shipping_country,
    status,
    notes,
    df_file_name
FROM {{zone_name}}.ecommerce.all_orders
ORDER BY order_id;

-- ============================================================================
-- Query 2: Q1 Filter — file_filter isolates first-quarter orders
-- ============================================================================
-- The q1_orders table uses file_filter = '*q1*' to read only orders_q1.avro.

ASSERT ROW_COUNT = 40
ASSERT VALUE order_id = 'ORD-10000' WHERE order_id = 'ORD-10000'
ASSERT VALUE order_id = 'ORD-10039' WHERE order_id = 'ORD-10039'
SELECT
    order_id,
    customer_id,
    customer_name,
    product_category,
    quantity,
    unit_price_cents,
    status
FROM {{zone_name}}.ecommerce.q1_orders
ORDER BY order_id;

-- ============================================================================
-- Query 3: Sample — max_rows limits each file to 10 rows
-- ============================================================================
-- Two files × 10 rows each = 20 rows total.

ASSERT ROW_COUNT = 20
SELECT
    order_id,
    customer_name,
    product_category,
    unit_price_cents,
    status
FROM {{zone_name}}.ecommerce.sample_orders
ORDER BY order_id;

-- ============================================================================
-- Query 4: Revenue by Category — Monetary arithmetic on integer cents
-- ============================================================================
-- Aggregates quantity × unit_price_cents per category. Validates that Avro
-- integer/long types support correct arithmetic without precision loss.

ASSERT ROW_COUNT = 5
ASSERT VALUE total_revenue_cents = 71200 WHERE product_category = 'Electronics'
ASSERT VALUE total_revenue_cents = 146784 WHERE product_category = 'Clothing'
ASSERT VALUE total_revenue_cents = 226752 WHERE product_category = 'Home & Garden'
ASSERT VALUE total_revenue_cents = 311104 WHERE product_category = 'Books'
ASSERT VALUE total_revenue_cents = 399840 WHERE product_category = 'Sports'
SELECT
    product_category,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_items,
    SUM(quantity * unit_price_cents) AS total_revenue_cents
FROM {{zone_name}}.ecommerce.all_orders
GROUP BY product_category
ORDER BY total_revenue_cents;

-- ============================================================================
-- Query 5: NULL Handling — Nullable union fields (discount_pct, notes)
-- ============================================================================
-- Avro union types ["null", "double"] and ["null", "string"] must surface as
-- SQL NULLs. Validates COALESCE, IS NULL, and COUNT behavior on nullable unions.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 80
ASSERT VALUE orders_with_discount = 60
ASSERT VALUE orders_without_discount = 20
ASSERT VALUE orders_with_notes = 32
ASSERT VALUE orders_without_notes = 48
SELECT
    COUNT(*) AS total_orders,
    COUNT(discount_pct) AS orders_with_discount,
    SUM(CASE WHEN discount_pct IS NULL THEN 1 ELSE 0 END) AS orders_without_discount,
    COUNT(notes) AS orders_with_notes,
    SUM(CASE WHEN notes IS NULL THEN 1 ELSE 0 END) AS orders_without_notes
FROM {{zone_name}}.ecommerce.all_orders;

-- ============================================================================
-- Query 6: Status Breakdown — Aggregation with even distribution
-- ============================================================================
-- All 5 statuses have exactly 16 orders each. Tests GROUP BY on string column
-- read from Avro and validates precise counts.

ASSERT ROW_COUNT = 5
ASSERT VALUE order_count = 16 WHERE status = 'completed'
ASSERT VALUE order_count = 16 WHERE status = 'shipped'
ASSERT VALUE order_count = 16 WHERE status = 'pending'
ASSERT VALUE order_count = 16 WHERE status = 'cancelled'
ASSERT VALUE order_count = 16 WHERE status = 'returned'
SELECT
    status,
    COUNT(*) AS order_count,
    SUM(quantity * unit_price_cents) AS total_value_cents
FROM {{zone_name}}.ecommerce.all_orders
GROUP BY status
ORDER BY status;

-- ============================================================================
-- Query 7: Customer Analytics — Top customers by order frequency
-- ============================================================================
-- 15 distinct customers across 80 orders. Validates GROUP BY on customer_id
-- with aggregation, and confirms the top customer.

ASSERT ROW_COUNT = 15
ASSERT VALUE order_count = 6 WHERE customer_name = 'Alice Chen'
SELECT
    customer_id,
    customer_name,
    COUNT(*) AS order_count,
    SUM(quantity * unit_price_cents) AS total_spent_cents
FROM {{zone_name}}.ecommerce.all_orders
GROUP BY customer_id, customer_name
ORDER BY order_count DESC, customer_name;

-- ============================================================================
-- Query 8: Country Distribution — Shipping analytics
-- ============================================================================
-- 7 countries with 11-12 orders each. Tests GROUP BY and COUNT on string field.

ASSERT ROW_COUNT = 7
ASSERT VALUE order_count = 12 WHERE shipping_country = 'US'
ASSERT VALUE order_count = 12 WHERE shipping_country = 'UK'
ASSERT VALUE order_count = 12 WHERE shipping_country = 'DE'
SELECT
    shipping_country,
    COUNT(*) AS order_count,
    SUM(quantity * unit_price_cents) AS country_revenue_cents
FROM {{zone_name}}.ecommerce.all_orders
GROUP BY shipping_country
ORDER BY order_count DESC, shipping_country;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, distinct counts, NULL proportions,
-- and aggregate revenue across the entire dataset.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_orders = 80
ASSERT VALUE distinct_customers = 15
ASSERT VALUE distinct_categories = 5
ASSERT VALUE null_discount_count = 20
ASSERT VALUE null_notes_count = 48
ASSERT VALUE grand_total_revenue_cents = 1155680
SELECT
    COUNT(*) AS total_orders,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    COUNT(DISTINCT product_category) AS distinct_categories,
    SUM(CASE WHEN discount_pct IS NULL THEN 1 ELSE 0 END) AS null_discount_count,
    SUM(CASE WHEN notes IS NULL THEN 1 ELSE 0 END) AS null_notes_count,
    SUM(quantity * unit_price_cents) AS grand_total_revenue_cents
FROM {{zone_name}}.ecommerce.all_orders;
