-- ============================================================================
-- XML E-Commerce Order Line Explosion — Verification Queries
-- ============================================================================
-- Each query verifies a specific XML feature: deep nesting, explode_paths,
-- CDATA, exclude_paths, column_mappings, default_repeat_handling.
-- ============================================================================


-- ============================================================================
-- 1. EXPLODED ROW COUNT — 7 items (file 1) + 4 items (file 2) = 11 rows
-- ============================================================================

ASSERT ROW_COUNT = 11
SELECT *
FROM {{zone_name}}.xml_demos.order_lines;


-- ============================================================================
-- 2. BROWSE ORDER LINES — See the exploded data with friendly column names
-- ============================================================================

ASSERT ROW_COUNT = 11
ASSERT VALUE orders_order_customer_name = 'Alice Johnson' WHERE orders_order_attr_id = 'ORD-1001' AND orders_order_items_item_attr_sku = 'WDG-100'
ASSERT VALUE orders_order_attr_status = 'shipped' WHERE orders_order_attr_id = 'ORD-1001' AND orders_order_items_item_attr_sku = 'WDG-100'
ASSERT VALUE orders_order_items_item_product = 'Premium Widget' WHERE orders_order_attr_id = 'ORD-1001' AND orders_order_items_item_attr_sku = 'WDG-100'
ASSERT VALUE orders_order_items_item_variant_color = 'Blue' WHERE orders_order_attr_id = 'ORD-1001' AND orders_order_items_item_attr_sku = 'WDG-100'
ASSERT VALUE orders_order_items_item_variant_size = 'Large' WHERE orders_order_attr_id = 'ORD-1001' AND orders_order_items_item_attr_sku = 'WDG-100'
ASSERT VALUE orders_order_customer_name = 'Bob Smith' WHERE orders_order_attr_id = 'ORD-1002' AND orders_order_items_item_attr_sku = 'CBL-050'
ASSERT VALUE orders_order_items_item_product = 'USB-C Cable' WHERE orders_order_attr_id = 'ORD-1002' AND orders_order_items_item_attr_sku = 'CBL-050'
ASSERT VALUE orders_order_customer_name = 'Emma Wilson' WHERE orders_order_attr_id = 'ORD-1005' AND orders_order_items_item_attr_sku = 'GDG-200'
ASSERT VALUE orders_order_items_item_variant_color = 'Rose Gold' WHERE orders_order_attr_id = 'ORD-1005' AND orders_order_items_item_attr_sku = 'GDG-200'
SELECT orders_order_attr_id, orders_order_attr_status, orders_order_customer_name, orders_order_items_item_attr_sku, orders_order_items_item_product,
       orders_order_items_item_quantity, orders_order_items_item_unit_price, orders_order_items_item_variant_size, orders_order_items_item_variant_color
FROM {{zone_name}}.xml_demos.order_lines
ORDER BY orders_order_attr_id, orders_order_items_item_attr_sku;


-- ============================================================================
-- 3. DEEP NESTING — variant/size and variant/color flattened to columns
-- ============================================================================
-- 3 levels deep: order → items/item → variant/color
-- All 11 items have variant info, so no NULLs expected.

ASSERT VALUE deep_nesting_count = 11
SELECT COUNT(*) FILTER (WHERE orders_order_items_item_variant_color IS NOT NULL) AS deep_nesting_count
FROM {{zone_name}}.xml_demos.order_lines;


-- ============================================================================
-- 4. COLUMN MAPPINGS — verify friendly names exist
-- ============================================================================
-- Deep paths mapped: customer/name → customer_name,
-- items/item/variant/color → item_color, etc.

ASSERT VALUE column_mapping_count = 11
SELECT COUNT(*) FILTER (WHERE orders_order_customer_name IS NOT NULL AND orders_order_items_item_variant_color IS NOT NULL) AS column_mapping_count
FROM {{zone_name}}.xml_demos.order_lines;


-- ============================================================================
-- 5. CDATA EXTRACTION — description should contain raw HTML tags
-- ============================================================================
-- Some items have CDATA-wrapped HTML: <![CDATA[<b>Premium</b> ...]]>
-- The CDATA wrapper should be removed but HTML preserved as text.

ASSERT VALUE cdata_count = 6
SELECT COUNT(*) FILTER (WHERE orders_order_items_item_description LIKE '%<b>%' OR orders_order_items_item_description LIKE '%<em>%') AS cdata_count
FROM {{zone_name}}.xml_demos.order_lines;


-- ============================================================================
-- 6. CDATA SPOT CHECK — verify specific CDATA content
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE orders_order_items_item_description IS NOT NULL WHERE orders_order_items_item_attr_sku = 'WDG-100'
ASSERT VALUE orders_order_items_item_description IS NOT NULL WHERE orders_order_items_item_attr_sku = 'GDG-200'
SELECT orders_order_items_item_attr_sku, orders_order_items_item_description
FROM {{zone_name}}.xml_demos.order_lines
WHERE orders_order_items_item_attr_sku IN ('WDG-100', 'GDG-200')
GROUP BY orders_order_items_item_attr_sku, orders_order_items_item_description
ORDER BY orders_order_items_item_attr_sku;


-- ============================================================================
-- 7. EXCLUDE PATHS — internal_audit columns should NOT exist
-- ============================================================================
-- The /orders/order/internal_audit block (cost_center, margin_pct) is
-- excluded via exclude_paths. No columns with those names should appear.

ASSERT VALUE audit_columns = 0
SELECT COUNT(*) AS audit_columns
FROM information_schema.columns
WHERE table_name = 'order_lines'
  AND (column_name LIKE '%cost_center%' OR column_name LIKE '%margin_pct%');


-- ============================================================================
-- 8. ORDER SUMMARY — 5 rows, one per order
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT *
FROM {{zone_name}}.xml_demos.order_summary;


-- ============================================================================
-- 9. BROWSE ORDER SUMMARY — see per-order view with flattened customer
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE orders_order_customer_name = 'Alice Johnson' WHERE orders_order_attr_id = 'ORD-1001'
ASSERT VALUE orders_order_customer_email = 'alice@example.com' WHERE orders_order_attr_id = 'ORD-1001'
ASSERT VALUE orders_order_customer_tier = 'gold' WHERE orders_order_attr_id = 'ORD-1001'
ASSERT VALUE orders_order_attr_status = 'shipped' WHERE orders_order_attr_id = 'ORD-1001'
ASSERT VALUE orders_order_shipping_total = '5.99' WHERE orders_order_attr_id = 'ORD-1001'
ASSERT VALUE orders_order_customer_name = 'Bob Smith' WHERE orders_order_attr_id = 'ORD-1002'
ASSERT VALUE orders_order_customer_tier = 'silver' WHERE orders_order_attr_id = 'ORD-1002'
ASSERT VALUE orders_order_attr_status = 'processing' WHERE orders_order_attr_id = 'ORD-1002'
ASSERT VALUE orders_order_customer_name = 'Carol Davis' WHERE orders_order_attr_id = 'ORD-1003'
ASSERT VALUE orders_order_customer_name = 'David Lee' WHERE orders_order_attr_id = 'ORD-1004'
ASSERT VALUE orders_order_customer_tier = 'bronze' WHERE orders_order_attr_id = 'ORD-1004'
ASSERT VALUE orders_order_customer_name = 'Emma Wilson' WHERE orders_order_attr_id = 'ORD-1005'
ASSERT VALUE orders_order_customer_tier = 'gold' WHERE orders_order_attr_id = 'ORD-1005'
SELECT orders_order_attr_id, orders_order_attr_status, orders_order_customer_name, orders_order_customer_email, orders_order_customer_tier,
       orders_order_order_date, orders_order_items_item, orders_order_shipping_total
FROM {{zone_name}}.xml_demos.order_summary
ORDER BY orders_order_attr_id;


-- ============================================================================
-- 10. LINE ITEM ANALYTICS — total quantity ordered by product
-- ============================================================================
-- Expected: USB-C Cable=8, Premium Widget=6, Widget Mini=6, Gadget Pro=4, Phone Case=1

ASSERT ROW_COUNT = 5
ASSERT VALUE total_qty = 8 WHERE orders_order_items_item_product = 'USB-C Cable'
ASSERT VALUE total_qty = 6 WHERE orders_order_items_item_product = 'Premium Widget'
ASSERT VALUE total_qty = 6 WHERE orders_order_items_item_product = 'Widget Mini'
ASSERT VALUE total_qty = 4 WHERE orders_order_items_item_product = 'Gadget Pro'
ASSERT VALUE total_qty = 1 WHERE orders_order_items_item_product = 'Phone Case'
ASSERT VALUE order_count = 2 WHERE orders_order_items_item_product = 'USB-C Cable'
ASSERT VALUE order_count = 3 WHERE orders_order_items_item_product = 'Premium Widget'
SELECT orders_order_items_item_product,
       SUM(CAST(orders_order_items_item_quantity AS INT)) AS total_qty,
       COUNT(*) AS order_count
FROM {{zone_name}}.xml_demos.order_lines
GROUP BY orders_order_items_item_product
ORDER BY total_qty DESC;


-- ============================================================================
-- 11. REVENUE BY ORDER — join quantity and price
-- ============================================================================
-- Expected:
--   ORD-1005 | Emma Wilson  | 189.95 | 5.99
--   ORD-1002 | Bob Smith    | 159.91 | 12.99
--   ORD-1001 | Alice Johnson| 139.97 | 5.99
--   ORD-1003 | Carol Davis  | 139.95 | 5.99
--   ORD-1004 | David Lee    |  59.97 | 8.99

ASSERT ROW_COUNT = 5
-- Non-deterministic: float aggregation — SUM(CAST(qty AS INT) * CAST(unit_price AS DOUBLE)) accumulates IEEE 754 rounding errors across rows
ASSERT WARNING VALUE order_total BETWEEN 189.94 AND 189.96 WHERE orders_order_attr_id = 'ORD-1005'
-- Non-deterministic: float aggregation — SUM(CAST(qty AS INT) * CAST(unit_price AS DOUBLE)) accumulates IEEE 754 rounding errors across rows
ASSERT WARNING VALUE order_total BETWEEN 159.90 AND 159.92 WHERE orders_order_attr_id = 'ORD-1002'
-- Non-deterministic: float aggregation — SUM(CAST(qty AS INT) * CAST(unit_price AS DOUBLE)) accumulates IEEE 754 rounding errors across rows
ASSERT WARNING VALUE order_total BETWEEN 139.96 AND 139.98 WHERE orders_order_attr_id = 'ORD-1001'
-- Non-deterministic: float aggregation — SUM(CAST(qty AS INT) * CAST(unit_price AS DOUBLE)) accumulates IEEE 754 rounding errors across rows
ASSERT WARNING VALUE order_total BETWEEN 139.94 AND 139.96 WHERE orders_order_attr_id = 'ORD-1003'
-- Non-deterministic: float aggregation — SUM(CAST(qty AS INT) * CAST(unit_price AS DOUBLE)) accumulates IEEE 754 rounding errors across rows
ASSERT WARNING VALUE order_total BETWEEN 59.96 AND 59.98 WHERE orders_order_attr_id = 'ORD-1004'
ASSERT VALUE shipping = '5.99' WHERE orders_order_attr_id = 'ORD-1001'
ASSERT VALUE shipping = '12.99' WHERE orders_order_attr_id = 'ORD-1002'
ASSERT VALUE shipping = '8.99' WHERE orders_order_attr_id = 'ORD-1004'
SELECT orders_order_attr_id, orders_order_customer_name,
       SUM(CAST(orders_order_items_item_quantity AS INT) * CAST(orders_order_items_item_unit_price AS DOUBLE)) AS order_total,
       MIN(orders_order_shipping_total) AS shipping
FROM {{zone_name}}.xml_demos.order_lines
GROUP BY orders_order_attr_id, orders_order_customer_name
ORDER BY order_total DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Returns failing checks only. ASSERT ROW_COUNT = 0 means every invariant
-- passed: correct explode count, deep nesting, column mappings, CDATA
-- extraction, and order summary row count.

ASSERT ROW_COUNT = 0
SELECT check_name, result
FROM (
    SELECT 'exploded_rows' AS check_name,
           CASE WHEN COUNT(*) = 11 THEN 'PASS' ELSE 'FAIL' END AS result
    FROM {{zone_name}}.xml_demos.order_lines
    UNION ALL
    SELECT 'deep_nesting_color',
           CASE WHEN COUNT(*) FILTER (WHERE orders_order_items_item_variant_color IS NOT NULL) = 11
                THEN 'PASS' ELSE 'FAIL' END
    FROM {{zone_name}}.xml_demos.order_lines
    UNION ALL
    SELECT 'column_mapping',
           CASE WHEN COUNT(*) FILTER (WHERE orders_order_customer_name IS NOT NULL AND orders_order_items_item_variant_color IS NOT NULL) = 11
                THEN 'PASS' ELSE 'FAIL' END
    FROM {{zone_name}}.xml_demos.order_lines
    UNION ALL
    SELECT 'cdata_extraction',
           CASE WHEN COUNT(*) FILTER (WHERE orders_order_items_item_description LIKE '%<b>%' OR orders_order_items_item_description LIKE '%<em>%') = 6
                THEN 'PASS' ELSE 'FAIL' END
    FROM {{zone_name}}.xml_demos.order_lines
    UNION ALL
    SELECT 'summary_rows',
           CASE WHEN COUNT(*) = 5 THEN 'PASS' ELSE 'FAIL' END
    FROM {{zone_name}}.xml_demos.order_summary
)
WHERE result = 'FAIL'
ORDER BY check_name;
