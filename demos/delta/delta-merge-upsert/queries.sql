-- ============================================================================
-- Delta MERGE — CDC Upsert with BY SOURCE — Educational Queries
-- ============================================================================
-- WHAT: MERGE INTO with WHEN MATCHED, WHEN NOT MATCHED, and the powerful
--       WHEN NOT MATCHED BY SOURCE clause for full CDC-style upserts.
-- WHY:  In CDC pipelines, you need to handle three cases: existing records
--       that changed, new records to insert, and stale records in the
--       target that are no longer present in the source. The BY SOURCE
--       clause handles this third case without a separate DELETE statement.
-- HOW:  Delta scans both target and source, joins on the ON condition,
--       then evaluates clauses for matched rows AND for target rows with
--       no source match. The entire result is committed atomically.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Target vs Source Before MERGE
-- ============================================================================
-- The target has 15 products. The source feed has 12 items: 8 updates
-- for existing products (ids 1-8) and 4 new products (ids 16-19).
-- Products 9-15 are NOT in the source feed at all.

ASSERT ROW_COUNT = 15
SELECT id, sku, name, category, price, in_stock
FROM {{zone_name}}.delta_demos.upsert_products
ORDER BY id;

ASSERT ROW_COUNT = 12
SELECT id, sku, name, category, price, in_stock
FROM {{zone_name}}.delta_demos.product_feed
ORDER BY id;


-- ============================================================================
-- MERGE: Three-Way CDC Upsert
-- ============================================================================
-- This single MERGE statement handles all three CDC cases atomically:
--
--   WHEN MATCHED → Update price and stock from the feed.
--       8 products (ids 1-8) will be updated with new prices/stock.
--
--   WHEN NOT MATCHED → Insert new products from the feed.
--       4 new products (ids 16-19) will be added to the catalog.
--
--   WHEN NOT MATCHED BY SOURCE → Delete discontinued products.
--       Products 9-15 are NOT in the feed. Of those, items with
--       in_stock <= 5 are considered discontinued: ids 13 (5), 14 (2), 15 (1).
--       The predicate AND target.in_stock <= 5 protects well-stocked items.
--       → 3 discontinued products deleted

ASSERT ROW_COUNT = 15
MERGE INTO {{zone_name}}.delta_demos.upsert_products AS target
USING {{zone_name}}.delta_demos.product_feed AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET
        price    = source.price,
        in_stock = source.in_stock
WHEN NOT MATCHED THEN
    INSERT (id, sku, name, category, price, in_stock)
    VALUES (source.id, source.sku, source.name, source.category,
            source.price, source.in_stock)
WHEN NOT MATCHED BY SOURCE AND target.in_stock <= 5 THEN
    DELETE;


-- ============================================================================
-- EXPLORE: Product Catalog After MERGE
-- ============================================================================
-- The MERGE performed three operations atomically:
--   - Updated 8 products (ids 1-8) with new prices and stock levels
--   - Inserted 4 new products (ids 16-19)
--   - Deleted 3 discontinued products with low stock (ids 13, 14, 15)
--
-- Final count: 15 original - 3 deleted + 4 inserted = 16 products

ASSERT ROW_COUNT = 16
SELECT id, sku, name, category, price, in_stock
FROM {{zone_name}}.delta_demos.upsert_products
ORDER BY id;


-- ============================================================================
-- LEARN: WHEN MATCHED — Price & Stock Updates
-- ============================================================================
-- The 8 matched products were updated with new prices and stock levels
-- from the supplier feed. Let's verify a few key updates:

ASSERT ROW_COUNT = 8
ASSERT VALUE price = 24.99 WHERE id = 1
ASSERT VALUE price = 69.99 WHERE id = 4
ASSERT VALUE in_stock = 50 WHERE id = 8
SELECT id, sku, name, price, in_stock
FROM {{zone_name}}.delta_demos.upsert_products
WHERE id BETWEEN 1 AND 8
ORDER BY id;


-- ============================================================================
-- LEARN: WHEN NOT MATCHED BY SOURCE — Discontinued Removal
-- ============================================================================
-- Products 9-15 were NOT in the source feed. The BY SOURCE clause only
-- deleted those with in_stock <= 5 (discontinued stock):
--   - id=13 VGA Adapter (5 units) → DELETED
--   - id=14 Parallel Port Cable (2 units) → DELETED
--   - id=15 Floppy Drive USB (1 unit) → DELETED
--
-- Products 9-12 survived because they had sufficient stock:

ASSERT ROW_COUNT = 4
ASSERT VALUE in_stock = 300 WHERE id = 9
ASSERT VALUE in_stock = 25 WHERE id = 11
SELECT id, sku, name, in_stock
FROM {{zone_name}}.delta_demos.upsert_products
WHERE id BETWEEN 9 AND 15
ORDER BY id;


-- ============================================================================
-- EXPLORE: New Products from Feed
-- ============================================================================
-- The 4 new products (ids 16-19) were inserted from the supplier feed
-- as they had no matching id in the target:

ASSERT ROW_COUNT = 4
ASSERT VALUE name = 'Thunderbolt Dock' WHERE id = 19
ASSERT VALUE price = 19.99 WHERE id = 16
SELECT id, sku, name, category, price, in_stock
FROM {{zone_name}}.delta_demos.upsert_products
WHERE id BETWEEN 16 AND 19
ORDER BY id;


-- ============================================================================
-- EXPLORE: Category Summary After MERGE
-- ============================================================================
-- Let's see how the catalog looks across categories after the merge:

ASSERT ROW_COUNT = 3
ASSERT VALUE product_count = 8 WHERE category = 'electronics'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(AVG(price), 2) AS avg_price,
       SUM(in_stock) AS total_stock
FROM {{zone_name}}.delta_demos.upsert_products
GROUP BY category
ORDER BY product_count DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify final_row_count: 15 - 3 deleted + 4 inserted = 16
ASSERT ROW_COUNT = 16
SELECT * FROM {{zone_name}}.delta_demos.upsert_products;

-- Verify discontinued_removed: 3 low-stock products removed by BY SOURCE
ASSERT VALUE cnt = 0
SELECT COUNT(*) FILTER (WHERE id IN (13, 14, 15)) AS cnt FROM {{zone_name}}.delta_demos.upsert_products;

-- Verify new_products_inserted: 4 new products (ids 16-19) inserted
ASSERT VALUE cnt = 4
SELECT COUNT(*) FILTER (WHERE id BETWEEN 16 AND 19) AS cnt FROM {{zone_name}}.delta_demos.upsert_products;

-- Verify well_stocked_survived: Products 9-12 not deleted (had stock > 5)
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.upsert_products WHERE id BETWEEN 9 AND 12;

-- Verify mouse_price_updated: Wireless Mouse price dropped to 24.99
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.upsert_products WHERE id = 1 AND price = 24.99;

-- Verify headset_stock_updated: Headset Pro restocked to 50
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.upsert_products WHERE id = 8 AND in_stock = 50;

-- Verify thunderbolt_inserted: Thunderbolt Dock exists with correct price
ASSERT VALUE cnt = 1
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.upsert_products WHERE id = 19 AND price = 229.99;

-- Verify electronics_count: 8 electronics products after merge
ASSERT VALUE cnt = 8
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.upsert_products WHERE category = 'electronics';
