-- ============================================================================
-- Delta Partition MERGE — Educational Queries
-- ============================================================================
-- WHAT: MERGE INTO a PARTITIONED BY table matches source rows against target
--       rows, then executes UPDATE (matched) or INSERT (not matched) — but
--       only rewrites the partition directories that contain affected rows.
-- WHY:  In a real product catalog with millions of SKUs across dozens of
--       categories, a daily supplier sync should not rewrite every partition.
--       Partition-scoped MERGE ensures that unchanged categories (like Sports)
--       never have their Parquet files opened or rewritten.
-- HOW:  The MERGE engine reads partition metadata from the Delta log, prunes
--       partitions that have no matching source rows, and only writes new
--       files into affected partition directories.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Product Catalog by Category
-- ============================================================================
-- The catalog has 60 products across 4 categories (15 each). Let's see the
-- starting state of each partition before the supplier feed arrives:

ASSERT ROW_COUNT = 4
ASSERT VALUE product_count = 15 WHERE category = 'Electronics'
ASSERT VALUE product_count = 15 WHERE category = 'Clothing'
ASSERT VALUE product_count = 15 WHERE category = 'Home'
ASSERT VALUE product_count = 15 WHERE category = 'Sports'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(SUM(price), 2) AS total_value,
       ROUND(AVG(price), 2) AS avg_price,
       SUM(stock) AS total_stock
FROM {{zone_name}}.delta_demos.partitioned_product_catalog
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 2: Inspect the Supplier Feed
-- ============================================================================
-- The supplier sent 18 rows: updates for existing products (price reductions
-- and stock restocks) plus brand-new items. Note that Sports has zero rows
-- in the feed — that entire partition will be skipped during MERGE.

ASSERT ROW_COUNT = 3
SELECT category,
       COUNT(*) AS feed_rows
FROM {{zone_name}}.delta_demos.partitioned_supplier_feed
GROUP BY category
ORDER BY category;


-- ============================================================================
-- LEARN: Partition-Scoped MERGE — Upsert from Supplier Feed
-- ============================================================================
-- This MERGE matches on product id. For matched rows it updates price and
-- stock from the supplier. For new products (not matched) it inserts them.
--
-- Partition impact:
--   Electronics: 5 updated + 3 inserted → partition rewritten
--   Clothing:    5 updated + 2 inserted → partition rewritten
--   Home:        3 updated + 0 inserted → partition rewritten
--   Sports:      0 rows in feed         → partition UNTOUCHED
--
-- The key insight: Sports partition files are never read or written because
-- no source rows have category = 'Sports'.

ASSERT ROW_COUNT = 18
MERGE INTO {{zone_name}}.delta_demos.partitioned_product_catalog AS target
USING {{zone_name}}.delta_demos.partitioned_supplier_feed AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    price = source.price,
    stock = source.stock
WHEN NOT MATCHED THEN INSERT (id, sku, name, price, stock, supplier, category)
    VALUES (source.id, source.sku, source.name, source.price, source.stock,
            source.supplier, source.category);


-- ============================================================================
-- Query 3: Post-MERGE — Electronics Partition (5 Updated + 3 New)
-- ============================================================================
-- Electronics grew from 15 to 18 products. Let's verify the updated prices
-- and confirm the 3 new products appeared:

ASSERT ROW_COUNT = 18
ASSERT VALUE price = 24.99 WHERE id = 1
ASSERT VALUE price = 69.99 WHERE id = 3
ASSERT VALUE price = 119.99 WHERE id = 5
SELECT id, sku, name, price, stock
FROM {{zone_name}}.delta_demos.partitioned_product_catalog
WHERE category = 'Electronics'
ORDER BY id;


-- ============================================================================
-- Query 4: Post-MERGE — Clothing Partition (5 Updated + 2 New)
-- ============================================================================
-- Clothing grew from 15 to 17 products. The updated items got lower prices
-- and higher stock levels:

ASSERT ROW_COUNT = 17
ASSERT VALUE price = 54.99 WHERE id = 17
ASSERT VALUE price = 129.99 WHERE id = 19
SELECT id, sku, name, price, stock
FROM {{zone_name}}.delta_demos.partitioned_product_catalog
WHERE category = 'Clothing'
ORDER BY id;


-- ============================================================================
-- Query 5: Post-MERGE — Home Partition (3 Updated, No New)
-- ============================================================================
-- Home had 3 price reductions but no new products — it stays at 15 items:

ASSERT ROW_COUNT = 15
ASSERT VALUE price = 12.99 WHERE id = 33
ASSERT VALUE price = 49.99 WHERE id = 38
ASSERT VALUE price = 9.99 WHERE id = 40
SELECT id, sku, name, price, stock
FROM {{zone_name}}.delta_demos.partitioned_product_catalog
WHERE category = 'Home'
ORDER BY id;


-- ============================================================================
-- Query 6: Post-MERGE — Sports Partition (Untouched)
-- ============================================================================
-- Sports had zero rows in the supplier feed. This partition was completely
-- skipped during the MERGE — no files were read, no files were rewritten.
-- Every value should be identical to the baseline:

ASSERT ROW_COUNT = 1
ASSERT VALUE total_value = 413.85
ASSERT VALUE total_stock = 2495
SELECT COUNT(*) AS product_count,
       ROUND(SUM(price), 2) AS total_value,
       SUM(stock) AS total_stock
FROM {{zone_name}}.delta_demos.partitioned_product_catalog
WHERE category = 'Sports';


-- ============================================================================
-- Query 7: Cross-Partition Summary After MERGE
-- ============================================================================
-- Compare all 4 categories after the MERGE. Electronics and Clothing grew,
-- Home stayed the same size with updated prices, Sports is unchanged:

ASSERT ROW_COUNT = 4
ASSERT VALUE product_count = 18 WHERE category = 'Electronics'
ASSERT VALUE product_count = 17 WHERE category = 'Clothing'
ASSERT VALUE product_count = 15 WHERE category = 'Home'
ASSERT VALUE product_count = 15 WHERE category = 'Sports'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(SUM(price), 2) AS total_value,
       SUM(stock) AS total_stock
FROM {{zone_name}}.delta_demos.partitioned_product_catalog
GROUP BY category
ORDER BY category;


-- ============================================================================
-- Query 8: New Products Verification
-- ============================================================================
-- Verify that all 5 new products (3 Electronics + 2 Clothing) were inserted
-- correctly with proper category assignment:

ASSERT ROW_COUNT = 5
ASSERT VALUE name = 'Webcam 4K Pro' WHERE id = 61
ASSERT VALUE name = 'Thermal Leggings' WHERE id = 65
SELECT id, sku, name, price, stock, category
FROM {{zone_name}}.delta_demos.partitioned_product_catalog
WHERE id >= 61
ORDER BY id;


-- ============================================================================
-- Query 9: Supplier Contribution Across Partitions
-- ============================================================================
-- Aggregate by supplier across all partitions to see which suppliers have
-- the most products. This query reads all 4 partition directories:

ASSERT ROW_COUNT >= 5
ASSERT VALUE product_count = 7 WHERE supplier = 'TechCorp'
SELECT supplier,
       COUNT(*) AS product_count,
       ROUND(SUM(price), 2) AS total_value,
       SUM(stock) AS total_stock
FROM {{zone_name}}.delta_demos.partitioned_product_catalog
GROUP BY supplier
ORDER BY product_count DESC, supplier
LIMIT 10;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: 60 original + 5 new = 65
ASSERT VALUE cnt = 65
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_product_catalog;

-- Verify electronics_count: 15 + 3 new = 18
ASSERT VALUE cnt = 18
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_product_catalog WHERE category = 'Electronics';

-- Verify clothing_count: 15 + 2 new = 17
ASSERT VALUE cnt = 17
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_product_catalog WHERE category = 'Clothing';

-- Verify home_count: unchanged at 15
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_product_catalog WHERE category = 'Home';

-- Verify sports_count: unchanged at 15
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_product_catalog WHERE category = 'Sports';

-- Verify updated_price: id=1 Wireless Mouse 29.99 → 24.99
ASSERT VALUE price = 24.99
SELECT price FROM {{zone_name}}.delta_demos.partitioned_product_catalog WHERE id = 1;

-- Verify sports_unchanged: Sports total value identical to baseline
ASSERT VALUE total_value = 413.85
SELECT ROUND(SUM(price), 2) AS total_value FROM {{zone_name}}.delta_demos.partitioned_product_catalog WHERE category = 'Sports';

-- Verify new_products_exist: all 5 new IDs present
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.partitioned_product_catalog WHERE id IN (61, 62, 63, 64, 65);
