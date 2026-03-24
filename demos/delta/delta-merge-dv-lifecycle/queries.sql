-- ============================================================================
-- Delta MERGE — Generating & Materializing Deletion Vectors — Educational Queries
-- ============================================================================
-- WHAT: MERGE INTO with conditional WHEN MATCHED clauses generates deletion
--       vectors (DVs) for old row versions. OPTIMIZE materializes DVs by
--       rewriting data files without the DV-marked rows.
-- WHY:  Real ERP-to-catalog sync pipelines use MERGE to atomically apply
--       mixed changes — price updates, discontinuations, and new products —
--       in a single transaction. Understanding how MERGE creates DVs and
--       how OPTIMIZE cleans them up is essential for Delta table maintenance.
-- HOW:  Delta's MERGE evaluates WHEN clauses top-to-bottom per source row.
--       UPDATE marks the old row with a DV and writes the new row to a fresh
--       data file. DELETE marks the row with a DV only. INSERT writes to a
--       new data file. OPTIMIZE later rewrites all files, excluding DV-marked
--       rows, producing clean compacted Parquet.
--
-- This demo covers the full MERGE → DV → OPTIMIZE lifecycle:
--   1. EXPLORE  — Baseline catalog: 40 products, per-category stats
--   2. EXPLORE  — Preview supplier feed: 10 updates, 5 deletes, 5 inserts
--   3. STEP 1   — MERGE supplier_feed INTO product_catalog
--   4. LEARN    — Post-merge state: verify updates, deletions, insertions
--   5. INSPECT  — DESCRIBE DETAIL to see accumulated DVs
--   6. STEP 2   — OPTIMIZE to materialize all DVs
--   7. INSPECT  — DESCRIBE DETAIL to confirm DVs are gone
--   8. EXPLORE  — Transaction history via DESCRIBE HISTORY
--   9. VERIFY   — Full correctness checks
-- ============================================================================


-- ============================================================================
-- EXPLORE: Baseline Product Catalog — 40 Products
-- ============================================================================
-- The catalog has 40 products across 4 categories with 10 products each.
-- This is the state before the daily ERP supplier feed arrives.

ASSERT ROW_COUNT = 4
ASSERT VALUE product_count = 10 WHERE category = 'electronics'
ASSERT VALUE product_count = 10 WHERE category = 'clothing'
ASSERT VALUE product_count = 10 WHERE category = 'home'
ASSERT VALUE product_count = 10 WHERE category = 'food'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(AVG(price), 2) AS avg_price,
       SUM(stock) AS total_stock
FROM {{zone_name}}.delta_demos.product_catalog
GROUP BY category
ORDER BY category;


-- ============================================================================
-- EXPLORE: Preview Supplier Feed — What's Coming
-- ============================================================================
-- The upstream ERP system sent 20 rows in the daily feed. Let's classify
-- them before running the MERGE to understand exactly what will happen:
--   - 10 rows match existing SKUs with stock > 0 (will UPDATE price/stock)
--   - 5 rows match existing SKUs with stock = 0 (will DELETE — discontinued)
--   - 5 rows have new SKUs not in the catalog (will INSERT)

ASSERT ROW_COUNT = 20
SELECT sku, name, category, price, stock,
       CASE
           WHEN stock = 0 THEN 'DELETE (discontinued)'
           ELSE 'UPDATE or INSERT'
       END AS feed_action
FROM {{zone_name}}.delta_demos.supplier_feed
ORDER BY stock, sku;


-- ============================================================================
-- EXPLORE: Identify the 5 Discontinued Products (stock = 0)
-- ============================================================================
-- These 5 SKUs will be removed from the catalog by the MERGE. One product
-- from each category ensures the demo shows cross-category impact.

ASSERT ROW_COUNT = 5
SELECT sf.sku, sf.name, sf.category, pc.stock AS current_stock
FROM {{zone_name}}.delta_demos.supplier_feed sf
JOIN {{zone_name}}.delta_demos.product_catalog pc ON sf.sku = pc.sku
WHERE sf.stock = 0
ORDER BY sf.sku;


-- ============================================================================
-- STEP 1: MERGE — Apply Supplier Feed to Product Catalog
-- ============================================================================
-- This is the core operation. A single MERGE statement atomically:
--   1. UPDATES price, stock, and last_updated for 10 matched SKUs (stock > 0)
--   2. DELETES 5 matched SKUs where stock = 0 (discontinued by supplier)
--   3. INSERTS 5 brand-new SKUs that don't exist in the catalog
--
-- Under the hood, Delta creates deletion vectors for every row touched by
-- UPDATE or DELETE. UPDATE = DV on old row + write new row. DELETE = DV only.
-- INSERT writes new rows to fresh data files. All in one atomic commit.

ASSERT ROW_COUNT = 20
MERGE INTO {{zone_name}}.delta_demos.product_catalog AS target
USING {{zone_name}}.delta_demos.supplier_feed AS source
ON target.sku = source.sku
WHEN MATCHED AND source.stock > 0 THEN
    UPDATE SET price = source.price,
               stock = source.stock,
               last_updated = source.last_updated
WHEN MATCHED AND source.stock = 0 THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT (sku, name, category, price, stock, supplier, last_updated)
    VALUES (source.sku, source.name, source.category,
            source.price, source.stock, source.supplier, source.last_updated);


-- ============================================================================
-- LEARN: Post-Merge Catalog — Still 40 Products (40 - 5 + 5)
-- ============================================================================
-- The catalog lost 5 discontinued products and gained 5 new ones, netting
-- the same 40-row count. But the composition changed: prices and stock
-- shifted for 10 existing products, and 5 SKUs were swapped out entirely.

ASSERT ROW_COUNT = 4
ASSERT VALUE product_count = 10 WHERE category = 'electronics'
ASSERT VALUE product_count = 10 WHERE category = 'clothing'
ASSERT VALUE product_count = 10 WHERE category = 'home'
ASSERT VALUE product_count = 10 WHERE category = 'food'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(AVG(price), 2) AS avg_price,
       SUM(stock) AS total_stock
FROM {{zone_name}}.delta_demos.product_catalog
GROUP BY category
ORDER BY category;


-- ============================================================================
-- LEARN: Verify Deletions — 5 Discontinued SKUs Are Gone
-- ============================================================================
-- The MERGE matched these 5 SKUs with stock = 0 in the source, triggering
-- the WHEN MATCHED AND source.stock = 0 THEN DELETE clause. Delta created
-- deletion vectors marking these rows — the Parquet data files were NOT
-- rewritten. The rows are simply invisible to queries.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 0
SELECT sku, name
FROM {{zone_name}}.delta_demos.product_catalog
WHERE sku IN ('ELEC-1009', 'CLTH-2008', 'HOME-3009', 'FOOD-4003', 'FOOD-4008');


-- ============================================================================
-- LEARN: Verify New Products — 5 New SKUs Inserted
-- ============================================================================
-- These SKUs had no match in the target, so the WHEN NOT MATCHED clause
-- inserted them as brand-new rows written to fresh data files.

ASSERT ROW_COUNT = 5
SELECT sku, name, category, price, stock, supplier
FROM {{zone_name}}.delta_demos.product_catalog
WHERE sku IN ('ELEC-1011', 'CLTH-2011', 'HOME-3011', 'FOOD-4011', 'FOOD-4012')
ORDER BY sku;


-- ============================================================================
-- LEARN: Verify Specific Price Updates
-- ============================================================================
-- The WHEN MATCHED AND source.stock > 0 clause updated these rows. Delta
-- created a DV on the old row version and wrote a new row with updated
-- values. Let's spot-check four products across different categories:

ASSERT ROW_COUNT = 4
ASSERT VALUE price = 44.99 WHERE sku = 'ELEC-1001'
ASSERT VALUE price = 99.99 WHERE sku = 'CLTH-2005'
ASSERT VALUE price = 27.99 WHERE sku = 'FOOD-4001'
ASSERT VALUE stock = 180 WHERE sku = 'HOME-3003'
SELECT sku, name, price, stock, last_updated
FROM {{zone_name}}.delta_demos.product_catalog
WHERE sku IN ('ELEC-1001', 'CLTH-2005', 'FOOD-4001', 'HOME-3003')
ORDER BY sku;


-- ============================================================================
-- LEARN: Updated vs Unchanged vs New Products
-- ============================================================================
-- The MERGE touched 20 source rows but only 15 ended up in the catalog
-- (10 updated + 5 inserted). 25 original products were untouched. We can
-- tell which is which by looking at the last_updated timestamp.

ASSERT ROW_COUNT = 2
ASSERT VALUE product_count = 25 WHERE merge_status = 'Unchanged (2025-03-01)'
ASSERT VALUE product_count = 15 WHERE merge_status = 'Updated or New (2025-03-15)'
SELECT CASE
           WHEN last_updated = '2025-03-15' THEN 'Updated or New (2025-03-15)'
           ELSE 'Unchanged (2025-03-01)'
       END AS merge_status,
       COUNT(*) AS product_count
FROM {{zone_name}}.delta_demos.product_catalog
GROUP BY CASE
           WHEN last_updated = '2025-03-15' THEN 'Updated or New (2025-03-15)'
           ELSE 'Unchanged (2025-03-01)'
       END
ORDER BY merge_status;


-- ============================================================================
-- INSPECT: Table Detail Before OPTIMIZE — DVs Are Accumulated
-- ============================================================================
-- DESCRIBE DETAIL shows the physical storage layout. After the MERGE, you'll
-- see the original data file(s) with DV bitmap files attached, plus new data
-- files from the UPDATE writes and INSERT writes. The DVs mark 15 row indices
-- as deleted (10 old versions from UPDATE + 5 from DELETE).

ASSERT ROW_COUNT >= 2
DESCRIBE DETAIL {{zone_name}}.delta_demos.product_catalog;


-- ============================================================================
-- STEP 2: OPTIMIZE — Materialize All Deletion Vectors
-- ============================================================================
-- OPTIMIZE rewrites the data files, physically excluding rows marked by DVs.
-- After OPTIMIZE:
--   - No more DV bitmap files (all materialized into clean data)
--   - Fewer, larger data files (small files merged)
--   - Same logical data, better read performance
-- This is the weekly maintenance step that keeps the Delta table healthy.

OPTIMIZE {{zone_name}}.delta_demos.product_catalog;


-- ============================================================================
-- INSPECT: Table Detail After OPTIMIZE — DVs Are Gone
-- ============================================================================
-- Compare with the pre-OPTIMIZE detail: the DV bitmap files are gone and
-- the data files are compacted. The logical data is identical — only the
-- physical storage layout changed.

ASSERT ROW_COUNT >= 1
DESCRIBE DETAIL {{zone_name}}.delta_demos.product_catalog;


-- ============================================================================
-- EXPLORE: Final Catalog Summary — Per-Category Aggregates
-- ============================================================================
-- After OPTIMIZE, the table is fully compacted. The data reflects all MERGE
-- changes: updated prices, removed discontinued items, and new products.

ASSERT ROW_COUNT = 4
ASSERT VALUE total_stock = 1200 WHERE category = 'electronics'
ASSERT VALUE total_stock = 1905 WHERE category = 'clothing'
ASSERT VALUE total_stock = 1640 WHERE category = 'home'
ASSERT VALUE total_stock = 2870 WHERE category = 'food'
SELECT category,
       COUNT(*) AS product_count,
       ROUND(AVG(price), 2) AS avg_price,
       SUM(stock) AS total_stock
FROM {{zone_name}}.delta_demos.product_catalog
GROUP BY category
ORDER BY category;


-- ============================================================================
-- EXPLORE: Transaction History — Every Operation Logged
-- ============================================================================
-- DESCRIBE HISTORY reveals the full version log for the product_catalog:
--   v0: CREATE TABLE
--   v1: INSERT (40 baseline products)
--   v2: MERGE (10 updates + 5 deletes + 5 inserts — DVs created)
--   v3: OPTIMIZE (DVs materialized into compacted files)

ASSERT ROW_COUNT = 4
DESCRIBE HISTORY {{zone_name}}.delta_demos.product_catalog;


-- ============================================================================
-- EXPLORE: Time Travel — Access Pre-MERGE Catalog
-- ============================================================================
-- Even after the MERGE and OPTIMIZE, the original 40-row catalog is still
-- accessible via time travel. Version 1 is the state after the initial
-- INSERT but before the MERGE — all 40 original products intact.

ASSERT VALUE original_count = 40
SELECT COUNT(*) AS original_count
FROM {{zone_name}}.delta_demos.product_catalog VERSION AS OF 1;


-- ============================================================================
-- STEP 3: VACUUM — Purge Orphaned Files
-- ============================================================================
-- OPTIMIZE left behind the old data files and DV bitmaps as orphaned files.
-- VACUUM removes files no longer referenced by any active transaction,
-- reclaiming storage. This completes the full lifecycle:
--   MERGE (create DVs) → OPTIMIZE (materialize DVs) → VACUUM (purge orphans)

VACUUM {{zone_name}}.delta_demos.product_catalog RETAIN 0 HOURS;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 40 - 5 deleted + 5 inserted = 40
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.product_catalog;

-- Verify deleted_skus_gone: all 5 discontinued SKUs were removed
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_catalog
WHERE sku IN ('ELEC-1009', 'CLTH-2008', 'HOME-3009', 'FOOD-4003', 'FOOD-4008');

-- Verify new_skus_present: all 5 new SKUs were inserted
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_catalog
WHERE sku IN ('ELEC-1011', 'CLTH-2011', 'HOME-3011', 'FOOD-4011', 'FOOD-4012');

-- Verify earbuds_price_updated: ELEC-1001 price dropped from 49.99 to 44.99
ASSERT VALUE price = 44.99
SELECT price FROM {{zone_name}}.delta_demos.product_catalog WHERE sku = 'ELEC-1001';

-- Verify coffee_price_updated: FOOD-4001 price rose from 24.99 to 27.99
ASSERT VALUE price = 27.99
SELECT price FROM {{zone_name}}.delta_demos.product_catalog WHERE sku = 'FOOD-4001';

-- Verify shoes_price_updated: CLTH-2005 price dropped from 109.99 to 99.99
ASSERT VALUE price = 99.99
SELECT price FROM {{zone_name}}.delta_demos.product_catalog WHERE sku = 'CLTH-2005';

-- Verify pillow_stock_updated: HOME-3003 stock increased from 150 to 180
ASSERT VALUE stock = 180
SELECT stock FROM {{zone_name}}.delta_demos.product_catalog WHERE sku = 'HOME-3003';

-- Verify electronics_count: 10 electronics products after MERGE
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_catalog WHERE category = 'electronics';

-- Verify clothing_count: 10 clothing products after MERGE
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_catalog WHERE category = 'clothing';

-- Verify home_count: 10 home products after MERGE
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_catalog WHERE category = 'home';

-- Verify food_count: 10 food products after MERGE
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_catalog WHERE category = 'food';

-- Verify updated_timestamp_count: 15 rows with 2025-03-15 (10 updated + 5 inserted)
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_catalog WHERE last_updated = '2025-03-15';

-- Verify sku_uniqueness: all 40 SKUs are unique
ASSERT VALUE cnt = 40
SELECT COUNT(DISTINCT sku) AS cnt FROM {{zone_name}}.delta_demos.product_catalog;
