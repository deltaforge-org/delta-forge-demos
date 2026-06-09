-- ============================================================================
-- Delta MERGE — Advanced Patterns & Conditional Logic — Educational Queries
-- ============================================================================
-- WHAT: MERGE INTO with multiple WHEN MATCHED clauses using conditional
--       predicates to route rows to UPDATE, DELETE, or INSERT actions.
-- WHY:  Real ETL pipelines need to handle mixed changes atomically —
--       update existing products, remove discontinued items, and add new
--       ones — all in a single transaction to avoid partial state.
-- HOW:  Delta evaluates WHEN clauses top-to-bottom for each source row.
--       The first matching clause executes. Under the hood, Delta reads
--       both tables, computes the join, and writes a new set of data
--       files reflecting all changes in one commit.
-- ============================================================================


-- ============================================================================
-- PREVIEW: Staging Data About to Be Merged
-- ============================================================================
-- Before running the MERGE, let's inspect the staging table to understand
-- what changes are queued up. Notice the three groups:
--   - 12 rows with qty > 0 matching existing SKUs (will UPDATE)
--   - 3 rows with qty = 0 matching existing SKUs (will DELETE)
--   - 15 rows with new SKUs not in master (will INSERT)

ASSERT ROW_COUNT = 30
SELECT id, sku, name, category, price, qty, supplier, last_updated
FROM {{zone_name}}.delta_demos.inventory_updates
ORDER BY id;


-- ============================================================================
-- MERGE: Apply Updates to Master Inventory
-- ============================================================================
-- This is the core operation. A single MERGE statement atomically:
--   1. UPDATES price, qty, and last_updated for matched SKUs with qty > 0
--   2. DELETES matched SKUs where qty = 0 (out-of-stock / discontinued)
--   3. INSERTS brand-new SKUs that don't exist in master
--
-- Delta evaluates WHEN clauses top-to-bottom. The first clause that matches
-- a given row executes. In Delta's transaction log, this entire MERGE is a
-- single commit: one JSON entry in _delta_log/ that records removed files
-- (old data) and added files (new data) together.

ASSERT ROW_COUNT = 30
MERGE INTO {{zone_name}}.delta_demos.inventory_master AS target
USING {{zone_name}}.delta_demos.inventory_updates AS source
ON target.sku = source.sku
WHEN MATCHED AND source.qty > 0 THEN
    UPDATE SET price = source.price,
               qty = source.qty,
               last_updated = source.last_updated
WHEN MATCHED AND source.qty = 0 THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT (id, sku, name, category, price, qty, supplier, last_updated)
    VALUES (source.id, source.sku, source.name, source.category,
            source.price, source.qty, source.supplier, source.last_updated);


-- ============================================================================
-- EXPLORE: Inventory After the MERGE
-- ============================================================================
-- The MERGE operation applied three types of changes atomically:
--   1. Updated price/qty for 12 existing SKUs (source qty > 0)
--   2. Deleted 3 SKUs where source qty = 0 (discontinued)
--   3. Inserted 15 brand-new SKUs
-- Let's see the final inventory by category:

ASSERT VALUE product_count = 20 WHERE category = 'Electronics'
-- Non-deterministic: ROUND(SUM(price * qty), 2) on DOUBLE columns — float addition order may vary by ±0.01
ASSERT WARNING VALUE inventory_value BETWEEN 92575.60 AND 92577.60 WHERE category = 'Electronics'
ASSERT ROW_COUNT = 4
SELECT category,
       COUNT(*) AS product_count,
       ROUND(SUM(price * qty), 2) AS inventory_value,
       ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.delta_demos.inventory_master
GROUP BY category
ORDER BY inventory_value DESC;


-- ============================================================================
-- LEARN: Conditional WHEN MATCHED — UPDATE vs DELETE
-- ============================================================================
-- The key Delta MERGE feature demonstrated here is using multiple
-- WHEN MATCHED clauses with different predicates:
--
--   WHEN MATCHED AND source.qty > 0 THEN UPDATE ...
--   WHEN MATCHED AND source.qty = 0 THEN DELETE
--
-- Delta evaluates these in order. If a matched row has qty > 0, it
-- updates price and quantity. If qty = 0, it deletes the row entirely.
-- This is one atomic operation — there is no window where the table
-- is in a partial state.
--
-- Let's verify the 3 deleted items are gone (Cable Tray, Whiteboard
-- Large, Label Maker — all had qty = 0 in the source):

ASSERT ROW_COUNT = 0
SELECT sku, name
FROM {{zone_name}}.delta_demos.inventory_master
WHERE sku IN ('FURN-004', 'FURN-008', 'SUPP-008');


-- ============================================================================
-- EXPLORE: Updated vs Unchanged Products
-- ============================================================================
-- Of the 40 original products, 12 were updated (matched with qty > 0),
-- 3 were deleted (matched with qty = 0), and 25 were unchanged (not in
-- the source table at all). Let's compare:

ASSERT ROW_COUNT = 3
ASSERT VALUE product_count = 15 WHERE merge_action = 'Inserted by MERGE'
ASSERT VALUE product_count = 25 WHERE merge_action = 'Unchanged'
ASSERT VALUE product_count = 12 WHERE merge_action = 'Updated by MERGE'
SELECT CASE
           WHEN last_updated = '2024-06-01' AND id <= 40 THEN 'Updated by MERGE'
           WHEN last_updated = '2024-06-01' AND id > 40 THEN 'Inserted by MERGE'
           ELSE 'Unchanged'
       END AS merge_action,
       COUNT(*) AS product_count
FROM {{zone_name}}.delta_demos.inventory_master
GROUP BY CASE
           WHEN last_updated = '2024-06-01' AND id <= 40 THEN 'Updated by MERGE'
           WHEN last_updated = '2024-06-01' AND id > 40 THEN 'Inserted by MERGE'
           ELSE 'Unchanged'
       END
ORDER BY merge_action;


-- ============================================================================
-- LEARN: WHEN NOT MATCHED — Inserting New Products
-- ============================================================================
-- The WHEN NOT MATCHED clause handles source rows that have no match
-- in the target. These 15 new products (ids 41-55) were inserted in
-- the same atomic commit as the updates and deletes.

ASSERT ROW_COUNT = 15
SELECT id, sku, name, category, price, qty, supplier
FROM {{zone_name}}.delta_demos.inventory_master
WHERE id BETWEEN 41 AND 55
ORDER BY id;


-- ============================================================================
-- EXPLORE: Price Change Impact
-- ============================================================================
-- Let's look at specific items that were updated to see the new prices.
-- The Wireless Mouse dropped from 29.99 to 27.99, while the Keyboard
-- Mechanical went up from 89.99 to 94.99:

ASSERT ROW_COUNT = 4
ASSERT VALUE price = 27.99 WHERE sku = 'ELEC-001'
ASSERT VALUE price = 94.99 WHERE sku = 'ELEC-005'
SELECT sku, name, price, qty, last_updated
FROM {{zone_name}}.delta_demos.inventory_master
WHERE sku IN ('ELEC-001', 'ELEC-005', 'BOOK-001', 'ELEC-007')
ORDER BY sku;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 40 original - 3 deleted + 15 inserted = 52
ASSERT ROW_COUNT = 52
SELECT * FROM {{zone_name}}.delta_demos.inventory_master;

-- Verify deleted_items_gone: 3 discontinued SKUs (qty=0) were removed
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.inventory_master WHERE sku IN ('FURN-004', 'FURN-008', 'SUPP-008');

-- Verify mouse_price_updated: Wireless Mouse price updated to 27.99
ASSERT VALUE price = 27.99
SELECT price FROM {{zone_name}}.delta_demos.inventory_master WHERE sku = 'ELEC-001';

-- Verify sql_book_qty: SQL book quantity updated to 220
ASSERT VALUE qty = 220
SELECT qty FROM {{zone_name}}.delta_demos.inventory_master WHERE sku = 'BOOK-001';

-- Verify new_products_count: 15 new products inserted (ids 41-55)
ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.inventory_master WHERE id BETWEEN 41 AND 55;

-- Verify monitor_unchanged: Monitor price unchanged at 349.99
ASSERT VALUE price = 349.99
SELECT price FROM {{zone_name}}.delta_demos.inventory_master WHERE sku = 'ELEC-006';

-- Verify electronics_count: 20 Electronics products after MERGE
ASSERT VALUE cnt = 20
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.inventory_master WHERE category = 'Electronics';

-- Verify updated_timestamp_count: 27 rows updated/inserted with 2024-06-01
ASSERT VALUE cnt = 27
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.inventory_master WHERE last_updated = '2024-06-01';
