-- ============================================================================
-- Delta Table Properties — Configuration Lifecycle — Educational Queries
-- ============================================================================
-- WHAT: Demonstrates Delta TBLPROPERTIES set at table creation and their
--       observable effects via DESCRIBE HISTORY and DESCRIBE DETAIL.
-- WHY:  Table properties control critical behaviors: change data feed,
--       auto-optimization, checkpoint frequency, and more. Understanding how
--       they affect storage and operations is essential for production Delta.
-- HOW:  Properties were set at CREATE TABLE time (see setup.sql). DESCRIBE
--       HISTORY and DESCRIBE DETAIL reveal their effects on the table's
--       physical layout and transaction log. VERSION AS OF compares states.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Current Inventory After All Operations
-- ============================================================================
-- After the initial load, price increase, restock, and discontinuation,
-- 13 items remain. Let's see the category breakdown.

ASSERT ROW_COUNT = 5
ASSERT VALUE items = 3 WHERE category = 'plumbing'
ASSERT VALUE total_qty = 870 WHERE category = 'plumbing'
SELECT category,
       COUNT(*) AS items,
       SUM(quantity) AS total_qty,
       ROUND(AVG(unit_price), 2) AS avg_price
FROM {{zone_name}}.delta_demos.inventory_items
GROUP BY category
ORDER BY total_qty DESC;


-- ============================================================================
-- EXPLORE: Warehouse Distribution
-- ============================================================================
-- Inventory spread across 3 warehouses after discontinuation.

ASSERT ROW_COUNT = 3
ASSERT VALUE total_qty = 1060 WHERE warehouse = 'warehouse-south'
ASSERT VALUE items = 5 WHERE warehouse = 'warehouse-north'
SELECT warehouse,
       COUNT(*) AS items,
       SUM(quantity) AS total_qty
FROM {{zone_name}}.delta_demos.inventory_items
GROUP BY warehouse
ORDER BY total_qty DESC;


-- ============================================================================
-- LEARN: Modifying Properties with ALTER TABLE SET
-- ============================================================================
-- Properties can be changed after creation. Here we enable deletion vectors
-- and adjust the target file size for this table. These are recorded in the
-- transaction log just like data operations.

ALTER TABLE {{zone_name}}.delta_demos.inventory_items
SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.targetFileSize' = '67108864'
);


-- ============================================================================
-- LEARN: Removing Properties with ALTER TABLE UNSET
-- ============================================================================
-- Properties can also be removed. Let's remove the auto-optimize setting.

ALTER TABLE {{zone_name}}.delta_demos.inventory_items
UNSET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite');


-- ============================================================================
-- LEARN: DESCRIBE HISTORY — See How Properties Affected Operations
-- ============================================================================
-- The transaction log records every operation including ALTER TABLE.
-- This gives a complete timeline of both data changes and config changes.
-- Expected versions: V0 CREATE, V1 INSERT, V2 UPDATE, V3 UPDATE, V4 DELETE,
-- plus ALTER SET and ALTER UNSET operations.

-- Non-deterministic: timestamps set at write time
ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.delta_demos.inventory_items;


-- ============================================================================
-- LEARN: DESCRIBE DETAIL — Physical Layout Inspection
-- ============================================================================
-- DESCRIBE DETAIL shows how properties translate to physical storage:
-- file counts, sizes, and layout characteristics.

-- Non-deterministic: file sizes depend on engine and compression
ASSERT WARNING ROW_COUNT >= 1
DESCRIBE DETAIL {{zone_name}}.delta_demos.inventory_items;


-- ============================================================================
-- LEARN: Time Travel — Compare Before and After Property Effects
-- ============================================================================
-- At Version 1 (initial insert), all 15 items existed.
-- After the DELETE (Version 4), only 13 remain.
-- Time travel works regardless of property changes.

ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.inventory_items VERSION AS OF 1;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total rows: 13 items after discontinuation
ASSERT VALUE cnt = 13
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.inventory_items;

-- Verify distinct categories: 5 categories remain
ASSERT VALUE cnt = 5
SELECT COUNT(DISTINCT category) AS cnt FROM {{zone_name}}.delta_demos.inventory_items;

-- Verify distinct warehouses: 3 warehouses
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT warehouse) AS cnt FROM {{zone_name}}.delta_demos.inventory_items;

-- Verify total quantity across all items
ASSERT VALUE total = 2655
SELECT SUM(quantity) AS total FROM {{zone_name}}.delta_demos.inventory_items;

-- Verify items with unit_price > 30 (post price increase)
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.inventory_items WHERE unit_price > 30;
