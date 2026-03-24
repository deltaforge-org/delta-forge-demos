-- ============================================================================
-- Delta Table Properties — Configuration Lifecycle — Educational Queries
-- ============================================================================
-- WHAT: Demonstrates the full lifecycle of Delta TBLPROPERTIES — setting them
--       at creation, inspecting them, modifying them, and observing effects.
-- WHY:  Table properties control critical behaviors: change data feed,
--       auto-optimization, checkpoint frequency, and more. Understanding how
--       to manage them is essential for production Delta deployments.
-- HOW:  SHOW TABLE PROPERTIES reads the current config. ALTER TABLE SET/UNSET
--       modifies it. DESCRIBE HISTORY and DESCRIBE DETAIL reveal the effects
--       of these settings on the table's physical layout.
-- ============================================================================


-- ============================================================================
-- LEARN: Inspecting Table Properties
-- ============================================================================
-- SHOW TABLE PROPERTIES reveals every property set on the table.
-- We created this table with enableChangeDataFeed, optimizeWrite, and
-- checkpointInterval. Let's verify they are set.

-- Non-deterministic: property count may vary with engine defaults
ASSERT WARNING ROW_COUNT >= 3
SHOW TABLE PROPERTIES {{zone_name}}.props_demos.inventory_items;


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
FROM {{zone_name}}.props_demos.inventory_items
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
FROM {{zone_name}}.props_demos.inventory_items
GROUP BY warehouse
ORDER BY total_qty DESC;


-- ============================================================================
-- LEARN: Modifying Properties with ALTER TABLE SET
-- ============================================================================
-- Properties can be changed after creation. Here we enable deletion vectors
-- and adjust the target file size for this table.

ALTER TABLE {{zone_name}}.props_demos.inventory_items
SET TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.targetFileSize' = '67108864'
);

-- Verify the new properties are now set
-- Non-deterministic: total property count depends on engine
ASSERT WARNING ROW_COUNT >= 5
SHOW TABLE PROPERTIES {{zone_name}}.props_demos.inventory_items;


-- ============================================================================
-- LEARN: Removing Properties with ALTER TABLE UNSET
-- ============================================================================
-- Properties can also be removed. Let's remove the auto-optimize setting
-- and verify it's gone.

ALTER TABLE {{zone_name}}.props_demos.inventory_items
UNSET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite');

-- Verify properties after UNSET
-- Non-deterministic: total property count depends on engine
ASSERT WARNING ROW_COUNT >= 4
SHOW TABLE PROPERTIES {{zone_name}}.props_demos.inventory_items;


-- ============================================================================
-- LEARN: DESCRIBE HISTORY — See How Properties Affected Operations
-- ============================================================================
-- The transaction log records every operation including ALTER TABLE.
-- This gives a complete timeline of both data changes and config changes.

-- Non-deterministic: timestamps set at write time
ASSERT WARNING ROW_COUNT >= 6
DESCRIBE HISTORY {{zone_name}}.props_demos.inventory_items;


-- ============================================================================
-- LEARN: DESCRIBE DETAIL — Physical Layout Inspection
-- ============================================================================
-- DESCRIBE DETAIL shows how properties translate to physical storage:
-- file counts, sizes, and layout characteristics.

-- Non-deterministic: file sizes depend on engine and compression
ASSERT WARNING ROW_COUNT >= 1
DESCRIBE DETAIL {{zone_name}}.props_demos.inventory_items;


-- ============================================================================
-- LEARN: Time Travel — Compare Before and After Property Effects
-- ============================================================================
-- At Version 1 (initial insert), all 15 items existed.
-- After the DELETE (Version 4), only 13 remain.
-- Time travel works regardless of property changes.

ASSERT VALUE cnt = 15
SELECT COUNT(*) AS cnt
FROM {{zone_name}}.props_demos.inventory_items VERSION AS OF 1;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total rows: 13 items after discontinuation
ASSERT VALUE cnt = 13
SELECT COUNT(*) AS cnt FROM {{zone_name}}.props_demos.inventory_items;

-- Verify distinct categories: 5 categories remain
ASSERT VALUE cnt = 5
SELECT COUNT(DISTINCT category) AS cnt FROM {{zone_name}}.props_demos.inventory_items;

-- Verify distinct warehouses: 3 warehouses
ASSERT VALUE cnt = 3
SELECT COUNT(DISTINCT warehouse) AS cnt FROM {{zone_name}}.props_demos.inventory_items;

-- Verify total quantity across all items
ASSERT VALUE total = 2655
SELECT SUM(quantity) AS total FROM {{zone_name}}.props_demos.inventory_items;

-- Verify items with unit_price > 30 (post price increase)
ASSERT VALUE cnt = 4
SELECT COUNT(*) AS cnt FROM {{zone_name}}.props_demos.inventory_items WHERE unit_price > 30;
