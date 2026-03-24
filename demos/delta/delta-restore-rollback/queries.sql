-- ============================================================================
-- Delta RESTORE — Rollback to Previous Versions — Educational Queries
-- ============================================================================
-- WHAT: RESTORE rewinds a Delta table to any previous version in its history.
-- WHY:  Accidental DELETEs, bad UPDATEs, or corrupt loads can be undone
--       instantly without backup restores, reprocessing, or downtime.
-- HOW:  RESTORE writes a NEW commit whose actions replicate the target
--       version's snapshot. The old versions remain intact in the log, so
--       RESTORE itself is also reversible.
--
-- Version history we will build:
--   V0: CREATE empty delta table (done in setup.sql)
--   V1: INSERT 30 products (done in setup.sql)
--   V2: UPDATE — 10% price increase for Electronics
--   V3: UPDATE — set status='discontinued' for 5 items
--   V4: DELETE — remove discontinued items (ACCIDENT!)
--   V5: RESTORE TO VERSION 3 — undo the accidental delete
--   V6: UPDATE — reactivate recovered items with clearance discount
-- ============================================================================


-- ============================================================================
-- EXPLORE: V0 Baseline — All 30 Products
-- ============================================================================
-- The setup script inserted 30 products across 6 categories.
-- All items are active, at their original prices.

ASSERT ROW_COUNT = 30
SELECT id, name, category, price, qty, status
FROM {{zone_name}}.delta_demos.product_inventory
ORDER BY category, id;

ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 30
ASSERT VALUE categories = 5
ASSERT VALUE active_count = 30
SELECT COUNT(*) AS total_products,
       COUNT(DISTINCT category) AS categories,
       COUNT(*) FILTER (WHERE status = 'active') AS active_count
FROM {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- V1: UPDATE — 10% price increase for Electronics
-- ============================================================================
-- Business decision: raise all Electronics prices by 10%.
-- This creates version 1 in the Delta transaction log.

ASSERT ROW_COUNT = 6
UPDATE {{zone_name}}.delta_demos.product_inventory
SET price = ROUND(price * 1.10, 2)
WHERE category = 'Electronics';

-- Verify V1: Electronics prices should be 10% higher
ASSERT ROW_COUNT = 6
SELECT id, name, category, price, status
FROM {{zone_name}}.delta_demos.product_inventory
WHERE category = 'Electronics'
ORDER BY id;


-- ============================================================================
-- V2: UPDATE — Discontinue 5 slow-moving items
-- ============================================================================
-- Mark 5 items with low sales as 'discontinued':
--   id=8  Standing Desk (low qty=15)
--   id=11 Filing Cabinet
--   id=23 Sound Bar (low qty=20)
--   id=24 DAC Amplifier
--   id=29 Screen Cleaner

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.delta_demos.product_inventory
SET status = 'discontinued'
WHERE id IN (8, 11, 23, 24, 29);

-- Verify V2: 5 items should now be discontinued, 25 still active
ASSERT ROW_COUNT = 2
ASSERT VALUE item_count = 25 WHERE status = 'active'
ASSERT VALUE item_count = 5 WHERE status = 'discontinued'
SELECT status, COUNT(*) AS item_count
FROM {{zone_name}}.delta_demos.product_inventory
GROUP BY status;

ASSERT ROW_COUNT = 5
SELECT id, name, category, price, status
FROM {{zone_name}}.delta_demos.product_inventory
WHERE status = 'discontinued'
ORDER BY id;


-- ============================================================================
-- V3: DELETE — ACCIDENTAL deletion of discontinued items!
-- ============================================================================
-- Someone runs a cleanup query that permanently removes all discontinued
-- items. This is a MISTAKE — we still needed those rows for reporting
-- and potential reactivation.

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.product_inventory
WHERE status = 'discontinued';

-- Verify V3: Only 25 rows remain — the 5 discontinued items are GONE
ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 25
SELECT COUNT(*) AS total_products,
       COUNT(DISTINCT category) AS categories
FROM {{zone_name}}.delta_demos.product_inventory;

-- The deleted items are no longer in the table:
ASSERT ROW_COUNT = 0
SELECT id, name
FROM {{zone_name}}.delta_demos.product_inventory
WHERE id IN (8, 11, 23, 24, 29);


-- ============================================================================
-- V4: RESTORE TO VERSION 3 — Undo the accidental delete
-- ============================================================================
-- RESTORE rolls the table back to V3 state where discontinued items still
-- exist. All 30 rows are recovered, with V2 price increases AND V3
-- discontinuation status intact.
--
-- Under the hood, Delta:
--   1. Reads the snapshot metadata at version 3
--   2. Computes the diff between current state and version 3
--   3. Writes a NEW commit (V5) with Add/Remove actions to recreate V3's state
--   4. The transaction log now has V0..V5 — nothing is deleted
--
-- This means RESTORE is safe: it is just another commit. You can even
-- RESTORE away a RESTORE if you change your mind.

RESTORE {{zone_name}}.delta_demos.product_inventory TO VERSION 3;

-- Verify V4: All 30 rows are back, 5 still marked as discontinued
ASSERT ROW_COUNT = 1
ASSERT VALUE total_products = 30
ASSERT VALUE active_count = 25
ASSERT VALUE discontinued_count = 5
SELECT COUNT(*) AS total_products,
       COUNT(*) FILTER (WHERE status = 'active') AS active_count,
       COUNT(*) FILTER (WHERE status = 'discontinued') AS discontinued_count
FROM {{zone_name}}.delta_demos.product_inventory;

-- The recovered items are present again:
ASSERT ROW_COUNT = 5
SELECT id, name, category, price, status
FROM {{zone_name}}.delta_demos.product_inventory
WHERE id IN (8, 11, 23, 24, 29)
ORDER BY id;


-- ============================================================================
-- V5: UPDATE — Reactivate recovered items with clearance discount
-- ============================================================================
-- Now that we've recovered the data, instead of deleting discontinued items,
-- reactivate them with a 25% clearance discount to move the stock.

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.delta_demos.product_inventory
SET status = 'active',
    price = ROUND(price * 0.75, 2)
WHERE status = 'discontinued';

-- Verify V5: All 30 products are now active
ASSERT ROW_COUNT = 1
ASSERT VALUE item_count = 30
SELECT status, COUNT(*) AS item_count
FROM {{zone_name}}.delta_demos.product_inventory
GROUP BY status;

-- The recovered items now have clearance prices:
ASSERT ROW_COUNT = 5
SELECT id, name, category, price, status
FROM {{zone_name}}.delta_demos.product_inventory
WHERE id IN (8, 11, 23, 24, 29)
ORDER BY id;


-- ============================================================================
-- LEARN: RESTORE Preserves All Previous Versions
-- ============================================================================
-- A common misconception is that RESTORE erases versions. It does not.
-- The version log is append-only. After RESTORE TO VERSION 3, the history is:
--   V0 -> V1 -> V2 -> V3 -> V4 (accidental delete) -> V5 (RESTORE) -> V6 (update)
-- You could still travel back to V4 to see the accidentally-deleted state.

ASSERT ROW_COUNT = 1
ASSERT VALUE current_row_count = 30
SELECT 'All 30 products restored and active' AS observation,
       COUNT(*) AS current_row_count
FROM {{zone_name}}.delta_demos.product_inventory;


-- ============================================================================
-- EXPLORE: Spot-Check Key Price Transformations
-- ============================================================================
-- Laptop Pro (id=1, Electronics): 1299.99 * 1.10 = 1429.99 (V1 price increase)
-- Standing Desk (id=8, Furniture): 599.99 * 0.75 = 449.99 (V5 clearance)
-- Notebook A5 (id=13, Stationery): unchanged at 5.99

ASSERT ROW_COUNT = 3
ASSERT VALUE price = 1429.99 WHERE id = 1
ASSERT VALUE price = 449.99 WHERE id = 8
ASSERT VALUE price = 5.99 WHERE id = 13
SELECT id, name, category, price,
       CASE
           WHEN id = 1  THEN 'Electronics +10%'
           WHEN id = 8  THEN 'Recovered, clearance -25%'
           WHEN id = 13 THEN 'Never modified'
       END AS price_explanation
FROM {{zone_name}}.delta_demos.product_inventory
WHERE id IN (1, 8, 13)
ORDER BY id;


-- ============================================================================
-- EXPLORE: Category Price Summary
-- ============================================================================
-- Electronics got a 10% price increase in V2, which persisted through RESTORE.
-- The 5 recovered items (non-Electronics) got a 25% clearance discount in V6.
-- Other items remained unchanged throughout.

ASSERT ROW_COUNT = 5
SELECT category,
       COUNT(*) AS item_count,
       ROUND(MIN(price), 2) AS min_price,
       ROUND(MAX(price), 2) AS max_price,
       ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.delta_demos.product_inventory
GROUP BY category
ORDER BY category;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: all 30 products restored and active
ASSERT ROW_COUNT = 30
SELECT * FROM {{zone_name}}.delta_demos.product_inventory;

-- Verify all_active: no non-active products remain after reactivation
ASSERT VALUE cnt = 0
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_inventory WHERE status != 'active';

-- Verify recovered_items: 5 previously discontinued items recovered by RESTORE
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.product_inventory WHERE id IN (8, 11, 23, 24, 29);

-- Verify laptop_price: Laptop Pro price = 1299.99 * 1.10 = 1429.99 (V1 increase)
ASSERT VALUE price = 1429.99
SELECT price FROM {{zone_name}}.delta_demos.product_inventory WHERE id = 1;

-- Verify clearance_price: Standing Desk price = 599.99 * 0.75 = 449.99 (V5 clearance)
ASSERT VALUE price = 449.99
SELECT price FROM {{zone_name}}.delta_demos.product_inventory WHERE id = 8;

-- Verify stationery_unchanged: Notebook A5 price never modified
ASSERT VALUE price = 5.99
SELECT price FROM {{zone_name}}.delta_demos.product_inventory WHERE id = 13;

-- Verify category_count: all 5 categories present
ASSERT VALUE cnt = 5
SELECT COUNT(DISTINCT category) AS cnt FROM {{zone_name}}.delta_demos.product_inventory;

-- Verify dac_clearance: DAC Amplifier price = 129.99 * 0.75 = 97.49 (V5 clearance)
ASSERT VALUE price = 97.49
SELECT price FROM {{zone_name}}.delta_demos.product_inventory WHERE id = 24;
