-- ============================================================================
-- Delta Constraints Lifecycle — Educational Queries
-- ============================================================================
-- WHAT: CHECK constraints (delta.constraints.*) are expressions stored in the
--       Delta transaction log. Every writer must evaluate them before committing.
-- WHY:  Constraints guarantee data invariants survive every mutation — not just
--       the initial load, but UPDATEs, DELETEs, and schema changes too.
-- HOW:  This demo proves it: we mutate the table three times and re-validate
--       constraints after each step, showing they hold through the full lifecycle.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Browse the Product Catalog
-- ============================================================================
-- The products table has three CHECK constraints:
--   price_positive:        price > 0
--   stock_non_negative:    stock >= 0    (zero is allowed — for discontinued items)
--   discount_non_negative: discount >= 0
--
-- Every row must satisfy all three expressions. Let's inspect the data.

ASSERT ROW_COUNT = 10
SELECT id, name, category, price, stock, discount
FROM {{zone_name}}.delta_demos.products
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: Validate All Constraints Hold on Baseline Data
-- ============================================================================
-- Before any mutations, confirm zero violations across all three constraints.
-- We also verify the total product count. This is our baseline checkpoint.

ASSERT VALUE total_products = 20
ASSERT VALUE price_violations = 0
ASSERT VALUE stock_violations = 0
ASSERT VALUE discount_violations = 0
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) AS total_products,
    COUNT(*) FILTER (WHERE price <= 0) AS price_violations,
    COUNT(*) FILTER (WHERE stock < 0) AS stock_violations,
    COUNT(*) FILTER (WHERE discount < 0) AS discount_violations
FROM {{zone_name}}.delta_demos.products;


-- ============================================================================
-- EXPLORE: Category Summary — Pre-Update Baseline
-- ============================================================================
-- Since price > 0 is guaranteed, every SUM and AVG is meaningful — no negative
-- prices can pollute the aggregation.

ASSERT ROW_COUNT = 6
ASSERT VALUE item_count = 4 WHERE category = 'Electronics'
ASSERT VALUE item_count = 4 WHERE category = 'Tools'
ASSERT VALUE item_count = 2 WHERE category = 'Books'
SELECT category,
       COUNT(*) AS item_count,
       ROUND(SUM(price), 2) AS total_price,
       ROUND(AVG(price), 2) AS avg_price,
       ROUND(MIN(price), 2) AS min_price,
       ROUND(MAX(price), 2) AS max_price
FROM {{zone_name}}.delta_demos.products
GROUP BY category
ORDER BY total_price DESC;


-- ============================================================================
-- STEP: Bulk Price Increase — 10% Across All Products
-- ============================================================================
-- A global price adjustment. Because price > 0 is enforced, multiplying by
-- 1.10 can never produce a non-positive result. The constraint is inherently
-- safe for multiplicative increases on positive values.

ASSERT ROW_COUNT = 20
UPDATE {{zone_name}}.delta_demos.products
SET price = ROUND(price * 1.10, 2);


-- ============================================================================
-- LEARN: Re-Validate Constraints After Price Update
-- ============================================================================
-- This is the key educational moment: constraints survive DML. The 10% increase
-- preserved all three invariants. In a real Delta table, if any UPDATE produced
-- a row with price <= 0, the entire transaction would be rejected.

ASSERT VALUE total_products = 20
ASSERT VALUE price_violations = 0
ASSERT VALUE stock_violations = 0
ASSERT VALUE discount_violations = 0
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) AS total_products,
    COUNT(*) FILTER (WHERE price <= 0) AS price_violations,
    COUNT(*) FILTER (WHERE stock < 0) AS stock_violations,
    COUNT(*) FILTER (WHERE discount < 0) AS discount_violations
FROM {{zone_name}}.delta_demos.products;


-- ============================================================================
-- STEP: Discontinue Items — Set Stock to Zero
-- ============================================================================
-- Setting stock = 0 is allowed because the constraint is stock >= 0 (not > 0).
-- This is a deliberate design choice: zero stock means "discontinued", while
-- negative stock would be an inventory error.

ASSERT ROW_COUNT = 3
UPDATE {{zone_name}}.delta_demos.products
SET stock = 0
WHERE id IN (2, 8, 14);


-- ============================================================================
-- LEARN: Verify Boundary Behavior — Zero Stock Is Valid
-- ============================================================================
-- Constraint stock >= 0 allows the boundary value. This is an important
-- distinction: the constraint protects against negative inventory without
-- preventing items from being marked as out-of-stock.

ASSERT VALUE zero_stock_items = 3
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS zero_stock_items
FROM {{zone_name}}.delta_demos.products
WHERE stock = 0;


-- ============================================================================
-- STEP: Remove Discontinued Products
-- ============================================================================
-- Delete the zero-stock items. This is safe — the constraint on the remaining
-- rows is unaffected by removing rows.

ASSERT ROW_COUNT = 3
DELETE FROM {{zone_name}}.delta_demos.products
WHERE stock = 0;


-- ============================================================================
-- LEARN: Final Constraint Validation After All Mutations
-- ============================================================================
-- After INSERT → UPDATE (price) → UPDATE (stock) → DELETE, all constraints
-- still hold on the remaining 17 products. This proves constraints are durable
-- through the full DML lifecycle.

ASSERT VALUE total_products = 17
ASSERT VALUE price_violations = 0
ASSERT VALUE stock_violations = 0
ASSERT VALUE discount_violations = 0
ASSERT ROW_COUNT = 1
SELECT
    COUNT(*) AS total_products,
    COUNT(*) FILTER (WHERE price <= 0) AS price_violations,
    COUNT(*) FILTER (WHERE stock < 0) AS stock_violations,
    COUNT(*) FILTER (WHERE discount < 0) AS discount_violations
FROM {{zone_name}}.delta_demos.products;


-- ============================================================================
-- EXPLORE: Post-DML Category Summary
-- ============================================================================
-- Compare with the pre-update summary: prices are 10% higher, three items
-- were removed (Widget B from Tools, Shirt Blue from Clothing, Mug Fancy
-- from Home), but all categories still have at least one product.

ASSERT ROW_COUNT = 6
ASSERT VALUE item_count = 4 WHERE category = 'Electronics'
ASSERT VALUE item_count = 3 WHERE category = 'Tools'
ASSERT VALUE item_count = 3 WHERE category = 'Clothing'
ASSERT VALUE item_count = 3 WHERE category = 'Home'
SELECT category,
       COUNT(*) AS item_count,
       ROUND(SUM(price), 2) AS total_price,
       ROUND(AVG(price), 2) AS avg_price
FROM {{zone_name}}.delta_demos.products
GROUP BY category
ORDER BY total_price DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify final row count
ASSERT ROW_COUNT = 17
SELECT * FROM {{zone_name}}.delta_demos.products;

-- Verify price constraint holds (all positive)
ASSERT VALUE price_violations = 0
SELECT COUNT(*) FILTER (WHERE price <= 0) AS price_violations FROM {{zone_name}}.delta_demos.products;

-- Verify stock constraint holds (all non-negative, and no zeros remain)
ASSERT VALUE stock_violations = 0
SELECT COUNT(*) FILTER (WHERE stock < 0) AS stock_violations FROM {{zone_name}}.delta_demos.products;

-- Verify discount constraint holds
ASSERT VALUE discount_violations = 0
SELECT COUNT(*) FILTER (WHERE discount < 0) AS discount_violations FROM {{zone_name}}.delta_demos.products;

-- Verify specific price after 10% increase (Widget A: 25.00 → 27.50)
ASSERT VALUE price = 27.5 WHERE id = 1
SELECT id, name, price FROM {{zone_name}}.delta_demos.products WHERE id IN (1, 3);

-- Verify deleted items are gone
ASSERT ROW_COUNT = 0
SELECT * FROM {{zone_name}}.delta_demos.products WHERE id IN (2, 8, 14);

-- Verify all 6 categories survived the deletions
ASSERT VALUE category_count = 6
SELECT COUNT(DISTINCT category) AS category_count FROM {{zone_name}}.delta_demos.products;

-- Verify discounted items count
ASSERT VALUE discounted_count = 8
SELECT COUNT(*) AS discounted_count FROM {{zone_name}}.delta_demos.products WHERE discount > 0;
