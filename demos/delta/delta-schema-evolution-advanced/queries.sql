-- ============================================================================
-- Delta Advanced Schema Evolution — Educational Queries
-- ============================================================================
-- WHAT: Multiple rounds of ALTER TABLE ADD COLUMN can evolve a Delta table's
--       schema incrementally, from 5 columns to 8 across separate phases.
-- WHY:  In production, schemas rarely reach their final form on day one.
--       New business requirements (weight for shipping, discounts for sales,
--       supplier for procurement) arrive in waves over months or years.
-- HOW:  Each ADD COLUMN updates the schema in the transaction log metadata.
--       Old Parquet files are never rewritten — they return NULL for missing
--       columns. Backfill UPDATEs selectively populate historical rows.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Initial 5-Column Schema
-- ============================================================================
-- The product_catalog starts with 5 columns: id, name, category, price, stock.
-- Let's confirm the baseline schema and row count before we evolve it.

ASSERT ROW_COUNT = 5
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'delta_demos'
  AND table_name = 'evolving_product_catalog'
ORDER BY ordinal_position;

ASSERT VALUE baseline_row_count = 30
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS baseline_row_count
FROM {{zone_name}}.delta_demos.evolving_product_catalog;


-- ============================================================================
-- PHASE 1: Add weight_kg and discount_pct columns
-- ============================================================================
-- New business requirement: shipping needs weight, and sales wants to track
-- discount percentages. We add both columns in one phase. This is a
-- metadata-only operation — existing Parquet files are never rewritten.
-- All 30 existing rows will return NULL for these new columns.

ALTER TABLE {{zone_name}}.delta_demos.evolving_product_catalog ADD COLUMN weight_kg DOUBLE;
ALTER TABLE {{zone_name}}.delta_demos.evolving_product_catalog ADD COLUMN discount_pct DOUBLE;

-- Confirm the schema now has 7 columns:
ASSERT ROW_COUNT = 7
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'delta_demos'
  AND table_name = 'evolving_product_catalog'
ORDER BY ordinal_position;

-- All original rows show NULL for the new columns:
ASSERT ROW_COUNT = 5
SELECT id, name, weight_kg, discount_pct
FROM {{zone_name}}.delta_demos.evolving_product_catalog
WHERE id <= 5
ORDER BY id;


-- ============================================================================
-- PHASE 2: Insert 15 new products with weight and discount populated
-- ============================================================================
-- New products inserted after the ADD COLUMN have all 7 columns populated.
-- This creates the first layer of the "evolution timeline" — old rows have
-- NULLs, new rows have values.

ASSERT ROW_COUNT = 15
INSERT INTO {{zone_name}}.delta_demos.evolving_product_catalog VALUES
    (31, 'Wireless Earbuds',      'Electronics',   39.99,  130, 0.15,  0.10),
    (32, 'Docking Station',       'Electronics',   119.99, 55,  0.85,  0.05),
    (33, 'Smart Power Adapter',   'Electronics',   27.99,  170, 0.12,  0.08),
    (34, 'Executive Desk',        'Furniture',     549.99, 15,  45.00, 0.00),
    (35, 'Mesh Office Chair',     'Furniture',     329.99, 25,  12.50, 0.10),
    (36, 'Monitor Arm Dual',      'Furniture',     69.99,  80,  3.20,  0.00),
    (37, 'Fountain Pen',          'Stationery',    34.99,  100, 0.05,  0.00),
    (38, 'Leather Journal',       'Stationery',    29.99,  90,  0.35,  0.05),
    (39, 'Desk Calendar 2025',    'Stationery',    8.99,   200, 0.20,  0.00),
    (40, 'Laptop Backpack',       'Accessories',   59.99,  70,  1.10,  0.12),
    (41, 'USB Desk Fan',          'Accessories',   16.99,  150, 0.45,  0.00),
    (42, 'Blue Light Glasses',    'Accessories',   24.99,  120, 0.04,  0.10),
    (43, 'Wireless Charger',      'Electronics',   19.99,  200, 0.18,  0.12),
    (44, 'Noise Cancel Headset',  'Electronics',   149.99, 45,  0.30,  0.08),
    (45, 'Sit-Stand Mat',         'Furniture',     44.99,  65,  2.80,  0.00);

-- Compare old vs. new rows side by side:
-- ids 1,15 are original (NULL for new columns), ids 31,45 have values
ASSERT ROW_COUNT = 4
ASSERT VALUE weight_kg IS NULL WHERE id = 1
ASSERT VALUE weight_kg IS NULL WHERE id = 15
ASSERT VALUE weight_kg = 0.15 WHERE id = 31
ASSERT VALUE weight_kg = 2.80 WHERE id = 45
ASSERT VALUE discount_pct = 0.10 WHERE id = 31
SELECT id, name, weight_kg, discount_pct
FROM {{zone_name}}.delta_demos.evolving_product_catalog
WHERE id IN (1, 15, 31, 45)
ORDER BY id;


-- ============================================================================
-- PHASE 3: Backfill weight_kg for 10 original products (ids 1-10)
-- ============================================================================
-- Not all historical rows need backfilling. Here we selectively populate
-- weight_kg for the first 10 high-priority products. The remaining 20
-- original rows keep their NULLs — this is common in production when
-- historical data is partially available.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 0.08  WHERE id = 1;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 0.95  WHERE id = 2;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 0.12  WHERE id = 3;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 2.50  WHERE id = 4;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 0.20  WHERE id = 5;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 15.00 WHERE id = 6;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 25.00 WHERE id = 7;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 18.00 WHERE id = 8;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 1.20  WHERE id = 9;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET weight_kg = 22.00 WHERE id = 10;

-- Verify the backfill: ids 1-10 now have weight, ids 11-30 still NULL:
ASSERT ROW_COUNT = 5
SELECT id, name, weight_kg
FROM {{zone_name}}.delta_demos.evolving_product_catalog
WHERE id IN (1, 5, 10, 11, 20)
ORDER BY id;


-- ============================================================================
-- PHASE 4: Set discount for Electronics products (ids 1-5)
-- ============================================================================
-- A targeted promotion: set discount_pct = 0.15 for the first 5 Electronics
-- products. This demonstrates category-based UPDATEs on evolved columns.

ASSERT ROW_COUNT = 1
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET discount_pct = 0.15 WHERE id = 1;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET discount_pct = 0.15 WHERE id = 2;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET discount_pct = 0.15 WHERE id = 3;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET discount_pct = 0.15 WHERE id = 4;
UPDATE {{zone_name}}.delta_demos.evolving_product_catalog SET discount_pct = 0.15 WHERE id = 5;

-- Confirm discounts are set for ids 1-5 but not for ids 6-10:
ASSERT ROW_COUNT = 10
SELECT id, name, category, discount_pct
FROM {{zone_name}}.delta_demos.evolving_product_catalog
WHERE id BETWEEN 1 AND 10
ORDER BY id;


-- ============================================================================
-- PHASE 5: Add supplier column — another evolution
-- ============================================================================
-- Procurement needs supplier tracking. A third ADD COLUMN extends the schema
-- to 8 columns. Again, this is metadata-only and instantaneous.

ALTER TABLE {{zone_name}}.delta_demos.evolving_product_catalog ADD COLUMN supplier VARCHAR;

-- Confirm the schema now has 8 columns:
ASSERT ROW_COUNT = 8
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'delta_demos'
  AND table_name = 'evolving_product_catalog'
ORDER BY ordinal_position;


-- ============================================================================
-- PHASE 6: Insert 5 newest products with all 8 columns populated
-- ============================================================================
-- The final batch of products arrives after all schema evolutions are
-- complete, so every column is populated — including supplier.

ASSERT ROW_COUNT = 5
INSERT INTO {{zone_name}}.delta_demos.evolving_product_catalog VALUES
    (46, 'Smart Desk Lamp',       'Electronics',   64.99,  95,  0.55,  0.10, 'TechGlow Inc'),
    (47, 'Bamboo Desk Shelf',     'Furniture',     39.99,  50,  3.00,  0.05, 'EcoOffice Co'),
    (48, 'Calligraphy Set',       'Stationery',    42.99,  75,  0.60,  0.00, 'ArtWrite Ltd'),
    (49, 'Ergonomic Wrist Pad',   'Accessories',   21.99,  110, 0.25,  0.08, 'ComfortPlus'),
    (50, 'Thunderbolt Cable 2m',  'Electronics',   34.99,  160, 0.10,  0.12, 'CableWorks');


-- ============================================================================
-- LEARN: The Evolution Timeline Creates Layered NULLs
-- ============================================================================
-- Each phase of evolution leaves a distinct NULL pattern:
--
-- Original (ids 1-30):   weight_kg/discount_pct added later via ALTER TABLE.
--   - ids 1-10:  weight_kg backfilled, ids 1-5 got discount_pct = 0.15
--   - ids 11-30: weight_kg = NULL, discount_pct = NULL
-- Phase 2 (ids 31-45):  Inserted with weight + discount, supplier = NULL
-- Phase 6 (ids 46-50):  All 8 columns populated (including supplier)
--
-- This layering tells the story of when each column was introduced.

ASSERT ROW_COUNT = 5
ASSERT VALUE rows = 20 WHERE evolution_phase = 'Phase 1: no backfill'
ASSERT VALUE has_weight = 0 WHERE evolution_phase = 'Phase 1: no backfill'
ASSERT VALUE rows = 5 WHERE evolution_phase = 'Phase 7: fully populated'
ASSERT VALUE has_supplier = 5 WHERE evolution_phase = 'Phase 7: fully populated'
SELECT
    CASE
        WHEN id BETWEEN 1 AND 5  THEN 'Phase 1: backfill weight + discount'
        WHEN id BETWEEN 6 AND 10 THEN 'Phase 1: backfill weight only'
        WHEN id BETWEEN 11 AND 30 THEN 'Phase 1: no backfill'
        WHEN id BETWEEN 31 AND 45 THEN 'Phase 3: weight + discount'
        ELSE 'Phase 7: fully populated'
    END AS evolution_phase,
    COUNT(*) AS rows,
    COUNT(weight_kg) AS has_weight,
    COUNT(discount_pct) AS has_discount,
    COUNT(supplier) AS has_supplier
FROM {{zone_name}}.delta_demos.evolving_product_catalog
GROUP BY CASE
    WHEN id BETWEEN 1 AND 5  THEN 'Phase 1: backfill weight + discount'
    WHEN id BETWEEN 6 AND 10 THEN 'Phase 1: backfill weight only'
    WHEN id BETWEEN 11 AND 30 THEN 'Phase 1: no backfill'
    WHEN id BETWEEN 31 AND 45 THEN 'Phase 3: weight + discount'
    ELSE 'Phase 7: fully populated'
END
ORDER BY evolution_phase;


-- ============================================================================
-- LEARN: Backfill Strategy — Partial vs. Complete
-- ============================================================================
-- Not all historical rows need backfilling. In this demo:
--   - weight_kg was backfilled for the first 10 products (high-priority items)
--   - discount_pct was set to 0.15 for only 5 Electronics items (a promotion)
--   - supplier was never backfilled for any old rows
--
-- This selective approach is common in production: you backfill what you can
-- and accept NULLs where historical data is unavailable.

ASSERT ROW_COUNT = 5
ASSERT VALUE weight_kg = '(NULL)' WHERE id = 15
ASSERT VALUE supplier = 'CableWorks' WHERE id = 50
SELECT id, name, category,
       CASE WHEN weight_kg IS NULL THEN '(NULL)' ELSE CAST(weight_kg AS VARCHAR) END AS weight_kg,
       CASE WHEN discount_pct IS NULL THEN '(NULL)' ELSE CAST(discount_pct AS VARCHAR) END AS discount_pct,
       CASE WHEN supplier IS NULL THEN '(NULL)' ELSE supplier END AS supplier
FROM {{zone_name}}.delta_demos.evolving_product_catalog
WHERE id IN (1, 6, 15, 35, 50)
ORDER BY id;


-- ============================================================================
-- LEARN: Column Addition is Metadata-Only
-- ============================================================================
-- When ALTER TABLE ADD COLUMN runs, only the _delta_log is updated with the
-- new schema. The existing Parquet files on disk are untouched. This makes
-- ADD COLUMN instantaneous regardless of table size — whether your table has
-- 50 rows or 50 billion rows, the operation takes the same time.
--
-- The cost is paid at read time: the query engine must recognize missing
-- columns and fill them with NULL. Modern columnar engines handle this
-- efficiently through schema-on-read.

ASSERT VALUE null_weight = 20
ASSERT VALUE null_discount = 25
ASSERT VALUE null_supplier = 45
ASSERT VALUE total = 50
ASSERT ROW_COUNT = 1
SELECT COUNT(*) FILTER (WHERE weight_kg IS NULL) AS null_weight,
       COUNT(*) FILTER (WHERE discount_pct IS NULL) AS null_discount,
       COUNT(*) FILTER (WHERE supplier IS NULL) AS null_supplier,
       COUNT(*) AS total
FROM {{zone_name}}.delta_demos.evolving_product_catalog;


-- ============================================================================
-- EXPLORE: Category-Level Analysis Across Evolution Phases
-- ============================================================================
-- Despite the schema growing over time, analytics on original columns
-- (price, stock, category) work identically across all rows.

ASSERT ROW_COUNT = 4
ASSERT VALUE products = 16 WHERE category = 'Electronics'
ASSERT VALUE total_stock = 1915 WHERE category = 'Electronics'
-- Non-deterministic: ROUND(AVG(DOUBLE)) may vary by floating-point accumulation order across platforms
ASSERT WARNING VALUE avg_price BETWEEN 56.47 AND 56.51 WHERE category = 'Electronics'
SELECT category,
       COUNT(*) AS products,
       ROUND(AVG(price), 2) AS avg_price,
       SUM(stock) AS total_stock,
       COUNT(weight_kg) AS with_weight,
       COUNT(supplier) AS with_supplier
FROM {{zone_name}}.delta_demos.evolving_product_catalog
GROUP BY category
ORDER BY category;


-- ============================================================================
-- EXPLORE: The Newest Products — All Columns Populated
-- ============================================================================
-- Products from Phase 6 (ids 46-50) were inserted after all 3 ADD COLUMN
-- operations, so they have every field populated including supplier.

ASSERT ROW_COUNT = 5
SELECT id, name, category, price, stock, weight_kg, discount_pct, supplier
FROM {{zone_name}}.delta_demos.evolving_product_catalog
WHERE id >= 46
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 50
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.evolving_product_catalog;

-- Verify 20 rows still have NULL weight (ids 11-30 never backfilled)
ASSERT VALUE null_weight_count = 20
SELECT COUNT(*) FILTER (WHERE weight_kg IS NULL) AS null_weight_count FROM {{zone_name}}.delta_demos.evolving_product_catalog;

-- Verify 30 rows have weight populated
ASSERT VALUE with_weight_count = 30
SELECT COUNT(*) FILTER (WHERE weight_kg IS NOT NULL) AS with_weight_count FROM {{zone_name}}.delta_demos.evolving_product_catalog;

-- Verify 5 products have discount_pct = 0.15 (Electronics promotion)
ASSERT VALUE electronics_discount = 5
SELECT COUNT(*) FILTER (WHERE discount_pct = 0.15) AS electronics_discount FROM {{zone_name}}.delta_demos.evolving_product_catalog;

-- Verify 45 rows have NULL supplier (only ids 46-50 have supplier)
ASSERT VALUE supplier_null_count = 45
SELECT COUNT(*) FILTER (WHERE supplier IS NULL) AS supplier_null_count FROM {{zone_name}}.delta_demos.evolving_product_catalog;

-- Verify schema has 8 columns after all ADD COLUMN operations
ASSERT VALUE column_count = 8
SELECT COUNT(*) AS column_count FROM information_schema.columns
WHERE table_schema = 'delta_demos' AND table_name = 'evolving_product_catalog';

-- Verify original product price is unchanged
ASSERT VALUE price = 29.99
SELECT price FROM {{zone_name}}.delta_demos.evolving_product_catalog WHERE id = 1;

-- Verify newest product (id=50) has all columns populated
ASSERT VALUE newest_product_complete = 1
SELECT COUNT(*) AS newest_product_complete FROM {{zone_name}}.delta_demos.evolving_product_catalog
WHERE id = 50
  AND name IS NOT NULL AND category IS NOT NULL
  AND price IS NOT NULL AND stock IS NOT NULL
  AND weight_kg IS NOT NULL AND discount_pct IS NOT NULL
  AND supplier IS NOT NULL;
