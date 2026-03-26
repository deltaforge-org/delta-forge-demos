-- ============================================================================
-- Iceberg UniForm Verification — Queries
-- ============================================================================
-- Each section is a separate query. Operations build on each other.
-- After all queries pass, run verify.py to check Iceberg metadata.
-- ============================================================================


-- ============================================================================
-- A1: Products — Baseline (10 rows)
-- ============================================================================

ASSERT ROW_COUNT = 10
SELECT * FROM {{zone_name}}.iceberg_verify.products ORDER BY id;


-- ============================================================================
-- A2: Products — Category breakdown
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 3 WHERE category = 'Electronics'
ASSERT VALUE cnt = 3 WHERE category = 'Furniture'
ASSERT VALUE cnt = 4 WHERE category = 'Audio'
SELECT category, COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_verify.products
GROUP BY category ORDER BY category;


-- ============================================================================
-- A3: Products — UPDATE Electronics prices +10%
-- ============================================================================

UPDATE {{zone_name}}.iceberg_verify.products
SET price = ROUND(price * 1.10, 2)
WHERE category = 'Electronics';


-- ============================================================================
-- A4: Products — Verify updated prices
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE price = 1099.99 WHERE name = 'Laptop'
ASSERT VALUE price = 32.99 WHERE name = 'Mouse'
ASSERT VALUE price = 54.99 WHERE name = 'Hub'
SELECT name, ROUND(price, 2) AS price
FROM {{zone_name}}.iceberg_verify.products
WHERE category = 'Electronics' ORDER BY id;


-- ============================================================================
-- A5: Products — DELETE inactive
-- ============================================================================

DELETE FROM {{zone_name}}.iceberg_verify.products WHERE is_active = false;


-- ============================================================================
-- A6: Products — Verify 8 rows remain
-- ============================================================================

ASSERT ROW_COUNT = 8
SELECT * FROM {{zone_name}}.iceberg_verify.products ORDER BY id;


-- ============================================================================
-- A7: Products — INSERT 2 new
-- ============================================================================

INSERT INTO {{zone_name}}.iceberg_verify.products VALUES
    (11, 'Webcam',  'Electronics', 69.99, 85, true),
    (12, 'Footrest','Furniture',   29.99, 300, true);


-- ============================================================================
-- A8: Products — Final state (10 rows)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE cnt = 10
ASSERT VALUE total_value = 2747.90
SELECT COUNT(*) AS cnt, ROUND(SUM(price), 2) AS total_value
FROM {{zone_name}}.iceberg_verify.products;


-- ============================================================================
-- B1: Sales — Baseline (12 rows, 3 regions)
-- ============================================================================

ASSERT ROW_COUNT = 12
SELECT * FROM {{zone_name}}.iceberg_verify.sales ORDER BY id;


-- ============================================================================
-- B2: Sales — Per-region counts
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE cnt = 4 WHERE region = 'us-east'
ASSERT VALUE cnt = 4 WHERE region = 'us-west'
ASSERT VALUE cnt = 4 WHERE region = 'eu-west'
SELECT region, COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_verify.sales
GROUP BY region ORDER BY region;


-- ============================================================================
-- B3: Sales — UPDATE Q2 amounts +5%
-- ============================================================================

UPDATE {{zone_name}}.iceberg_verify.sales
SET amount = ROUND(amount * 1.05, 2)
WHERE quarter = 'Q2';


-- ============================================================================
-- B4: Sales — Verify Q2 amounts
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE amount = 157.50 WHERE id = 3
ASSERT VALUE amount = 315.00 WHERE id = 4
ASSERT VALUE amount = 94.50 WHERE id = 7
ASSERT VALUE amount = 262.50 WHERE id = 8
ASSERT VALUE amount = 136.50 WHERE id = 11
ASSERT VALUE amount = 283.50 WHERE id = 12
SELECT id, ROUND(amount, 2) AS amount
FROM {{zone_name}}.iceberg_verify.sales
WHERE quarter = 'Q2' ORDER BY id;


-- ============================================================================
-- B5: Sales — DELETE eu-west low amount
-- ============================================================================

DELETE FROM {{zone_name}}.iceberg_verify.sales
WHERE region = 'eu-west' AND amount < 120;


-- ============================================================================
-- B6: Sales — Verify rows after delete
-- ============================================================================
ASSERT ROW_COUNT = 11
SELECT * FROM {{zone_name}}.iceberg_verify.sales ORDER BY id;


-- ============================================================================
-- B7: Sales — INSERT Q3 rows
-- ============================================================================

INSERT INTO {{zone_name}}.iceberg_verify.sales VALUES
    (13, 'Widget', 'us-east', 'Q3', 175.00, 10),
    (14, 'Gadget', 'us-west', 'Q3', 225.00, 8);


-- ============================================================================
-- B8: Sales — Final state (13 rows)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE cnt = 13
ASSERT VALUE total_amount = 2469.50
SELECT COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total_amount
FROM {{zone_name}}.iceberg_verify.sales;


-- ============================================================================
-- C1: Evolve — Baseline (5 rows, 3 columns)
-- ============================================================================

ASSERT ROW_COUNT = 5
SELECT * FROM {{zone_name}}.iceberg_verify.evolve ORDER BY id;


-- ============================================================================
-- C2: Evolve — ADD COLUMN category
-- ============================================================================

ALTER TABLE {{zone_name}}.iceberg_verify.evolve ADD COLUMN category VARCHAR;


-- ============================================================================
-- C3: Evolve — Verify NULL backfill
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE has_category = 0
SELECT COUNT(category) AS has_category
FROM {{zone_name}}.iceberg_verify.evolve;


-- ============================================================================
-- C4a: Evolve — Backfill group-a
-- ============================================================================

UPDATE {{zone_name}}.iceberg_verify.evolve
SET category = 'group-a'
WHERE id <= 3;


-- ============================================================================
-- C4b: Evolve — Backfill group-b
-- ============================================================================

UPDATE {{zone_name}}.iceberg_verify.evolve
SET category = 'group-b'
WHERE id > 3;


-- ============================================================================
-- C5: Evolve — INSERT with full schema
-- ============================================================================

INSERT INTO {{zone_name}}.iceberg_verify.evolve
SELECT * FROM (VALUES
    (6, 'Zeta',  60.0, 'group-a'),
    (7, 'Eta',   70.0, 'group-b'),
    (8, 'Theta', 80.0, 'group-a')
) AS t(id, name, value, category);


-- ============================================================================
-- C6: Evolve — Verify 8 rows, all have category
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE cnt = 8
ASSERT VALUE has_category = 8
SELECT COUNT(*) AS cnt, COUNT(category) AS has_category
FROM {{zone_name}}.iceberg_verify.evolve;


-- ============================================================================
-- C7: Evolve — Category breakdown
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE cnt = 5 WHERE category = 'group-a'
ASSERT VALUE cnt = 3 WHERE category = 'group-b'
SELECT category, COUNT(*) AS cnt
FROM {{zone_name}}.iceberg_verify.evolve
GROUP BY category ORDER BY category;


-- ============================================================================
-- C8: Evolve — ADD another column
-- ============================================================================

ALTER TABLE {{zone_name}}.iceberg_verify.evolve ADD COLUMN priority INT;


-- ============================================================================
-- C9: Evolve — Final state (8 rows, 5 columns)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE cnt = 8
ASSERT VALUE total_value = 360.0
SELECT COUNT(*) AS cnt, ROUND(SUM(value), 1) AS total_value
FROM {{zone_name}}.iceberg_verify.evolve;


-- ============================================================================
-- D1: V3 — Baseline (6 rows)
-- ============================================================================

ASSERT ROW_COUNT = 6
SELECT * FROM {{zone_name}}.iceberg_verify.v3_table ORDER BY id;


-- ============================================================================
-- D2: V3 — UPDATE tag
-- ============================================================================

UPDATE {{zone_name}}.iceberg_verify.v3_table SET tag = 'x' WHERE id <= 3;


-- ============================================================================
-- D3: V3 — DELETE tag=x
-- ============================================================================

DELETE FROM {{zone_name}}.iceberg_verify.v3_table WHERE tag = 'x';


-- ============================================================================
-- D4: V3 — Final state (3 rows)
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE cnt = 3
ASSERT VALUE total_value = 16.5
SELECT COUNT(*) AS cnt, ROUND(SUM(value), 1) AS total_value
FROM {{zone_name}}.iceberg_verify.v3_table;


-- ============================================================================
-- VERIFY: All Checks — Summary across all 4 tables
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE products_count = 10
ASSERT VALUE sales_count = 13
ASSERT VALUE evolve_count = 8
ASSERT VALUE v3_count = 3
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_verify.products)  AS products_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_verify.sales)     AS sales_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_verify.evolve)    AS evolve_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_verify.v3_table)  AS v3_count;
