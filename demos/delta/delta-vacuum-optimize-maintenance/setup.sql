-- ============================================================================
-- Delta Maintenance Playbook — OPTIMIZE Then VACUUM — Setup Script
-- ============================================================================
-- Simulates an e-commerce order pipeline with daily micro-batch ingestion.
-- Each day's batch creates a separate small Parquet file, leading to the
-- classic "small files problem." Subsequent mutations (shipping, cancellations,
-- price fixes) create additional orphaned file versions.
--
-- Operations:
--   1. CREATE DELTA TABLE
--   2-6. INSERT 5 daily micro-batches (8 orders each = 40 rows total)
--   7. UPDATE — ship Monday's batch (8 rows → 'shipped')
--   8. DELETE — cancel 3 orders
--   9. UPDATE — fix pricing errors on 4 orders (+$5.00 surcharge)
--
-- After setup, the table has 37 rows spread across many small files with
-- multiple orphaned versions. The queries.sql script demonstrates the
-- OPTIMIZE → VACUUM maintenance playbook to clean this up.
--
-- Tables created:
--   1. order_pipeline — 37 final rows after micro-batch loads and mutations
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: order_pipeline — e-commerce order pipeline
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.order_pipeline (
    id              INT,
    order_ref       VARCHAR,
    category        VARCHAR,
    product         VARCHAR,
    price           DOUBLE,
    status          VARCHAR,
    order_date      VARCHAR
) LOCATION 'order_pipeline';


-- ============================================================================
-- STEP 2: Monday batch — 8 orders (version 1)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.order_pipeline VALUES
    (1,  'ORD-1001', 'Electronics', 'Wireless Headphones',   79.99, 'pending', '2025-03-03'),
    (2,  'ORD-1002', 'Electronics', 'USB-C Hub',             34.99, 'pending', '2025-03-03'),
    (3,  'ORD-1003', 'Clothing',    'Running Shoes',        129.99, 'pending', '2025-03-03'),
    (4,  'ORD-1004', 'Home',        'Desk Lamp',             45.00, 'pending', '2025-03-03'),
    (5,  'ORD-1005', 'Electronics', 'Keyboard',              89.99, 'pending', '2025-03-03'),
    (6,  'ORD-1006', 'Clothing',    'Winter Jacket',        199.99, 'pending', '2025-03-03'),
    (7,  'ORD-1007', 'Home',        'Coffee Maker',          65.00, 'pending', '2025-03-03'),
    (8,  'ORD-1008', 'Books',       'SQL Cookbook',           49.99, 'pending', '2025-03-03');


-- ============================================================================
-- STEP 3: Tuesday batch — 8 orders (version 2)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.order_pipeline VALUES
    (9,  'ORD-2001', 'Electronics', 'Monitor Stand',         39.99, 'pending', '2025-03-04'),
    (10, 'ORD-2002', 'Clothing',    'Hiking Boots',         159.99, 'pending', '2025-03-04'),
    (11, 'ORD-2003', 'Home',        'Air Purifier',         189.00, 'pending', '2025-03-04'),
    (12, 'ORD-2004', 'Books',       'Data Engineering',      59.99, 'pending', '2025-03-04'),
    (13, 'ORD-2005', 'Electronics', 'Webcam',                69.99, 'pending', '2025-03-04'),
    (14, 'ORD-2006', 'Clothing',    'Polo Shirt',            35.00, 'pending', '2025-03-04'),
    (15, 'ORD-2007', 'Home',        'Plant Pot Set',         28.00, 'pending', '2025-03-04'),
    (16, 'ORD-2008', 'Books',       'Rust Programming',      44.99, 'pending', '2025-03-04');


-- ============================================================================
-- STEP 4: Wednesday batch — 8 orders (version 3)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.order_pipeline VALUES
    (17, 'ORD-3001', 'Electronics', 'Tablet Stand',          24.99, 'pending', '2025-03-05'),
    (18, 'ORD-3002', 'Clothing',    'Denim Jeans',           79.99, 'pending', '2025-03-05'),
    (19, 'ORD-3003', 'Home',        'Bookshelf',            125.00, 'pending', '2025-03-05'),
    (20, 'ORD-3004', 'Books',       'Clean Code',            39.99, 'pending', '2025-03-05'),
    (21, 'ORD-3005', 'Electronics', 'Power Bank',            29.99, 'pending', '2025-03-05'),
    (22, 'ORD-3006', 'Clothing',    'Sneakers',              95.00, 'pending', '2025-03-05'),
    (23, 'ORD-3007', 'Home',        'Throw Blanket',         42.00, 'pending', '2025-03-05'),
    (24, 'ORD-3008', 'Books',       'System Design',         54.99, 'pending', '2025-03-05');


-- ============================================================================
-- STEP 5: Thursday batch — 8 orders (version 4)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.order_pipeline VALUES
    (25, 'ORD-4001', 'Electronics', 'Mouse Pad',             19.99, 'pending', '2025-03-06'),
    (26, 'ORD-4002', 'Clothing',    'Windbreaker',           85.00, 'pending', '2025-03-06'),
    (27, 'ORD-4003', 'Home',        'Kitchen Scale',         32.00, 'pending', '2025-03-06'),
    (28, 'ORD-4004', 'Books',       'DDIA',                  49.99, 'pending', '2025-03-06'),
    (29, 'ORD-4005', 'Electronics', 'HDMI Cable',            12.99, 'pending', '2025-03-06'),
    (30, 'ORD-4006', 'Clothing',    'Rain Coat',            110.00, 'pending', '2025-03-06'),
    (31, 'ORD-4007', 'Home',        'Wall Clock',            38.00, 'pending', '2025-03-06'),
    (32, 'ORD-4008', 'Books',       'The Pragmatic Prog.',   44.99, 'pending', '2025-03-06');


-- ============================================================================
-- STEP 6: Friday batch — 8 orders (version 5)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.order_pipeline VALUES
    (33, 'ORD-5001', 'Electronics', 'Phone Case',            15.99, 'pending', '2025-03-07'),
    (34, 'ORD-5002', 'Clothing',    'Baseball Cap',          22.00, 'pending', '2025-03-07'),
    (35, 'ORD-5003', 'Home',        'Candle Set',            27.00, 'pending', '2025-03-07'),
    (36, 'ORD-5004', 'Books',       'Learning SQL',          34.99, 'pending', '2025-03-07'),
    (37, 'ORD-5005', 'Electronics', 'Screen Protector',       9.99, 'pending', '2025-03-07'),
    (38, 'ORD-5006', 'Clothing',    'Swim Trunks',           40.00, 'pending', '2025-03-07'),
    (39, 'ORD-5007', 'Home',        'Picture Frame',         18.00, 'pending', '2025-03-07'),
    (40, 'ORD-5008', 'Books',       'Refactoring',           42.99, 'pending', '2025-03-07');


-- ============================================================================
-- STEP 7: UPDATE — ship Monday's batch (version 6)
-- ============================================================================
-- Monday's 8 orders have been packed and shipped. Status changes from
-- 'pending' to 'shipped'. Delta rewrites the file(s) containing these rows,
-- orphaning the old versions with 'pending' status.
UPDATE {{zone_name}}.delta_demos.order_pipeline
SET status = 'shipped'
WHERE id BETWEEN 1 AND 8;


-- ============================================================================
-- STEP 8: DELETE — cancel 3 orders (version 7)
-- ============================================================================
-- Orders 11 (Air Purifier), 22 (Sneakers), and 37 (Screen Protector) were
-- cancelled by customers. Delta rewrites the affected files without these
-- rows, orphaning the old versions.
DELETE FROM {{zone_name}}.delta_demos.order_pipeline
WHERE id IN (11, 22, 37);


-- ============================================================================
-- STEP 9: UPDATE — fix pricing errors on 4 orders (version 8)
-- ============================================================================
-- A $5.00 shipping surcharge was missed on 4 orders. Each price fix rewrites
-- the file containing that row, creating more orphaned versions.
UPDATE {{zone_name}}.delta_demos.order_pipeline
SET price = ROUND(price + 5.00, 2)
WHERE id IN (9, 17, 25, 33);
