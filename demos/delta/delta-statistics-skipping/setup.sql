-- ============================================================================
-- Data Skipping — Range Statistics in Action — Setup Script
-- ============================================================================
-- E-commerce order analytics: orders arrive in monthly batches with naturally
-- non-overlapping price ranges. Each batch creates separate Parquet files,
-- giving the engine distinct min/max ranges for effective data skipping.
--
-- Tables created:
--   1. orders — 45 orders in 3 monthly batches
--
-- Operations performed:
--   1. CREATE DELTA TABLE
--   2. INSERT Batch 1 (Jan) — 15 orders, unit_price [10.99 - 95.00]
--   3. INSERT Batch 2 (Feb) — 15 orders, unit_price [100.00 - 475.00]
--   4. INSERT Batch 3 (Mar) — 15 orders, unit_price [500.00 - 2000.00]
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: orders — e-commerce orders with monthly price tiers
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.orders (
    id              INT,
    order_ref       VARCHAR,
    customer_id     VARCHAR,
    category        VARCHAR,
    unit_price      DOUBLE,
    quantity         INT,
    line_total      DOUBLE,
    order_date      VARCHAR
) LOCATION 'orders';


-- ============================================================================
-- STEP 2: Batch 1 — January 2025, unit_price range [10.99 - 95.00]
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.orders VALUES
    (1,  'ORD-1001', 'C-200', 'electronics', 45.99,  2, 91.98,  '2025-01-03'),
    (2,  'ORD-1002', 'C-201', 'clothing',    29.50,  1, 29.50,  '2025-01-05'),
    (3,  'ORD-1003', 'C-202', 'groceries',   12.75,  4, 51.00,  '2025-01-07'),
    (4,  'ORD-1004', 'C-200', 'electronics', 89.99,  1, 89.99,  '2025-01-08'),
    (5,  'ORD-1005', 'C-203', 'clothing',    34.00,  3, 102.00, '2025-01-10'),
    (6,  'ORD-1006', 'C-204', 'groceries',   18.25,  2, 36.50,  '2025-01-12'),
    (7,  'ORD-1007', 'C-205', 'home',        67.50,  1, 67.50,  '2025-01-14'),
    (8,  'ORD-1008', 'C-201', 'electronics', 55.00,  2, 110.00, '2025-01-16'),
    (9,  'ORD-1009', 'C-206', 'clothing',    22.99,  1, 22.99,  '2025-01-18'),
    (10, 'ORD-1010', 'C-207', 'home',        78.50,  1, 78.50,  '2025-01-20'),
    (11, 'ORD-1011', 'C-208', 'groceries',   15.00,  5, 75.00,  '2025-01-22'),
    (12, 'ORD-1012', 'C-202', 'electronics', 95.00,  1, 95.00,  '2025-01-24'),
    (13, 'ORD-1013', 'C-209', 'clothing',    42.00,  2, 84.00,  '2025-01-26'),
    (14, 'ORD-1014', 'C-210', 'home',        10.99,  3, 32.97,  '2025-01-28'),
    (15, 'ORD-1015', 'C-200', 'groceries',   25.50,  2, 51.00,  '2025-01-30');


-- ============================================================================
-- STEP 3: Batch 2 — February 2025, unit_price range [100.00 - 475.00]
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.orders
SELECT * FROM (VALUES
    (16, 'ORD-2001', 'C-211', 'electronics', 249.99, 1, 249.99, '2025-02-02'),
    (17, 'ORD-2002', 'C-212', 'home',        175.00, 2, 350.00, '2025-02-04'),
    (18, 'ORD-2003', 'C-200', 'electronics', 399.99, 1, 399.99, '2025-02-06'),
    (19, 'ORD-2004', 'C-213', 'clothing',    125.00, 2, 250.00, '2025-02-08'),
    (20, 'ORD-2005', 'C-214', 'home',        310.50, 1, 310.50, '2025-02-10'),
    (21, 'ORD-2006', 'C-201', 'electronics', 189.99, 2, 379.98, '2025-02-12'),
    (22, 'ORD-2007', 'C-215', 'groceries',   100.00, 3, 300.00, '2025-02-14'),
    (23, 'ORD-2008', 'C-216', 'clothing',    155.00, 1, 155.00, '2025-02-16'),
    (24, 'ORD-2009', 'C-202', 'home',        225.75, 1, 225.75, '2025-02-18'),
    (25, 'ORD-2010', 'C-217', 'electronics', 475.00, 1, 475.00, '2025-02-20'),
    (26, 'ORD-2011', 'C-218', 'groceries',   110.50, 4, 442.00, '2025-02-22'),
    (27, 'ORD-2012', 'C-219', 'clothing',    199.99, 1, 199.99, '2025-02-24'),
    (28, 'ORD-2013', 'C-220', 'home',        350.00, 1, 350.00, '2025-02-26'),
    (29, 'ORD-2014', 'C-203', 'electronics', 285.00, 1, 285.00, '2025-02-27'),
    (30, 'ORD-2015', 'C-221', 'clothing',    140.00, 2, 280.00, '2025-02-28')
) AS t(id, order_ref, customer_id, category, unit_price, quantity, line_total, order_date);


-- ============================================================================
-- STEP 4: Batch 3 — March 2025, unit_price range [500.00 - 2000.00]
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.orders
SELECT * FROM (VALUES
    (31, 'ORD-3001', 'C-222', 'electronics', 999.99,  1, 999.99,  '2025-03-02'),
    (32, 'ORD-3002', 'C-223', 'home',        750.00,  1, 750.00,  '2025-03-04'),
    (33, 'ORD-3003', 'C-200', 'electronics', 1299.99, 1, 1299.99, '2025-03-06'),
    (34, 'ORD-3004', 'C-224', 'home',        525.00,  2, 1050.00, '2025-03-08'),
    (35, 'ORD-3005', 'C-225', 'electronics', 849.99,  1, 849.99,  '2025-03-10'),
    (36, 'ORD-3006', 'C-201', 'home',        650.00,  1, 650.00,  '2025-03-12'),
    (37, 'ORD-3007', 'C-226', 'electronics', 1750.00, 1, 1750.00, '2025-03-14'),
    (38, 'ORD-3008', 'C-227', 'clothing',    500.00,  2, 1000.00, '2025-03-16'),
    (39, 'ORD-3009', 'C-228', 'home',        1100.00, 1, 1100.00, '2025-03-18'),
    (40, 'ORD-3010', 'C-229', 'electronics', 2000.00, 1, 2000.00, '2025-03-20'),
    (41, 'ORD-3011', 'C-230', 'clothing',    575.00,  1, 575.00,  '2025-03-22'),
    (42, 'ORD-3012', 'C-202', 'home',        825.50,  1, 825.50,  '2025-03-24'),
    (43, 'ORD-3013', 'C-231', 'groceries',   600.00,  3, 1800.00, '2025-03-26'),
    (44, 'ORD-3014', 'C-232', 'electronics', 1450.00, 1, 1450.00, '2025-03-28'),
    (45, 'ORD-3015', 'C-200', 'home',        950.00,  1, 950.00,  '2025-03-30')
) AS t(id, order_ref, customer_id, category, unit_price, quantity, line_total, order_date);
