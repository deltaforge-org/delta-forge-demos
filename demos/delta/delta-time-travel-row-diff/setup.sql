-- ============================================================================
-- Delta Time Travel — Row-Level Change Detection — Setup Script
-- ============================================================================
-- An e-commerce order management system tracks 20 orders across 4 statuses.
-- A nightly ETL run modifies statuses and corrects a pricing error. Later,
-- cancelled orders are purged and new orders arrive. The analyst's job: find
-- exactly what changed, using only time travel (no CDF).
--
-- Version History:
--   V0: CREATE TABLE (empty)
--   V1: INSERT 20 orders (5 pending, 6 shipped, 6 delivered, 3 cancelled)
--   V2: UPDATE — batch status changes + price correction on order 3
--   V3: DELETE — remove 3 cancelled orders (ids 16, 17, 18)
--   V4: INSERT — 5 new pending orders (ids 21-25)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- VERSION 0+1: CREATE TABLE + INSERT 20 orders
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.ecom_orders (
    order_id    INT,
    customer    VARCHAR,
    product     VARCHAR,
    quantity    INT,
    unit_price  DOUBLE,
    status      VARCHAR,
    order_date  VARCHAR
) LOCATION 'delta-time-travel-row-diff/ecom_orders';


INSERT INTO {{zone_name}}.delta_demos.ecom_orders VALUES
    -- delivered (6 orders)
    (1,  'Alice Johnson',   'Laptop',          1, 999.99, 'delivered',  '2024-01-15'),
    (2,  'Bob Smith',       'Wireless Mouse',  2, 29.99,  'delivered',  '2024-01-16'),
    (12, 'Leo Garcia',      'RAM Module',      2, 64.99,  'delivered',  '2024-01-26'),
    (13, 'Mia Robinson',    'Mouse Pad',       1, 19.99,  'delivered',  '2024-01-27'),
    (14, 'Noah Clark',      'USB Drive',       4, 12.99,  'delivered',  '2024-01-28'),
    (19, 'Sam Turner',      'Power Bank',      2, 34.99,  'delivered',  '2024-02-02'),
    -- shipped (6 orders)
    (3,  'Carol Davis',     'USB-C Hub',       1, 45.00,  'shipped',    '2024-01-17'),
    (4,  'Dan Wilson',      'Keyboard',        1, 149.99, 'shipped',    '2024-01-18'),
    (10, 'Jack White',      'Charger',         2, 24.99,  'shipped',    '2024-01-24'),
    (11, 'Karen Adams',     'SSD Drive',       1, 89.99,  'shipped',    '2024-01-25'),
    (15, 'Olivia Hall',     'Ethernet Cable',  3, 8.99,   'shipped',    '2024-01-29'),
    (20, 'Tina Morgan',     'HDMI Cable',      2, 11.99,  'shipped',    '2024-02-03'),
    -- pending (5 orders)
    (5,  'Eve Martinez',    'Monitor',         1, 399.99, 'pending',    '2024-01-19'),
    (6,  'Frank Brown',     'Webcam',          3, 59.99,  'pending',    '2024-01-20'),
    (7,  'Grace Lee',       'Headphones',      1, 199.99, 'pending',    '2024-01-21'),
    (8,  'Hank Taylor',     'Tablet',          1, 549.99, 'pending',    '2024-01-22'),
    (9,  'Ivy Chen',        'Phone Case',      5, 15.99,  'pending',    '2024-01-23'),
    -- cancelled (3 orders)
    (16, 'Paul King',       'Bluetooth Speaker', 1, 79.99, 'cancelled', '2024-01-30'),
    (17, 'Quinn Wright',    'Smart Watch',     1, 249.99, 'cancelled',  '2024-01-31'),
    (18, 'Rita Scott',      'Fitness Tracker', 1, 129.99, 'cancelled',  '2024-02-01');


-- ============================================================================
-- VERSION 2: UPDATE — batch status changes + price correction
-- ============================================================================
-- The nightly ETL batch:
--   5 pending → shipped  (orders 5, 6, 7, 8, 9)
--   2 shipped → delivered (orders 3, 4)
--   Price fix on order 3: 45.00 → 49.99
UPDATE {{zone_name}}.delta_demos.ecom_orders
SET status = 'shipped'
WHERE order_id IN (5, 6, 7, 8, 9);

UPDATE {{zone_name}}.delta_demos.ecom_orders
SET status = 'delivered'
WHERE order_id IN (3, 4);

UPDATE {{zone_name}}.delta_demos.ecom_orders
SET unit_price = 49.99
WHERE order_id = 3;


-- ============================================================================
-- VERSION 5: DELETE — remove 3 cancelled orders
-- ============================================================================
DELETE FROM {{zone_name}}.delta_demos.ecom_orders
WHERE order_id IN (16, 17, 18);


-- ============================================================================
-- VERSION 6: INSERT — 5 new pending orders
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.ecom_orders VALUES
    (21, 'Uma Patel',       'Laptop Stand',              1, 69.99,  'pending', '2024-02-10'),
    (22, 'Victor Nguyen',   'Desk Lamp',                 1, 44.99,  'pending', '2024-02-11'),
    (23, 'Wendy Brooks',    'Cable Organizer',           2, 16.99,  'pending', '2024-02-12'),
    (24, 'Xavier Reed',     'Mechanical Keyboard',       1, 179.99, 'pending', '2024-02-13'),
    (25, 'Yara Sullivan',   'Noise Cancelling Earbuds',  1, 89.99,  'pending', '2024-02-14');
