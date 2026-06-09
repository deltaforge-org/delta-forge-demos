-- ============================================================================
-- Delta DML Patterns — INSERT, UPDATE & DELETE — Setup Script
-- ============================================================================
-- Creates an order management system with 60 baseline orders across 4 regions
-- and mixed statuses. The DML operations are in queries.sql.
--
-- Tables created:
--   1. order_history  — 60 orders across 4 regions, mixed statuses
--   2. order_archive  — empty archive table (same schema, used by DML queries)
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLEs with explicit schema
--   3. INSERT 60 rows — baseline orders
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: order_history — 60 orders across 4 regions
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.order_history (
    id          INT,
    customer    VARCHAR,
    product     VARCHAR,
    qty         INT,
    price       DOUBLE,
    status      VARCHAR,
    region      VARCHAR,
    order_date  VARCHAR
) LOCATION 'delta-dml-patterns/order_history';


-- ============================================================================
-- TABLE: order_archive — empty archive for cancelled-order DML demo
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.order_archive (
    id          INT,
    customer    VARCHAR,
    product     VARCHAR,
    qty         INT,
    price       DOUBLE,
    status      VARCHAR,
    region      VARCHAR,
    order_date  VARCHAR
) LOCATION 'delta-dml-patterns/order_archive';


-- STEP 2: Insert 60 known orders
INSERT INTO {{zone_name}}.delta_demos.order_history VALUES
    -- Group A: 8 cancelled orders with order_date < 2024-06-01
    (1,  'Alice',  'Desk',        2,  150.00, 'cancelled', 'us-east',  '2024-03-15'),
    (2,  'Bob',    'Chair',       1,  120.00, 'cancelled', 'us-west',  '2024-04-20'),
    (3,  'Carol',  'Lamp',        3,  25.00,  'cancelled', 'eu-west',  '2024-02-10'),
    (4,  'Dave',   'Shelf',       1,  89.00,  'cancelled', 'ap-south', '2024-05-01'),
    (5,  'Eve',    'Mug',         5,  12.00,  'cancelled', 'us-east',  '2024-01-25'),
    (6,  'Frank',  'Pen',         10, 3.50,   'cancelled', 'us-west',  '2024-03-30'),
    (7,  'Grace',  'Notebook',    4,  8.00,   'cancelled', 'eu-west',  '2024-05-15'),
    (8,  'Hank',   'Calendar',    2,  15.00,  'cancelled', 'ap-south', '2024-04-01'),
    -- Group B: 5 completed orders with order_date < 2024-01-01
    (9,  'Ivy',    'Desk',        1,  150.00, 'completed', 'us-west',  '2023-11-20'),
    (10, 'Jack',   'Chair',       2,  200.00, 'completed', 'eu-west',  '2023-10-15'),
    (11, 'Kate',   'Bag',         3,  45.00,  'completed', 'ap-south', '2023-12-01'),
    (12, 'Leo',    'Clock',       1,  60.00,  'completed', 'us-east',  '2023-09-18'),
    (13, 'Mia',    'Lamp',        2,  30.00,  'completed', 'us-west',  '2023-08-05'),
    -- Group C: 6 pending us-east orders
    (14, 'Noah',   'Laptop',      1,  999.99, 'pending',   'us-east',  '2024-07-10'),
    (15, 'Olga',   'Monitor',     2,  349.99, 'pending',   'us-east',  '2024-08-05'),
    (16, 'Pete',   'Desk',        1,  150.00, 'pending',   'us-east',  '2024-09-12'),
    (17, 'Quinn',  'Chair',       3,  120.00, 'pending',   'us-east',  '2024-06-20'),
    (18, 'Rosa',   'Bag',         2,  45.00,  'pending',   'us-east',  '2024-07-30'),
    (19, 'Sam',    'Headphones',  1,  199.99, 'pending',   'us-east',  '2024-10-01'),
    -- Group D: 7 additional electronics orders
    (20, 'Tina',   'Laptop',      1,  999.99, 'completed', 'us-west',  '2024-08-15'),
    (21, 'Uma',    'Monitor',     1,  349.99, 'shipped',   'eu-west',  '2024-07-22'),
    (22, 'Vince',  'Tablet',      2,  499.99, 'pending',   'ap-south', '2024-09-03'),
    (23, 'Wendy',  'Tablet',      1,  499.99, 'completed', 'us-east',  '2024-06-18'),
    (24, 'Xander', 'Headphones',  3,  199.99, 'shipped',   'us-west',  '2024-11-01'),
    (25, 'Yara',   'Smartwatch',  1,  249.99, 'pending',   'eu-west',  '2024-08-28'),
    (26, 'Zach',   'Smartwatch',  2,  249.99, 'completed', 'ap-south', '2024-10-10'),
    -- Group E: 34 remaining orders (various statuses, regions, non-electronics)
    (27, 'Amy',    'Desk',        2,  150.00, 'completed', 'us-east',  '2024-07-15'),
    (28, 'Brian',  'Chair',       1,  120.00, 'shipped',   'us-west',  '2024-08-20'),
    (29, 'Chloe',  'Lamp',        4,  25.00,  'completed', 'eu-west',  '2024-09-10'),
    (30, 'Dan',    'Shelf',       1,  89.00,  'shipped',   'ap-south', '2024-06-05'),
    (31, 'Elena',  'Mug',         6,  12.00,  'pending',   'us-west',  '2024-10-15'),
    (32, 'Felix',  'Pen',         8,  3.50,   'completed', 'eu-west',  '2024-07-22'),
    (33, 'Gina',   'Notebook',    3,  8.00,   'shipped',   'ap-south', '2024-11-01'),
    (34, 'Hugo',   'Calendar',    1,  15.00,  'pending',   'eu-west',  '2024-08-30'),
    (35, 'Iris',   'Bag',         2,  45.00,  'shipped',   'us-west',  '2024-09-18'),
    (36, 'Jake',   'Clock',       1,  60.00,  'completed', 'us-east',  '2024-06-25'),
    (37, 'Lily',   'Desk',        1,  150.00, 'cancelled', 'us-west',  '2024-07-01'),
    (38, 'Mike',   'Chair',       2,  120.00, 'cancelled', 'eu-west',  '2024-08-15'),
    (39, 'Nina',   'Lamp',        1,  25.00,  'pending',   'ap-south', '2024-09-05'),
    (40, 'Oscar',  'Shelf',       3,  89.00,  'shipped',   'ap-south', '2024-10-20'),
    (41, 'Paula',  'Mug',         4,  12.00,  'completed', 'us-west',  '2024-11-12'),
    (42, 'Reed',   'Pen',         12, 3.50,   'shipped',   'eu-west',  '2024-07-08'),
    (43, 'Sara',   'Notebook',    2,  8.00,   'completed', 'ap-south', '2024-08-22'),
    (44, 'Tom',    'Calendar',    1,  15.00,  'shipped',   'us-west',  '2024-06-15'),
    (45, 'Vera',   'Bag',         3,  45.00,  'completed', 'eu-west',  '2024-09-28'),
    (46, 'Will',   'Clock',       2,  60.00,  'pending',   'ap-south', '2024-10-05'),
    (47, 'Xena',   'Desk',        1,  150.00, 'shipped',   'eu-west',  '2024-07-19'),
    (48, 'Yuri',   'Chair',       2,  120.00, 'completed', 'ap-south', '2024-08-11'),
    (49, 'Zara',   'Lamp',        5,  25.00,  'cancelled', 'us-east',  '2024-09-22'),
    (50, 'Adam',   'Shelf',       2,  89.00,  'pending',   'us-west',  '2024-11-08'),
    (51, 'Beth',   'Mug',         3,  12.00,  'shipped',   'ap-south', '2024-06-30'),
    (52, 'Carl',   'Pen',         6,  3.50,   'completed', 'us-east',  '2024-07-25'),
    (53, 'Dana',   'Notebook',    1,  8.00,   'cancelled', 'eu-west',  '2024-10-14'),
    (54, 'Eric',   'Calendar',    2,  15.00,  'completed', 'us-west',  '2024-08-03'),
    (55, 'Faye',   'Bag',         1,  45.00,  'shipped',   'eu-west',  '2024-09-16'),
    (56, 'Greg',   'Clock',       3,  60.00,  'shipped',   'us-west',  '2024-10-28'),
    (57, 'Hope',   'Desk',        2,  150.00, 'completed', 'ap-south', '2024-07-04'),
    (58, 'Ivan',   'Chair',       1,  120.00, 'pending',   'eu-west',  '2024-11-19'),
    (59, 'Jade',   'Lamp',        2,  25.00,  'completed', 'us-west',  '2024-08-27'),
    (60, 'Kurt',   'Shelf',       4,  89.00,  'completed', 'eu-west',  '2024-06-10');

