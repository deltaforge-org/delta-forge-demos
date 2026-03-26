-- ============================================================================
-- Delta Partition-Scoped DELETE — Setup Script
-- ============================================================================
-- Creates a partitioned Delta table modelling an e-commerce order fulfillment
-- system and loads 45 baseline orders across three warehouse regions.
--
-- Tables created:
--   1. warehouse_orders — 45 rows, partitioned by region (us-west, us-central, us-east)
--
-- The queries.sql script then demonstrates partition-scoped DELETE,
-- cross-partition DELETE, and conditional DELETE with data predicates.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: warehouse_orders — E-commerce fulfillment orders
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.warehouse_orders (
    id          INT,
    order_ref   VARCHAR,
    region      VARCHAR,
    product     VARCHAR,
    category    VARCHAR,
    quantity    INT,
    unit_price  DECIMAL(10,2),
    status      VARCHAR,
    order_date  VARCHAR
) LOCATION '{{data_path}}/warehouse_orders'
PARTITIONED BY (region);
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.warehouse_orders TO USER {{current_user}};

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.warehouse_orders;

ALTER TABLE {{zone_name}}.delta_demos.warehouse_orders SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'iceberg',
  'delta.universalFormat.icebergVersion' = '3'
);

-- Region 1: us-west (15 orders)
INSERT INTO {{zone_name}}.delta_demos.warehouse_orders VALUES
    (1,  'ORD-1001', 'us-west', 'Laptop Pro',          'electronics', 2,  899.99, 'fulfilled', '2024-08-01'),
    (2,  'ORD-1002', 'us-west', 'Winter Jacket',       'clothing',    5,  129.99, 'fulfilled', '2024-08-02'),
    (3,  'ORD-1003', 'us-west', 'Wireless Headphones', 'electronics', 10, 79.99,  'pending',   '2024-08-03'),
    (4,  'ORD-1004', 'us-west', 'Standing Desk',       'home',        1,  549.99, 'cancelled', '2024-08-04'),
    (5,  'ORD-1005', 'us-west', 'Running Shoes',       'sports',      3,  159.99, 'fulfilled', '2024-08-05'),
    (6,  'ORD-1006', 'us-west', 'Protein Bars (case)', 'food',        20, 34.99,  'returned',  '2024-08-06'),
    (7,  'ORD-1007', 'us-west', 'USB-C Hub',           'electronics', 15, 49.99,  'pending',   '2024-08-07'),
    (8,  'ORD-1008', 'us-west', 'Silk Scarf',          'clothing',    8,  89.99,  'cancelled', '2024-08-08'),
    (9,  'ORD-1009', 'us-west', 'Air Purifier',        'home',        2,  299.99, 'fulfilled', '2024-08-09'),
    (10, 'ORD-1010', 'us-west', 'Yoga Mat',            'sports',      12, 39.99,  'returned',  '2024-08-10'),
    (11, 'ORD-1011', 'us-west', 'Tablet Stand',        'electronics', 6,  29.99,  'fulfilled', '2024-08-11'),
    (12, 'ORD-1012', 'us-west', 'Organic Coffee 5lb',  'food',        10, 44.99,  'pending',   '2024-08-12'),
    (13, 'ORD-1013', 'us-west', 'Denim Jacket',        'clothing',    4,  199.99, 'cancelled', '2024-08-13'),
    (14, 'ORD-1014', 'us-west', 'Smart Thermostat',    'home',        3,  249.99, 'fulfilled', '2024-08-14'),
    (15, 'ORD-1015', 'us-west', 'Resistance Bands',    'sports',      25, 19.99,  'pending',   '2024-08-15');

-- Region 2: us-central (15 orders)
INSERT INTO {{zone_name}}.delta_demos.warehouse_orders VALUES
    (16, 'ORD-1016', 'us-central', 'Monitor 27in',        'electronics', 3,  449.99, 'fulfilled', '2024-08-01'),
    (17, 'ORD-1017', 'us-central', 'Wool Sweater',        'clothing',    7,  119.99, 'pending',   '2024-08-02'),
    (18, 'ORD-1018', 'us-central', 'Robot Vacuum',        'home',        2,  399.99, 'fulfilled', '2024-08-03'),
    (19, 'ORD-1019', 'us-central', 'Dumbbells Pair',      'sports',      4,  89.99,  'cancelled', '2024-08-04'),
    (20, 'ORD-1020', 'us-central', 'Almonds Bulk',        'food',        15, 24.99,  'fulfilled', '2024-08-05'),
    (21, 'ORD-1021', 'us-central', 'Mechanical Keyboard', 'electronics', 10, 149.99, 'returned',  '2024-08-06'),
    (22, 'ORD-1022', 'us-central', 'Rain Jacket',         'clothing',    6,  179.99, 'fulfilled', '2024-08-07'),
    (23, 'ORD-1023', 'us-central', 'Bookshelf Oak',       'home',        1,  349.99, 'pending',   '2024-08-08'),
    (24, 'ORD-1024', 'us-central', 'Tennis Racket',       'sports',      3,  199.99, 'returned',  '2024-08-09'),
    (25, 'ORD-1025', 'us-central', 'Green Tea 100pk',     'food',        20, 29.99,  'fulfilled', '2024-08-10'),
    (26, 'ORD-1026', 'us-central', 'Webcam HD',           'electronics', 8,  69.99,  'cancelled', '2024-08-11'),
    (27, 'ORD-1027', 'us-central', 'Linen Shirt',         'clothing',    5,  59.99,  'pending',   '2024-08-12'),
    (28, 'ORD-1028', 'us-central', 'Plant Pot Set',       'home',        12, 34.99,  'fulfilled', '2024-08-13'),
    (29, 'ORD-1029', 'us-central', 'Jump Rope',           'sports',      10, 14.99,  'pending',   '2024-08-14'),
    (30, 'ORD-1030', 'us-central', 'Protein Powder',      'food',        6,  54.99,  'cancelled', '2024-08-15');

-- Region 3: us-east (15 orders)
INSERT INTO {{zone_name}}.delta_demos.warehouse_orders VALUES
    (31, 'ORD-1031', 'us-east', 'Phone Case Premium', 'electronics', 20, 39.99,  'fulfilled', '2024-08-01'),
    (32, 'ORD-1032', 'us-east', 'Hiking Boots',       'clothing',    3,  219.99, 'fulfilled', '2024-08-02'),
    (33, 'ORD-1033', 'us-east', 'LED Desk Lamp',      'home',        8,  69.99,  'pending',   '2024-08-03'),
    (34, 'ORD-1034', 'us-east', 'Basketball',         'sports',      6,  29.99,  'cancelled', '2024-08-04'),
    (35, 'ORD-1035', 'us-east', 'Dried Mango Case',   'food',        25, 19.99,  'fulfilled', '2024-08-05'),
    (36, 'ORD-1036', 'us-east', 'Power Bank',         'electronics', 15, 59.99,  'returned',  '2024-08-06'),
    (37, 'ORD-1037', 'us-east', 'Polo Shirt',         'clothing',    10, 49.99,  'fulfilled', '2024-08-07'),
    (38, 'ORD-1038', 'us-east', 'Shower Head',        'home',        4,  79.99,  'pending',   '2024-08-08'),
    (39, 'ORD-1039', 'us-east', 'Foam Roller',        'sports',      8,  24.99,  'returned',  '2024-08-09'),
    (40, 'ORD-1040', 'us-east', 'Trail Mix Bulk',     'food',        30, 14.99,  'fulfilled', '2024-08-10'),
    (41, 'ORD-1041', 'us-east', 'HDMI Cable 6ft',     'electronics', 50, 12.99,  'fulfilled', '2024-08-11'),
    (42, 'ORD-1042', 'us-east', 'Canvas Tote',        'clothing',    15, 34.99,  'cancelled', '2024-08-12'),
    (43, 'ORD-1043', 'us-east', 'Throw Pillow Set',   'home',        6,  44.99,  'pending',   '2024-08-13'),
    (44, 'ORD-1044', 'us-east', 'Water Bottle',       'sports',      20, 19.99,  'cancelled', '2024-08-14'),
    (45, 'ORD-1045', 'us-east', 'Olive Oil 3L',       'food',        5,  39.99,  'pending',   '2024-08-15');
