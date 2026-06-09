-- ============================================================================
-- Partition-Scoped Maintenance — Setup Script
-- ============================================================================
-- Creates a partitioned Delta table modelling a global e-commerce order
-- processing system with three warehouse datacenters.
--
-- Tables created:
--   1. warehouse_orders — 75 rows, partitioned by warehouse
--      (us-east-dc, eu-central-dc, ap-south-dc), 25 rows each
--
-- The queries.sql script then demonstrates partition-scoped DELETE,
-- UPDATE, selective OPTIMIZE on a single partition, and finally
-- OPTIMIZE on the remaining partitions.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: warehouse_orders — Global e-commerce order processing
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.warehouse_orders (
    id          INT,
    order_id    VARCHAR,
    warehouse   VARCHAR,
    product     VARCHAR,
    quantity    INT,
    unit_price  DECIMAL(10,2),
    order_date  VARCHAR,
    priority    VARCHAR
) LOCATION 'delta-partition-selective-optimize/warehouse_orders'
PARTITIONED BY (warehouse)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true'
);


-- Warehouse 1: us-east-dc (25 orders)
INSERT INTO {{zone_name}}.delta_demos.warehouse_orders VALUES
    ( 1, 'ORD-1001', 'us-east-dc', 'electronics',  4, 149.99, '2025-03-01', 'express'),
    ( 2, 'ORD-1002', 'us-east-dc', 'clothing',     2,  39.99, '2025-03-01', 'standard'),
    ( 3, 'ORD-1003', 'us-east-dc', 'food',        10,  12.50, '2025-03-02', 'standard'),
    ( 4, 'ORD-1004', 'us-east-dc', 'furniture',    1, 899.99, '2025-03-02', 'express'),
    ( 5, 'ORD-1005', 'us-east-dc', 'toys',         3,  24.99, '2025-03-03', 'standard'),
    ( 6, 'ORD-1006', 'us-east-dc', 'electronics',  2, 349.99, '2025-03-03', 'overnight'),
    ( 7, 'ORD-1007', 'us-east-dc', 'clothing',     5,  59.99, '2025-03-04', 'standard'),
    ( 8, 'ORD-1008', 'us-east-dc', 'food',         8,   9.99, '2025-03-04', 'express'),
    ( 9, 'ORD-1009', 'us-east-dc', 'furniture',    1, 549.99, '2025-03-05', 'standard'),
    (10, 'ORD-1010', 'us-east-dc', 'toys',         6,  34.99, '2025-03-05', 'overnight'),
    (11, 'ORD-1011', 'us-east-dc', 'electronics',  1, 799.99, '2025-03-06', 'express'),
    (12, 'ORD-1012', 'us-east-dc', 'clothing',     3,  29.99, '2025-03-06', 'standard'),
    (13, 'ORD-1013', 'us-east-dc', 'food',        15,  14.99, '2025-03-07', 'standard'),
    (14, 'ORD-1014', 'us-east-dc', 'furniture',    2, 419.99, '2025-03-07', 'express'),
    (15, 'ORD-1015', 'us-east-dc', 'toys',         4,  19.99, '2025-03-08', 'standard'),
    (16, 'ORD-1016', 'us-east-dc', 'electronics',  3, 129.99, '2025-03-08', 'overnight'),
    (17, 'ORD-1017', 'us-east-dc', 'clothing',     1,  89.99, '2025-03-09', 'express'),
    (18, 'ORD-1018', 'us-east-dc', 'food',        20,  11.99, '2025-03-09', 'standard'),
    (19, 'ORD-1019', 'us-east-dc', 'furniture',    1, 649.99, '2025-03-10', 'express'),
    (20, 'ORD-1020', 'us-east-dc', 'toys',         7,  44.99, '2025-03-10', 'standard'),
    (21, 'ORD-1021', 'us-east-dc', 'electronics',  2, 219.99, '2025-03-11', 'standard'),
    (22, 'ORD-1022', 'us-east-dc', 'clothing',     4,  49.99, '2025-03-11', 'express'),
    (23, 'ORD-1023', 'us-east-dc', 'food',         6,  18.99, '2025-03-12', 'overnight'),
    (24, 'ORD-1024', 'us-east-dc', 'furniture',    1, 329.99, '2025-03-12', 'standard'),
    (25, 'ORD-1025', 'us-east-dc', 'toys',         2,  64.99, '2025-03-12', 'express');

-- Warehouse 2: eu-central-dc (25 orders)
INSERT INTO {{zone_name}}.delta_demos.warehouse_orders VALUES
    (26, 'ORD-2001', 'eu-central-dc', 'electronics',  3, 179.99, '2025-03-01', 'standard'),
    (27, 'ORD-2002', 'eu-central-dc', 'clothing',     6,  44.99, '2025-03-01', 'express'),
    (28, 'ORD-2003', 'eu-central-dc', 'food',        12,  15.99, '2025-03-02', 'standard'),
    (29, 'ORD-2004', 'eu-central-dc', 'furniture',    1, 749.99, '2025-03-02', 'overnight'),
    (30, 'ORD-2005', 'eu-central-dc', 'toys',         5,  29.99, '2025-03-03', 'standard'),
    (31, 'ORD-2006', 'eu-central-dc', 'electronics',  1, 599.99, '2025-03-03', 'express'),
    (32, 'ORD-2007', 'eu-central-dc', 'clothing',     2,  79.99, '2025-03-04', 'standard'),
    (33, 'ORD-2008', 'eu-central-dc', 'food',         9,  11.49, '2025-03-04', 'express'),
    (34, 'ORD-2009', 'eu-central-dc', 'furniture',    1, 459.99, '2025-03-05', 'standard'),
    (35, 'ORD-2010', 'eu-central-dc', 'toys',         8,  17.99, '2025-03-05', 'overnight'),
    (36, 'ORD-2011', 'eu-central-dc', 'electronics',  2, 299.99, '2025-03-06', 'standard'),
    (37, 'ORD-2012', 'eu-central-dc', 'clothing',     3,  69.99, '2025-03-06', 'express'),
    (38, 'ORD-2013', 'eu-central-dc', 'food',         7,  22.99, '2025-03-07', 'standard'),
    (39, 'ORD-2014', 'eu-central-dc', 'furniture',    2, 369.99, '2025-03-07', 'express'),
    (40, 'ORD-2015', 'eu-central-dc', 'toys',         4,  54.99, '2025-03-08', 'standard'),
    (41, 'ORD-2016', 'eu-central-dc', 'electronics',  1, 449.99, '2025-03-08', 'overnight'),
    (42, 'ORD-2017', 'eu-central-dc', 'clothing',     5,  34.99, '2025-03-09', 'standard'),
    (43, 'ORD-2018', 'eu-central-dc', 'food',        14,  13.49, '2025-03-09', 'express'),
    (44, 'ORD-2019', 'eu-central-dc', 'furniture',    1, 579.99, '2025-03-10', 'standard'),
    (45, 'ORD-2020', 'eu-central-dc', 'toys',         3,  42.99, '2025-03-10', 'express'),
    (46, 'ORD-2021', 'eu-central-dc', 'electronics',  4, 159.99, '2025-03-11', 'standard'),
    (47, 'ORD-2022', 'eu-central-dc', 'clothing',     1,  99.99, '2025-03-11', 'overnight'),
    (48, 'ORD-2023', 'eu-central-dc', 'food',        11,  19.99, '2025-03-12', 'standard'),
    (49, 'ORD-2024', 'eu-central-dc', 'furniture',    1, 279.99, '2025-03-12', 'express'),
    (50, 'ORD-2025', 'eu-central-dc', 'toys',         6,  39.99, '2025-03-12', 'standard');

-- Warehouse 3: ap-south-dc (25 orders)
INSERT INTO {{zone_name}}.delta_demos.warehouse_orders VALUES
    (51, 'ORD-3001', 'ap-south-dc', 'electronics',  2, 199.99, '2025-03-01', 'express'),
    (52, 'ORD-3002', 'ap-south-dc', 'clothing',     4,  54.99, '2025-03-01', 'standard'),
    (53, 'ORD-3003', 'ap-south-dc', 'food',         8,  16.99, '2025-03-02', 'overnight'),
    (54, 'ORD-3004', 'ap-south-dc', 'furniture',    1, 699.99, '2025-03-02', 'standard'),
    (55, 'ORD-3005', 'ap-south-dc', 'toys',         5,  22.99, '2025-03-03', 'express'),
    (56, 'ORD-3006', 'ap-south-dc', 'electronics',  3, 249.99, '2025-03-03', 'standard'),
    (57, 'ORD-3007', 'ap-south-dc', 'clothing',     2,  74.99, '2025-03-04', 'express'),
    (58, 'ORD-3008', 'ap-south-dc', 'food',        15,  10.99, '2025-03-04', 'standard'),
    (59, 'ORD-3009', 'ap-south-dc', 'furniture',    1, 519.99, '2025-03-05', 'overnight'),
    (60, 'ORD-3010', 'ap-south-dc', 'toys',         7,  27.99, '2025-03-05', 'standard'),
    (61, 'ORD-3011', 'ap-south-dc', 'electronics',  1, 849.99, '2025-03-06', 'express'),
    (62, 'ORD-3012', 'ap-south-dc', 'clothing',     6,  32.99, '2025-03-06', 'standard'),
    (63, 'ORD-3013', 'ap-south-dc', 'food',        10,  21.49, '2025-03-07', 'standard'),
    (64, 'ORD-3014', 'ap-south-dc', 'furniture',    1, 389.99, '2025-03-07', 'overnight'),
    (65, 'ORD-3015', 'ap-south-dc', 'toys',         3,  49.99, '2025-03-08', 'express'),
    (66, 'ORD-3016', 'ap-south-dc', 'electronics',  2, 169.99, '2025-03-08', 'standard'),
    (67, 'ORD-3017', 'ap-south-dc', 'clothing',     1, 119.99, '2025-03-09', 'standard'),
    (68, 'ORD-3018', 'ap-south-dc', 'food',        18,   9.99, '2025-03-09', 'express'),
    (69, 'ORD-3019', 'ap-south-dc', 'furniture',    1, 759.99, '2025-03-10', 'standard'),
    (70, 'ORD-3020', 'ap-south-dc', 'toys',         4,  36.99, '2025-03-10', 'overnight'),
    (71, 'ORD-3021', 'ap-south-dc', 'electronics',  5, 109.99, '2025-03-11', 'express'),
    (72, 'ORD-3022', 'ap-south-dc', 'clothing',     3,  64.99, '2025-03-11', 'standard'),
    (73, 'ORD-3023', 'ap-south-dc', 'food',         6,  24.99, '2025-03-12', 'standard'),
    (74, 'ORD-3024', 'ap-south-dc', 'furniture',    2, 449.99, '2025-03-12', 'express'),
    (75, 'ORD-3025', 'ap-south-dc', 'toys',         9,  14.99, '2025-03-12', 'overnight');
