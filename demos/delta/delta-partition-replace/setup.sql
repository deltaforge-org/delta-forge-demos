-- ============================================================================
-- Delta Partition Replace — Setup Script
-- ============================================================================
-- Creates a monthly_sales table PARTITIONED BY (sale_month) with 60 baseline
-- rows across 3 months (Jan, Feb, Mar 2024), 20 transactions each.
--
-- Table: monthly_sales — 60 rows, partitioned by sale_month
--
-- Each transaction has: id, store_id, product, unit_price, qty, sale_date,
-- sale_month. Revenue = unit_price * qty.
--
-- February data intentionally contains pricing errors (Tool C at $45.00
-- instead of the correct $48.50) that will be corrected via partition replace.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: monthly_sales — 60 retail transactions across 3 months
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.monthly_sales (
    id         INT,
    store_id   VARCHAR,
    product    VARCHAR,
    unit_price DOUBLE,
    qty        INT,
    sale_date  VARCHAR,
    sale_month VARCHAR
) LOCATION 'delta-partition-replace/monthly_sales'
PARTITIONED BY (sale_month);


-- January 2024: ids 1-20
INSERT INTO {{zone_name}}.delta_demos.monthly_sales VALUES
    (1,  'STORE-01', 'Widget A',    120.50, 3, '2024-01-03', '2024-01'),
    (2,  'STORE-02', 'Gadget B',    89.99,  1, '2024-01-05', '2024-01'),
    (3,  'STORE-01', 'Widget A',    120.50, 2, '2024-01-07', '2024-01'),
    (4,  'STORE-03', 'Tool C',      45.00,  5, '2024-01-08', '2024-01'),
    (5,  'STORE-02', 'Widget A',    120.50, 1, '2024-01-10', '2024-01'),
    (6,  'STORE-01', 'Gadget B',    89.99,  2, '2024-01-12', '2024-01'),
    (7,  'STORE-03', 'Accessory D', 15.99,  8, '2024-01-13', '2024-01'),
    (8,  'STORE-01', 'Tool C',      45.00,  3, '2024-01-15', '2024-01'),
    (9,  'STORE-02', 'Accessory D', 15.99,  6, '2024-01-17', '2024-01'),
    (10, 'STORE-03', 'Widget A',    120.50, 2, '2024-01-18', '2024-01'),
    (11, 'STORE-01', 'Gadget B',    89.99,  1, '2024-01-19', '2024-01'),
    (12, 'STORE-02', 'Tool C',      45.00,  4, '2024-01-20', '2024-01'),
    (13, 'STORE-03', 'Widget A',    120.50, 1, '2024-01-22', '2024-01'),
    (14, 'STORE-01', 'Accessory D', 15.99,  10,'2024-01-23', '2024-01'),
    (15, 'STORE-02', 'Gadget B',    89.99,  2, '2024-01-24', '2024-01'),
    (16, 'STORE-03', 'Tool C',      45.00,  3, '2024-01-25', '2024-01'),
    (17, 'STORE-01', 'Widget A',    120.50, 1, '2024-01-26', '2024-01'),
    (18, 'STORE-02', 'Accessory D', 15.99,  5, '2024-01-27', '2024-01'),
    (19, 'STORE-03', 'Gadget B',    89.99,  1, '2024-01-28', '2024-01'),
    (20, 'STORE-01', 'Tool C',      45.00,  2, '2024-01-30', '2024-01');

-- February 2024: ids 21-40 (contains pricing errors in Tool C rows)
INSERT INTO {{zone_name}}.delta_demos.monthly_sales VALUES
    (21, 'STORE-01', 'Widget A',    120.50, 4, '2024-02-02', '2024-02'),
    (22, 'STORE-02', 'Gadget B',    89.99,  2, '2024-02-03', '2024-02'),
    (23, 'STORE-03', 'Tool C',      45.00,  6, '2024-02-05', '2024-02'),
    (24, 'STORE-01', 'Accessory D', 15.99,  4, '2024-02-06', '2024-02'),
    (25, 'STORE-02', 'Widget A',    120.50, 3, '2024-02-08', '2024-02'),
    (26, 'STORE-03', 'Gadget B',    89.99,  1, '2024-02-09', '2024-02'),
    (27, 'STORE-01', 'Tool C',      45.00,  5, '2024-02-10', '2024-02'),
    (28, 'STORE-02', 'Accessory D', 15.99,  7, '2024-02-12', '2024-02'),
    (29, 'STORE-03', 'Widget A',    120.50, 2, '2024-02-13', '2024-02'),
    (30, 'STORE-01', 'Gadget B',    89.99,  3, '2024-02-15', '2024-02'),
    (31, 'STORE-02', 'Tool C',      45.00,  2, '2024-02-16', '2024-02'),
    (32, 'STORE-03', 'Accessory D', 15.99,  9, '2024-02-17', '2024-02'),
    (33, 'STORE-01', 'Widget A',    120.50, 1, '2024-02-19', '2024-02'),
    (34, 'STORE-02', 'Gadget B',    89.99,  2, '2024-02-20', '2024-02'),
    (35, 'STORE-03', 'Tool C',      45.00,  3, '2024-02-22', '2024-02'),
    (36, 'STORE-01', 'Accessory D', 15.99,  6, '2024-02-23', '2024-02'),
    (37, 'STORE-02', 'Widget A',    120.50, 2, '2024-02-24', '2024-02'),
    (38, 'STORE-03', 'Gadget B',    89.99,  1, '2024-02-25', '2024-02'),
    (39, 'STORE-01', 'Tool C',      45.00,  4, '2024-02-27', '2024-02'),
    (40, 'STORE-02', 'Widget A',    120.50, 1, '2024-02-28', '2024-02');

-- March 2024: ids 41-60
INSERT INTO {{zone_name}}.delta_demos.monthly_sales VALUES
    (41, 'STORE-01', 'Widget A',    120.50, 5, '2024-03-01', '2024-03'),
    (42, 'STORE-02', 'Gadget B',    89.99,  3, '2024-03-03', '2024-03'),
    (43, 'STORE-03', 'Tool C',      45.00,  7, '2024-03-04', '2024-03'),
    (44, 'STORE-01', 'Accessory D', 15.99,  5, '2024-03-05', '2024-03'),
    (45, 'STORE-02', 'Widget A',    120.50, 2, '2024-03-07', '2024-03'),
    (46, 'STORE-03', 'Gadget B',    89.99,  2, '2024-03-08', '2024-03'),
    (47, 'STORE-01', 'Tool C',      45.00,  4, '2024-03-10', '2024-03'),
    (48, 'STORE-02', 'Accessory D', 15.99,  8, '2024-03-11', '2024-03'),
    (49, 'STORE-03', 'Widget A',    120.50, 3, '2024-03-13', '2024-03'),
    (50, 'STORE-01', 'Gadget B',    89.99,  1, '2024-03-14', '2024-03'),
    (51, 'STORE-02', 'Tool C',      45.00,  3, '2024-03-15', '2024-03'),
    (52, 'STORE-03', 'Accessory D', 15.99,  6, '2024-03-17', '2024-03'),
    (53, 'STORE-01', 'Widget A',    120.50, 2, '2024-03-18', '2024-03'),
    (54, 'STORE-02', 'Gadget B',    89.99,  4, '2024-03-20', '2024-03'),
    (55, 'STORE-03', 'Tool C',      45.00,  2, '2024-03-21', '2024-03'),
    (56, 'STORE-01', 'Accessory D', 15.99,  7, '2024-03-22', '2024-03'),
    (57, 'STORE-02', 'Widget A',    120.50, 1, '2024-03-24', '2024-03'),
    (58, 'STORE-03', 'Gadget B',    89.99,  2, '2024-03-25', '2024-03'),
    (59, 'STORE-01', 'Tool C',      45.00,  5, '2024-03-27', '2024-03'),
    (60, 'STORE-02', 'Widget A',    120.50, 3, '2024-03-29', '2024-03');
