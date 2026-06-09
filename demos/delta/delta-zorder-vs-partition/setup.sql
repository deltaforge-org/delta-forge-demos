-- ============================================================================
-- Delta Z-ORDER vs Partitioning — Choosing the Right Data Layout — Setup
-- ============================================================================
-- Creates two copies of the same e-commerce order dataset:
--   1. orders_partitioned — PARTITIONED BY customer_region (5 partitions)
--   2. orders_zorder      — Unpartitioned (Z-ORDER applied in queries.sql)
--
-- 100 orders across 5 regions, 4 product categories, 20 dates.
-- Two batch inserts per table to create file scatter for the Z-ORDER table.
--
-- Tables created:
--   1. orders_partitioned — 100 rows, partitioned by customer_region
--   2. orders_zorder      — 100 rows, unpartitioned (to be Z-ORDERed)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: orders_partitioned — Partitioned by customer_region
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.orders_partitioned (
    id                INT,
    order_id          VARCHAR,
    customer_region   VARCHAR,
    product_category  VARCHAR,
    order_amount      DOUBLE,
    quantity          INT,
    order_date        VARCHAR,
    payment_method    VARCHAR,
    shipping_priority VARCHAR,
    customer_rating   INT
) PARTITIONED BY (customer_region)
  LOCATION 'delta-zorder-vs-partition/orders_partitioned';


-- ============================================================================
-- TABLE 2: orders_zorder — Unpartitioned (Z-ORDER applied later)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.orders_zorder (
    id                INT,
    order_id          VARCHAR,
    customer_region   VARCHAR,
    product_category  VARCHAR,
    order_amount      DOUBLE,
    quantity          INT,
    order_date        VARCHAR,
    payment_method    VARCHAR,
    shipping_priority VARCHAR,
    customer_rating   INT
) LOCATION 'delta-zorder-vs-partition/orders_zorder';


-- ============================================================================
-- STEP 2: Batch 1 — 50 orders (rows 1-50) into both tables
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.orders_partitioned VALUES
    (1,  'ORD-0001', 'north-america', 'electronics',  349.99, 1, '2025-01-15', 'credit_card',   'express',   4),
    (2,  'ORD-0002', 'europe',        'clothing',      89.50, 3, '2025-01-15', 'paypal',        'standard',  5),
    (3,  'ORD-0003', 'asia-pacific',  'home-garden',  145.00, 2, '2025-01-15', 'credit_card',   'standard',  3),
    (4,  'ORD-0004', 'north-america', 'sports',        67.25, 4, '2025-01-15', 'bank_transfer', 'standard',  4),
    (5,  'ORD-0005', 'latin-america', 'electronics',  219.90, 1, '2025-01-18', 'credit_card',   'express',   5),
    (6,  'ORD-0006', 'europe',        'home-garden',   78.30, 2, '2025-01-18', 'paypal',        'standard',  4),
    (7,  'ORD-0007', 'north-america', 'clothing',     124.75, 1, '2025-01-18', 'credit_card',   'overnight', 3),
    (8,  'ORD-0008', 'middle-east',   'electronics',  289.00, 1, '2025-01-18', 'bank_transfer', 'express',   4),
    (9,  'ORD-0009', 'asia-pacific',  'clothing',      56.80, 5, '2025-01-18', 'paypal',        'standard',  2),
    (10, 'ORD-0010', 'north-america', 'electronics',  199.95, 2, '2025-01-18', 'credit_card',   'standard',  5),
    (11, 'ORD-0011', 'europe',        'electronics',  374.50, 1, '2025-01-22', 'credit_card',   'express',   4),
    (12, 'ORD-0012', 'north-america', 'home-garden',   43.20, 3, '2025-01-22', 'paypal',        'standard',  3),
    (13, 'ORD-0013', 'asia-pacific',  'sports',        34.99, 2, '2025-01-22', 'credit_card',   'standard',  4),
    (14, 'ORD-0014', 'latin-america', 'clothing',      72.10, 2, '2025-01-22', 'bank_transfer', 'standard',  3),
    (15, 'ORD-0015', 'europe',        'clothing',     158.40, 1, '2025-01-22', 'credit_card',   'express',   5),
    (16, 'ORD-0016', 'north-america', 'sports',        89.99, 3, '2025-01-25', 'paypal',        'standard',  4),
    (17, 'ORD-0017', 'middle-east',   'clothing',      47.60, 4, '2025-01-25', 'credit_card',   'standard',  3),
    (18, 'ORD-0018', 'asia-pacific',  'electronics',  267.80, 1, '2025-01-25', 'bank_transfer', 'express',   5),
    (19, 'ORD-0019', 'north-america', 'clothing',      95.25, 2, '2025-01-28', 'credit_card',   'standard',  4),
    (20, 'ORD-0020', 'europe',        'sports',        55.70, 3, '2025-01-28', 'paypal',        'standard',  3),
    (21, 'ORD-0021', 'north-america', 'electronics',  425.00, 1, '2025-01-28', 'credit_card',   'overnight', 5),
    (22, 'ORD-0022', 'latin-america', 'home-garden',   63.45, 2, '2025-01-28', 'paypal',        'standard',  4),
    (23, 'ORD-0023', 'europe',        'home-garden',  187.90, 1, '2025-01-28', 'credit_card',   'express',   4),
    (24, 'ORD-0024', 'asia-pacific',  'clothing',     112.30, 2, '2025-01-28', 'bank_transfer', 'standard',  3),
    (25, 'ORD-0025', 'middle-east',   'sports',        29.99, 6, '2025-01-28', 'paypal',        'standard',  2),
    (26, 'ORD-0026', 'north-america', 'home-garden',  234.15, 1, '2025-02-01', 'credit_card',   'express',   5),
    (27, 'ORD-0027', 'europe',        'electronics',  312.60, 1, '2025-02-01', 'credit_card',   'express',   4),
    (28, 'ORD-0028', 'north-america', 'clothing',      68.90, 3, '2025-02-01', 'paypal',        'standard',  4),
    (29, 'ORD-0029', 'asia-pacific',  'home-garden',   97.50, 2, '2025-02-01', 'credit_card',   'standard',  3),
    (30, 'ORD-0030', 'latin-america', 'sports',        41.80, 4, '2025-02-01', 'bank_transfer', 'standard',  3),
    (31, 'ORD-0031', 'north-america', 'electronics',  178.50, 2, '2025-02-01', 'credit_card',   'standard',  4),
    (32, 'ORD-0032', 'europe',        'clothing',      83.20, 1, '2025-02-01', 'paypal',        'standard',  5),
    (33, 'ORD-0033', 'asia-pacific',  'electronics',  291.75, 1, '2025-02-01', 'credit_card',   'express',   4),
    (34, 'ORD-0034', 'north-america', 'sports',       119.40, 2, '2025-02-04', 'bank_transfer', 'standard',  3),
    (35, 'ORD-0035', 'middle-east',   'home-garden',   52.10, 3, '2025-02-04', 'credit_card',   'standard',  4),
    (36, 'ORD-0036', 'europe',        'electronics',  246.80, 1, '2025-02-04', 'paypal',        'express',   5),
    (37, 'ORD-0037', 'latin-america', 'clothing',     135.60, 1, '2025-02-04', 'credit_card',   'overnight', 4),
    (38, 'ORD-0038', 'north-america', 'home-garden',   88.75, 4, '2025-02-08', 'credit_card',   'standard',  3),
    (39, 'ORD-0039', 'asia-pacific',  'sports',        73.20, 2, '2025-02-08', 'paypal',        'standard',  4),
    (40, 'ORD-0040', 'europe',        'home-garden',  162.35, 1, '2025-02-08', 'credit_card',   'express',   4),
    (41, 'ORD-0041', 'north-america', 'clothing',     176.80, 1, '2025-02-08', 'paypal',        'express',   5),
    (42, 'ORD-0042', 'middle-east',   'electronics',  198.50, 2, '2025-02-08', 'credit_card',   'standard',  4),
    (43, 'ORD-0043', 'asia-pacific',  'home-garden',  211.00, 1, '2025-02-10', 'bank_transfer', 'express',   3),
    (44, 'ORD-0044', 'latin-america', 'electronics',  333.25, 1, '2025-02-10', 'credit_card',   'overnight', 5),
    (45, 'ORD-0045', 'north-america', 'electronics',  156.30, 3, '2025-02-10', 'credit_card',   'standard',  4),
    (46, 'ORD-0046', 'europe',        'sports',        64.90, 2, '2025-02-13', 'paypal',        'standard',  3),
    (47, 'ORD-0047', 'north-america', 'clothing',      42.15, 5, '2025-02-13', 'bank_transfer', 'standard',  2),
    (48, 'ORD-0048', 'asia-pacific',  'electronics',  385.00, 1, '2025-02-13', 'credit_card',   'express',   5),
    (49, 'ORD-0049', 'latin-america', 'home-garden',  128.90, 2, '2025-02-13', 'paypal',        'standard',  4),
    (50, 'ORD-0050', 'europe',        'clothing',     101.55, 2, '2025-02-13', 'credit_card',   'standard',  4);

INSERT INTO {{zone_name}}.delta_demos.orders_zorder VALUES
    (1,  'ORD-0001', 'north-america', 'electronics',  349.99, 1, '2025-01-15', 'credit_card',   'express',   4),
    (2,  'ORD-0002', 'europe',        'clothing',      89.50, 3, '2025-01-15', 'paypal',        'standard',  5),
    (3,  'ORD-0003', 'asia-pacific',  'home-garden',  145.00, 2, '2025-01-15', 'credit_card',   'standard',  3),
    (4,  'ORD-0004', 'north-america', 'sports',        67.25, 4, '2025-01-15', 'bank_transfer', 'standard',  4),
    (5,  'ORD-0005', 'latin-america', 'electronics',  219.90, 1, '2025-01-18', 'credit_card',   'express',   5),
    (6,  'ORD-0006', 'europe',        'home-garden',   78.30, 2, '2025-01-18', 'paypal',        'standard',  4),
    (7,  'ORD-0007', 'north-america', 'clothing',     124.75, 1, '2025-01-18', 'credit_card',   'overnight', 3),
    (8,  'ORD-0008', 'middle-east',   'electronics',  289.00, 1, '2025-01-18', 'bank_transfer', 'express',   4),
    (9,  'ORD-0009', 'asia-pacific',  'clothing',      56.80, 5, '2025-01-18', 'paypal',        'standard',  2),
    (10, 'ORD-0010', 'north-america', 'electronics',  199.95, 2, '2025-01-18', 'credit_card',   'standard',  5),
    (11, 'ORD-0011', 'europe',        'electronics',  374.50, 1, '2025-01-22', 'credit_card',   'express',   4),
    (12, 'ORD-0012', 'north-america', 'home-garden',   43.20, 3, '2025-01-22', 'paypal',        'standard',  3),
    (13, 'ORD-0013', 'asia-pacific',  'sports',        34.99, 2, '2025-01-22', 'credit_card',   'standard',  4),
    (14, 'ORD-0014', 'latin-america', 'clothing',      72.10, 2, '2025-01-22', 'bank_transfer', 'standard',  3),
    (15, 'ORD-0015', 'europe',        'clothing',     158.40, 1, '2025-01-22', 'credit_card',   'express',   5),
    (16, 'ORD-0016', 'north-america', 'sports',        89.99, 3, '2025-01-25', 'paypal',        'standard',  4),
    (17, 'ORD-0017', 'middle-east',   'clothing',      47.60, 4, '2025-01-25', 'credit_card',   'standard',  3),
    (18, 'ORD-0018', 'asia-pacific',  'electronics',  267.80, 1, '2025-01-25', 'bank_transfer', 'express',   5),
    (19, 'ORD-0019', 'north-america', 'clothing',      95.25, 2, '2025-01-28', 'credit_card',   'standard',  4),
    (20, 'ORD-0020', 'europe',        'sports',        55.70, 3, '2025-01-28', 'paypal',        'standard',  3),
    (21, 'ORD-0021', 'north-america', 'electronics',  425.00, 1, '2025-01-28', 'credit_card',   'overnight', 5),
    (22, 'ORD-0022', 'latin-america', 'home-garden',   63.45, 2, '2025-01-28', 'paypal',        'standard',  4),
    (23, 'ORD-0023', 'europe',        'home-garden',  187.90, 1, '2025-01-28', 'credit_card',   'express',   4),
    (24, 'ORD-0024', 'asia-pacific',  'clothing',     112.30, 2, '2025-01-28', 'bank_transfer', 'standard',  3),
    (25, 'ORD-0025', 'middle-east',   'sports',        29.99, 6, '2025-01-28', 'paypal',        'standard',  2),
    (26, 'ORD-0026', 'north-america', 'home-garden',  234.15, 1, '2025-02-01', 'credit_card',   'express',   5),
    (27, 'ORD-0027', 'europe',        'electronics',  312.60, 1, '2025-02-01', 'credit_card',   'express',   4),
    (28, 'ORD-0028', 'north-america', 'clothing',      68.90, 3, '2025-02-01', 'paypal',        'standard',  4),
    (29, 'ORD-0029', 'asia-pacific',  'home-garden',   97.50, 2, '2025-02-01', 'credit_card',   'standard',  3),
    (30, 'ORD-0030', 'latin-america', 'sports',        41.80, 4, '2025-02-01', 'bank_transfer', 'standard',  3),
    (31, 'ORD-0031', 'north-america', 'electronics',  178.50, 2, '2025-02-01', 'credit_card',   'standard',  4),
    (32, 'ORD-0032', 'europe',        'clothing',      83.20, 1, '2025-02-01', 'paypal',        'standard',  5),
    (33, 'ORD-0033', 'asia-pacific',  'electronics',  291.75, 1, '2025-02-01', 'credit_card',   'express',   4),
    (34, 'ORD-0034', 'north-america', 'sports',       119.40, 2, '2025-02-04', 'bank_transfer', 'standard',  3),
    (35, 'ORD-0035', 'middle-east',   'home-garden',   52.10, 3, '2025-02-04', 'credit_card',   'standard',  4),
    (36, 'ORD-0036', 'europe',        'electronics',  246.80, 1, '2025-02-04', 'paypal',        'express',   5),
    (37, 'ORD-0037', 'latin-america', 'clothing',     135.60, 1, '2025-02-04', 'credit_card',   'overnight', 4),
    (38, 'ORD-0038', 'north-america', 'home-garden',   88.75, 4, '2025-02-08', 'credit_card',   'standard',  3),
    (39, 'ORD-0039', 'asia-pacific',  'sports',        73.20, 2, '2025-02-08', 'paypal',        'standard',  4),
    (40, 'ORD-0040', 'europe',        'home-garden',  162.35, 1, '2025-02-08', 'credit_card',   'express',   4),
    (41, 'ORD-0041', 'north-america', 'clothing',     176.80, 1, '2025-02-08', 'paypal',        'express',   5),
    (42, 'ORD-0042', 'middle-east',   'electronics',  198.50, 2, '2025-02-08', 'credit_card',   'standard',  4),
    (43, 'ORD-0043', 'asia-pacific',  'home-garden',  211.00, 1, '2025-02-10', 'bank_transfer', 'express',   3),
    (44, 'ORD-0044', 'latin-america', 'electronics',  333.25, 1, '2025-02-10', 'credit_card',   'overnight', 5),
    (45, 'ORD-0045', 'north-america', 'electronics',  156.30, 3, '2025-02-10', 'credit_card',   'standard',  4),
    (46, 'ORD-0046', 'europe',        'sports',        64.90, 2, '2025-02-13', 'paypal',        'standard',  3),
    (47, 'ORD-0047', 'north-america', 'clothing',      42.15, 5, '2025-02-13', 'bank_transfer', 'standard',  2),
    (48, 'ORD-0048', 'asia-pacific',  'electronics',  385.00, 1, '2025-02-13', 'credit_card',   'express',   5),
    (49, 'ORD-0049', 'latin-america', 'home-garden',  128.90, 2, '2025-02-13', 'paypal',        'standard',  4),
    (50, 'ORD-0050', 'europe',        'clothing',     101.55, 2, '2025-02-13', 'credit_card',   'standard',  4);


-- ============================================================================
-- STEP 3: Batch 2 — 50 orders (rows 51-100) into both tables
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.orders_partitioned
SELECT * FROM (VALUES
    (51, 'ORD-0051', 'north-america', 'home-garden',   79.60, 3, '2025-02-13', 'credit_card',   'standard',  3),
    (52, 'ORD-0052', 'asia-pacific',  'clothing',      65.40, 2, '2025-02-15', 'paypal',        'standard',  4),
    (53, 'ORD-0053', 'middle-east',   'home-garden',  143.70, 1, '2025-02-15', 'credit_card',   'express',   5),
    (54, 'ORD-0054', 'north-america', 'electronics',  287.45, 1, '2025-02-15', 'bank_transfer', 'overnight', 4),
    (55, 'ORD-0055', 'europe',        'electronics',  195.20, 2, '2025-02-15', 'credit_card',   'express',   5),
    (56, 'ORD-0056', 'latin-america', 'clothing',      58.30, 3, '2025-02-15', 'paypal',        'standard',  3),
    (57, 'ORD-0057', 'north-america', 'sports',        45.80, 4, '2025-02-15', 'credit_card',   'standard',  4),
    (58, 'ORD-0058', 'asia-pacific',  'home-garden',   37.25, 6, '2025-02-15', 'bank_transfer', 'standard',  2),
    (59, 'ORD-0059', 'europe',        'clothing',     189.90, 1, '2025-02-19', 'credit_card',   'express',   5),
    (60, 'ORD-0060', 'north-america', 'electronics',  223.10, 1, '2025-02-19', 'paypal',        'standard',  4),
    (61, 'ORD-0061', 'middle-east',   'clothing',      93.40, 2, '2025-02-19', 'credit_card',   'standard',  3),
    (62, 'ORD-0062', 'asia-pacific',  'electronics',  168.75, 2, '2025-02-19', 'paypal',        'standard',  4),
    (63, 'ORD-0063', 'north-america', 'home-garden',  256.80, 1, '2025-02-19', 'credit_card',   'express',   5),
    (64, 'ORD-0064', 'latin-america', 'sports',        82.15, 2, '2025-02-22', 'bank_transfer', 'standard',  4),
    (65, 'ORD-0065', 'europe',        'home-garden',   71.40, 4, '2025-02-22', 'credit_card',   'standard',  3),
    (66, 'ORD-0066', 'north-america', 'clothing',     147.60, 1, '2025-02-22', 'paypal',        'overnight', 4),
    (67, 'ORD-0067', 'asia-pacific',  'sports',       108.90, 1, '2025-02-22', 'credit_card',   'express',   5),
    (68, 'ORD-0068', 'north-america', 'electronics',  315.70, 1, '2025-02-25', 'credit_card',   'express',   4),
    (69, 'ORD-0069', 'europe',        'electronics',  402.30, 1, '2025-02-25', 'bank_transfer', 'overnight', 5),
    (70, 'ORD-0070', 'latin-america', 'home-garden',   94.55, 3, '2025-02-25', 'paypal',        'standard',  3),
    (71, 'ORD-0071', 'north-america', 'sports',        53.40, 3, '2025-02-25', 'credit_card',   'standard',  3),
    (72, 'ORD-0072', 'asia-pacific',  'clothing',     139.80, 1, '2025-02-25', 'credit_card',   'express',   4),
    (73, 'ORD-0073', 'middle-east',   'electronics',  257.90, 1, '2025-02-25', 'paypal',        'express',   4),
    (74, 'ORD-0074', 'europe',        'clothing',      76.25, 3, '2025-02-28', 'credit_card',   'standard',  3),
    (75, 'ORD-0075', 'north-america', 'home-garden',  185.30, 2, '2025-02-28', 'bank_transfer', 'standard',  5),
    (76, 'ORD-0076', 'asia-pacific',  'electronics',  204.60, 1, '2025-02-28', 'credit_card',   'express',   4),
    (77, 'ORD-0077', 'latin-america', 'clothing',      49.95, 4, '2025-03-02', 'paypal',        'standard',  2),
    (78, 'ORD-0078', 'north-america', 'electronics',  163.40, 2, '2025-03-02', 'credit_card',   'standard',  4),
    (79, 'ORD-0079', 'europe',        'home-garden',  118.65, 2, '2025-03-02', 'paypal',        'standard',  4),
    (80, 'ORD-0080', 'asia-pacific',  'home-garden',   86.70, 3, '2025-03-02', 'bank_transfer', 'standard',  3),
    (81, 'ORD-0081', 'north-america', 'clothing',     110.20, 2, '2025-03-02', 'credit_card',   'express',   4),
    (82, 'ORD-0082', 'middle-east',   'sports',        38.50, 5, '2025-03-05', 'credit_card',   'standard',  3),
    (83, 'ORD-0083', 'europe',        'electronics',  278.90, 1, '2025-03-05', 'paypal',        'express',   5),
    (84, 'ORD-0084', 'asia-pacific',  'clothing',      91.35, 2, '2025-03-05', 'credit_card',   'standard',  4),
    (85, 'ORD-0085', 'north-america', 'home-garden',   67.80, 3, '2025-03-05', 'bank_transfer', 'standard',  3),
    (86, 'ORD-0086', 'latin-america', 'electronics',  241.15, 1, '2025-03-08', 'credit_card',   'overnight', 5),
    (87, 'ORD-0087', 'north-america', 'sports',       142.70, 1, '2025-03-08', 'paypal',        'express',   4),
    (88, 'ORD-0088', 'europe',        'clothing',     167.45, 1, '2025-03-08', 'credit_card',   'express',   5),
    (89, 'ORD-0089', 'asia-pacific',  'sports',        59.80, 3, '2025-03-08', 'credit_card',   'standard',  3),
    (90, 'ORD-0090', 'north-america', 'electronics',  192.55, 2, '2025-03-08', 'bank_transfer', 'standard',  4),
    (91, 'ORD-0091', 'middle-east',   'home-garden',   74.20, 2, '2025-03-08', 'paypal',        'standard',  4),
    (92, 'ORD-0092', 'europe',        'sports',       126.30, 1, '2025-03-12', 'credit_card',   'express',   4),
    (93, 'ORD-0093', 'latin-america', 'home-garden',   55.90, 4, '2025-03-12', 'credit_card',   'standard',  3),
    (94, 'ORD-0094', 'north-america', 'clothing',      83.70, 2, '2025-03-12', 'paypal',        'standard',  4),
    (95, 'ORD-0095', 'asia-pacific',  'electronics',  328.40, 1, '2025-03-12', 'credit_card',   'express',   5),
    (96, 'ORD-0096', 'europe',        'home-garden',  199.85, 1, '2025-03-12', 'bank_transfer', 'overnight', 4),
    (97, 'ORD-0097', 'north-america', 'home-garden',  152.40, 2, '2025-03-15', 'credit_card',   'standard',  5),
    (98, 'ORD-0098', 'latin-america', 'sports',        27.60, 7, '2025-03-15', 'paypal',        'standard',  2),
    (99, 'ORD-0099', 'asia-pacific',  'home-garden',  174.25, 1, '2025-03-15', 'credit_card',   'express',   4),
    (100,'ORD-0100', 'north-america', 'electronics',  261.30, 1, '2025-03-15', 'credit_card',   'express',   5)
) AS t(id, order_id, customer_region, product_category, order_amount, quantity, order_date, payment_method, shipping_priority, customer_rating);

INSERT INTO {{zone_name}}.delta_demos.orders_zorder
SELECT * FROM (VALUES
    (51, 'ORD-0051', 'north-america', 'home-garden',   79.60, 3, '2025-02-13', 'credit_card',   'standard',  3),
    (52, 'ORD-0052', 'asia-pacific',  'clothing',      65.40, 2, '2025-02-15', 'paypal',        'standard',  4),
    (53, 'ORD-0053', 'middle-east',   'home-garden',  143.70, 1, '2025-02-15', 'credit_card',   'express',   5),
    (54, 'ORD-0054', 'north-america', 'electronics',  287.45, 1, '2025-02-15', 'bank_transfer', 'overnight', 4),
    (55, 'ORD-0055', 'europe',        'electronics',  195.20, 2, '2025-02-15', 'credit_card',   'express',   5),
    (56, 'ORD-0056', 'latin-america', 'clothing',      58.30, 3, '2025-02-15', 'paypal',        'standard',  3),
    (57, 'ORD-0057', 'north-america', 'sports',        45.80, 4, '2025-02-15', 'credit_card',   'standard',  4),
    (58, 'ORD-0058', 'asia-pacific',  'home-garden',   37.25, 6, '2025-02-15', 'bank_transfer', 'standard',  2),
    (59, 'ORD-0059', 'europe',        'clothing',     189.90, 1, '2025-02-19', 'credit_card',   'express',   5),
    (60, 'ORD-0060', 'north-america', 'electronics',  223.10, 1, '2025-02-19', 'paypal',        'standard',  4),
    (61, 'ORD-0061', 'middle-east',   'clothing',      93.40, 2, '2025-02-19', 'credit_card',   'standard',  3),
    (62, 'ORD-0062', 'asia-pacific',  'electronics',  168.75, 2, '2025-02-19', 'paypal',        'standard',  4),
    (63, 'ORD-0063', 'north-america', 'home-garden',  256.80, 1, '2025-02-19', 'credit_card',   'express',   5),
    (64, 'ORD-0064', 'latin-america', 'sports',        82.15, 2, '2025-02-22', 'bank_transfer', 'standard',  4),
    (65, 'ORD-0065', 'europe',        'home-garden',   71.40, 4, '2025-02-22', 'credit_card',   'standard',  3),
    (66, 'ORD-0066', 'north-america', 'clothing',     147.60, 1, '2025-02-22', 'paypal',        'overnight', 4),
    (67, 'ORD-0067', 'asia-pacific',  'sports',       108.90, 1, '2025-02-22', 'credit_card',   'express',   5),
    (68, 'ORD-0068', 'north-america', 'electronics',  315.70, 1, '2025-02-25', 'credit_card',   'express',   4),
    (69, 'ORD-0069', 'europe',        'electronics',  402.30, 1, '2025-02-25', 'bank_transfer', 'overnight', 5),
    (70, 'ORD-0070', 'latin-america', 'home-garden',   94.55, 3, '2025-02-25', 'paypal',        'standard',  3),
    (71, 'ORD-0071', 'north-america', 'sports',        53.40, 3, '2025-02-25', 'credit_card',   'standard',  3),
    (72, 'ORD-0072', 'asia-pacific',  'clothing',     139.80, 1, '2025-02-25', 'credit_card',   'express',   4),
    (73, 'ORD-0073', 'middle-east',   'electronics',  257.90, 1, '2025-02-25', 'paypal',        'express',   4),
    (74, 'ORD-0074', 'europe',        'clothing',      76.25, 3, '2025-02-28', 'credit_card',   'standard',  3),
    (75, 'ORD-0075', 'north-america', 'home-garden',  185.30, 2, '2025-02-28', 'bank_transfer', 'standard',  5),
    (76, 'ORD-0076', 'asia-pacific',  'electronics',  204.60, 1, '2025-02-28', 'credit_card',   'express',   4),
    (77, 'ORD-0077', 'latin-america', 'clothing',      49.95, 4, '2025-03-02', 'paypal',        'standard',  2),
    (78, 'ORD-0078', 'north-america', 'electronics',  163.40, 2, '2025-03-02', 'credit_card',   'standard',  4),
    (79, 'ORD-0079', 'europe',        'home-garden',  118.65, 2, '2025-03-02', 'paypal',        'standard',  4),
    (80, 'ORD-0080', 'asia-pacific',  'home-garden',   86.70, 3, '2025-03-02', 'bank_transfer', 'standard',  3),
    (81, 'ORD-0081', 'north-america', 'clothing',     110.20, 2, '2025-03-02', 'credit_card',   'express',   4),
    (82, 'ORD-0082', 'middle-east',   'sports',        38.50, 5, '2025-03-05', 'credit_card',   'standard',  3),
    (83, 'ORD-0083', 'europe',        'electronics',  278.90, 1, '2025-03-05', 'paypal',        'express',   5),
    (84, 'ORD-0084', 'asia-pacific',  'clothing',      91.35, 2, '2025-03-05', 'credit_card',   'standard',  4),
    (85, 'ORD-0085', 'north-america', 'home-garden',   67.80, 3, '2025-03-05', 'bank_transfer', 'standard',  3),
    (86, 'ORD-0086', 'latin-america', 'electronics',  241.15, 1, '2025-03-08', 'credit_card',   'overnight', 5),
    (87, 'ORD-0087', 'north-america', 'sports',       142.70, 1, '2025-03-08', 'paypal',        'express',   4),
    (88, 'ORD-0088', 'europe',        'clothing',     167.45, 1, '2025-03-08', 'credit_card',   'express',   5),
    (89, 'ORD-0089', 'asia-pacific',  'sports',        59.80, 3, '2025-03-08', 'credit_card',   'standard',  3),
    (90, 'ORD-0090', 'north-america', 'electronics',  192.55, 2, '2025-03-08', 'bank_transfer', 'standard',  4),
    (91, 'ORD-0091', 'middle-east',   'home-garden',   74.20, 2, '2025-03-08', 'paypal',        'standard',  4),
    (92, 'ORD-0092', 'europe',        'sports',       126.30, 1, '2025-03-12', 'credit_card',   'express',   4),
    (93, 'ORD-0093', 'latin-america', 'home-garden',   55.90, 4, '2025-03-12', 'credit_card',   'standard',  3),
    (94, 'ORD-0094', 'north-america', 'clothing',      83.70, 2, '2025-03-12', 'paypal',        'standard',  4),
    (95, 'ORD-0095', 'asia-pacific',  'electronics',  328.40, 1, '2025-03-12', 'credit_card',   'express',   5),
    (96, 'ORD-0096', 'europe',        'home-garden',  199.85, 1, '2025-03-12', 'bank_transfer', 'overnight', 4),
    (97, 'ORD-0097', 'north-america', 'home-garden',  152.40, 2, '2025-03-15', 'credit_card',   'standard',  5),
    (98, 'ORD-0098', 'latin-america', 'sports',        27.60, 7, '2025-03-15', 'paypal',        'standard',  2),
    (99, 'ORD-0099', 'asia-pacific',  'home-garden',  174.25, 1, '2025-03-15', 'credit_card',   'express',   4),
    (100,'ORD-0100', 'north-america', 'electronics',  261.30, 1, '2025-03-15', 'credit_card',   'express',   5)
) AS t(id, order_id, customer_region, product_category, order_amount, quantity, order_date, payment_method, shipping_priority, customer_rating);
