-- ============================================================================
-- Delta Partition Pitfalls — Setup Script
-- ============================================================================
-- Creates the events_by_customer table PARTITIONED BY (customer_id) with 60
-- rows — the intentionally WRONG partitioning strategy. 20 unique customers
-- produce 20 partition directories with only 3 rows each (the anti-pattern).
--
-- Table: events_by_customer — 60 rows, partitioned by customer_id
--
-- Distribution:
--   20 customers (C01-C20), each with exactly 3 events (one per month)
--   3 months: 2024-01, 2024-02, 2024-03 — 20 events each
--   Event types: 30 page_view, 15 add_to_cart, 9 purchase, 6 search
--   Purchases have dollar amounts; all other events have amount = 0.00
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: events_by_customer — Over-partitioned by customer_id (20 partitions)
-- ============================================================================
-- This is the ANTI-PATTERN: partitioning by a high-cardinality column creates
-- 20 partition directories, each containing only 3 tiny Parquet files.
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.events_by_customer (
    id           INT,
    customer_id  VARCHAR,
    event_type   VARCHAR,
    page_url     VARCHAR,
    amount       DOUBLE,
    created_at   VARCHAR,
    event_month  VARCHAR
) LOCATION 'delta-partition-pitfalls/events_by_customer'
PARTITIONED BY (customer_id);


-- 2024-01 events: ids 1-20
INSERT INTO {{zone_name}}.delta_demos.events_by_customer VALUES
    (1,  'C01', 'page_view',   '/products/shoes',          0.00,   '2024-01-03 10:00:00', '2024-01'),
    (2,  'C02', 'page_view',   '/products/jackets',        0.00,   '2024-01-04 11:07:00', '2024-01'),
    (3,  'C03', 'page_view',   '/products/watches',        0.00,   '2024-01-05 12:14:00', '2024-01'),
    (4,  'C04', 'page_view',   '/home',                    0.00,   '2024-01-07 13:21:00', '2024-01'),
    (5,  'C05', 'page_view',   '/products/bags',           0.00,   '2024-01-08 14:28:00', '2024-01'),
    (6,  'C06', 'page_view',   '/products/hats',           0.00,   '2024-01-09 15:35:00', '2024-01'),
    (7,  'C07', 'page_view',   '/products/shirts',         0.00,   '2024-01-10 16:42:00', '2024-01'),
    (8,  'C08', 'page_view',   '/categories/sale',         0.00,   '2024-01-11 17:49:00', '2024-01'),
    (9,  'C09', 'page_view',   '/products/sunglasses',     0.00,   '2024-01-12 18:56:00', '2024-01'),
    (10, 'C10', 'page_view',   '/blog/trends',             0.00,   '2024-01-14 19:03:00', '2024-01'),
    (11, 'C11', 'add_to_cart', '/cart?add=shoes',           0.00,   '2024-01-15 20:10:00', '2024-01'),
    (12, 'C12', 'add_to_cart', '/cart?add=jackets',         0.00,   '2024-01-16 21:17:00', '2024-01'),
    (13, 'C13', 'add_to_cart', '/cart?add=watches',         0.00,   '2024-01-17 10:24:00', '2024-01'),
    (14, 'C14', 'add_to_cart', '/cart?add=bags',            0.00,   '2024-01-18 11:31:00', '2024-01'),
    (15, 'C15', 'add_to_cart', '/cart?add=hats',            0.00,   '2024-01-19 12:38:00', '2024-01'),
    (16, 'C16', 'purchase',    '/checkout/confirm',         89.99,  '2024-01-20 13:45:00', '2024-01'),
    (17, 'C17', 'purchase',    '/checkout/confirm',         149.50, '2024-01-22 14:52:00', '2024-01'),
    (18, 'C18', 'purchase',    '/checkout/confirm',         249.00, '2024-01-23 15:59:00', '2024-01'),
    (19, 'C19', 'search',      '/search?q=running+shoes',  0.00,   '2024-01-25 16:06:00', '2024-01'),
    (20, 'C20', 'search',      '/search?q=winter+jackets', 0.00,   '2024-01-27 17:13:00', '2024-01');

-- 2024-02 events: ids 21-40
INSERT INTO {{zone_name}}.delta_demos.events_by_customer VALUES
    (21, 'C01', 'page_view',   '/products/shoes',          0.00,   '2024-02-01 10:00:00', '2024-02'),
    (22, 'C02', 'page_view',   '/products/jackets',        0.00,   '2024-02-02 11:07:00', '2024-02'),
    (23, 'C03', 'page_view',   '/products/watches',        0.00,   '2024-02-04 12:14:00', '2024-02'),
    (24, 'C04', 'page_view',   '/home',                    0.00,   '2024-02-05 13:21:00', '2024-02'),
    (25, 'C05', 'page_view',   '/products/bags',           0.00,   '2024-02-06 14:28:00', '2024-02'),
    (26, 'C06', 'page_view',   '/products/hats',           0.00,   '2024-02-08 15:35:00', '2024-02'),
    (27, 'C07', 'page_view',   '/products/shirts',         0.00,   '2024-02-09 16:42:00', '2024-02'),
    (28, 'C08', 'page_view',   '/categories/sale',         0.00,   '2024-02-10 17:49:00', '2024-02'),
    (29, 'C09', 'page_view',   '/products/sunglasses',     0.00,   '2024-02-12 18:56:00', '2024-02'),
    (30, 'C10', 'page_view',   '/blog/trends',             0.00,   '2024-02-13 19:03:00', '2024-02'),
    (31, 'C11', 'add_to_cart', '/cart?add=shoes',           0.00,   '2024-02-14 20:10:00', '2024-02'),
    (32, 'C12', 'add_to_cart', '/cart?add=jackets',         0.00,   '2024-02-15 21:17:00', '2024-02'),
    (33, 'C13', 'add_to_cart', '/cart?add=watches',         0.00,   '2024-02-17 10:24:00', '2024-02'),
    (34, 'C14', 'add_to_cart', '/cart?add=bags',            0.00,   '2024-02-18 11:31:00', '2024-02'),
    (35, 'C15', 'add_to_cart', '/cart?add=hats',            0.00,   '2024-02-19 12:38:00', '2024-02'),
    (36, 'C16', 'purchase',    '/checkout/confirm',         34.95,  '2024-02-20 13:45:00', '2024-02'),
    (37, 'C17', 'purchase',    '/checkout/confirm',         199.00, '2024-02-22 14:52:00', '2024-02'),
    (38, 'C18', 'purchase',    '/checkout/confirm',         75.00,  '2024-02-23 15:59:00', '2024-02'),
    (39, 'C19', 'search',      '/search?q=running+shoes',  0.00,   '2024-02-25 16:06:00', '2024-02'),
    (40, 'C20', 'search',      '/search?q=winter+jackets', 0.00,   '2024-02-27 17:13:00', '2024-02');

-- 2024-03 events: ids 41-60
INSERT INTO {{zone_name}}.delta_demos.events_by_customer VALUES
    (41, 'C01', 'page_view',   '/products/shoes',          0.00,   '2024-03-02 10:00:00', '2024-03'),
    (42, 'C02', 'page_view',   '/products/jackets',        0.00,   '2024-03-03 11:07:00', '2024-03'),
    (43, 'C03', 'page_view',   '/products/watches',        0.00,   '2024-03-04 12:14:00', '2024-03'),
    (44, 'C04', 'page_view',   '/home',                    0.00,   '2024-03-06 13:21:00', '2024-03'),
    (45, 'C05', 'page_view',   '/products/bags',           0.00,   '2024-03-07 14:28:00', '2024-03'),
    (46, 'C06', 'page_view',   '/products/hats',           0.00,   '2024-03-08 15:35:00', '2024-03'),
    (47, 'C07', 'page_view',   '/products/shirts',         0.00,   '2024-03-10 16:42:00', '2024-03'),
    (48, 'C08', 'page_view',   '/categories/sale',         0.00,   '2024-03-11 17:49:00', '2024-03'),
    (49, 'C09', 'page_view',   '/products/sunglasses',     0.00,   '2024-03-12 18:56:00', '2024-03'),
    (50, 'C10', 'page_view',   '/blog/trends',             0.00,   '2024-03-14 19:03:00', '2024-03'),
    (51, 'C11', 'add_to_cart', '/cart?add=shoes',           0.00,   '2024-03-15 20:10:00', '2024-03'),
    (52, 'C12', 'add_to_cart', '/cart?add=jackets',         0.00,   '2024-03-16 21:17:00', '2024-03'),
    (53, 'C13', 'add_to_cart', '/cart?add=watches',         0.00,   '2024-03-17 10:24:00', '2024-03'),
    (54, 'C14', 'add_to_cart', '/cart?add=bags',            0.00,   '2024-03-19 11:31:00', '2024-03'),
    (55, 'C15', 'add_to_cart', '/cart?add=hats',            0.00,   '2024-03-20 12:38:00', '2024-03'),
    (56, 'C16', 'purchase',    '/checkout/confirm',         129.99, '2024-03-21 13:45:00', '2024-03'),
    (57, 'C17', 'purchase',    '/checkout/confirm',         59.50,  '2024-03-23 14:52:00', '2024-03'),
    (58, 'C18', 'purchase',    '/checkout/confirm',         299.00, '2024-03-24 15:59:00', '2024-03'),
    (59, 'C19', 'search',      '/search?q=running+shoes',  0.00,   '2024-03-26 16:06:00', '2024-03'),
    (60, 'C20', 'search',      '/search?q=winter+jackets', 0.00,   '2024-03-28 17:13:00', '2024-03');
