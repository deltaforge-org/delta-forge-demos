-- ============================================================================
-- Delta OPTIMIZE — Manual File Compaction & TARGET SIZE — Setup
-- ============================================================================
-- An e-commerce platform ingests orders in daily batches. Each batch creates
-- a small Parquet file. Over 8 days, the table accumulates 8+ small files
-- that degrade read performance. Two UPDATE operations (cancellations and
-- refunds) create additional file fragmentation via copy-on-write.
--
-- The queries.sql script demonstrates OPTIMIZE to compact these files and
-- verify data integrity before and after compaction.
--
-- Tables created:
--   1. daily_orders — 80 orders across 8 daily batches
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: daily_orders — e-commerce order history
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.daily_orders (
    order_id    INT,
    customer_id VARCHAR,
    product     VARCHAR,
    quantity    INT,
    unit_price  DOUBLE,
    status      VARCHAR,
    region      VARCHAR,
    order_date  VARCHAR
) LOCATION 'delta-optimize-compaction/daily_orders';


-- ============================================================================
-- DAY 1: Monday orders (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.daily_orders VALUES
    (1,  'C-101', 'Laptop',        1, 999.99,  'completed', 'North', '2025-03-10'),
    (2,  'C-102', 'Mouse',         2, 29.99,   'completed', 'South', '2025-03-10'),
    (3,  'C-103', 'Keyboard',      1, 79.99,   'completed', 'East',  '2025-03-10'),
    (4,  'C-104', 'Monitor',       1, 449.99,  'completed', 'West',  '2025-03-10'),
    (5,  'C-105', 'Headphones',    3, 59.99,   'completed', 'North', '2025-03-10'),
    (6,  'C-106', 'Webcam',        1, 89.99,   'completed', 'South', '2025-03-10'),
    (7,  'C-107', 'USB Hub',       2, 34.99,   'completed', 'East',  '2025-03-10'),
    (8,  'C-108', 'Desk Lamp',     1, 44.99,   'completed', 'West',  '2025-03-10'),
    (9,  'C-109', 'Mouse Pad',     4, 12.99,   'completed', 'North', '2025-03-10'),
    (10, 'C-110', 'Cable Kit',     1, 24.99,   'completed', 'South', '2025-03-10');


-- ============================================================================
-- DAY 2: Tuesday orders (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.daily_orders VALUES
    (11, 'C-111', 'Laptop',        1, 1299.99, 'completed', 'East',  '2025-03-11'),
    (12, 'C-112', 'Tablet',        1, 499.99,  'completed', 'West',  '2025-03-11'),
    (13, 'C-113', 'Phone Case',    2, 19.99,   'completed', 'North', '2025-03-11'),
    (14, 'C-114', 'Charger',       3, 29.99,   'completed', 'South', '2025-03-11'),
    (15, 'C-115', 'Speakers',      1, 149.99,  'completed', 'East',  '2025-03-11'),
    (16, 'C-116', 'Keyboard',      1, 129.99,  'completed', 'West',  '2025-03-11'),
    (17, 'C-117', 'Monitor',       2, 349.99,  'completed', 'North', '2025-03-11'),
    (18, 'C-118', 'Desk Chair',    1, 289.99,  'completed', 'South', '2025-03-11'),
    (19, 'C-119', 'Webcam',        1, 69.99,   'completed', 'East',  '2025-03-11'),
    (20, 'C-120', 'Mouse',         1, 49.99,   'completed', 'West',  '2025-03-11');


-- ============================================================================
-- DAY 3: Wednesday orders (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.daily_orders VALUES
    (21, 'C-121', 'Laptop',        1, 899.99,  'completed', 'South', '2025-03-12'),
    (22, 'C-122', 'Headphones',    1, 199.99,  'completed', 'North', '2025-03-12'),
    (23, 'C-123', 'USB Hub',       3, 24.99,   'completed', 'East',  '2025-03-12'),
    (24, 'C-124', 'Cable Kit',     2, 34.99,   'completed', 'West',  '2025-03-12'),
    (25, 'C-125', 'Desk Lamp',     1, 64.99,   'completed', 'South', '2025-03-12'),
    (26, 'C-126', 'Monitor',       1, 549.99,  'completed', 'North', '2025-03-12'),
    (27, 'C-127', 'Tablet',        1, 399.99,  'completed', 'East',  '2025-03-12'),
    (28, 'C-128', 'Speakers',      2, 79.99,   'completed', 'West',  '2025-03-12'),
    (29, 'C-129', 'Phone Case',    5, 14.99,   'completed', 'South', '2025-03-12'),
    (30, 'C-130', 'Charger',       1, 39.99,   'completed', 'North', '2025-03-12');


-- ============================================================================
-- DAY 4: Thursday orders (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.daily_orders VALUES
    (31, 'C-131', 'Laptop',        1, 1099.99, 'completed', 'West',  '2025-03-13'),
    (32, 'C-132', 'Mouse',         2, 39.99,   'completed', 'East',  '2025-03-13'),
    (33, 'C-133', 'Keyboard',      1, 159.99,  'completed', 'South', '2025-03-13'),
    (34, 'C-134', 'Webcam',        1, 109.99,  'completed', 'North', '2025-03-13'),
    (35, 'C-135', 'Headphones',    1, 249.99,  'completed', 'West',  '2025-03-13'),
    (36, 'C-136', 'Desk Chair',    1, 349.99,  'completed', 'East',  '2025-03-13'),
    (37, 'C-137', 'USB Hub',       1, 44.99,   'completed', 'South', '2025-03-13'),
    (38, 'C-138', 'Monitor',       1, 699.99,  'completed', 'North', '2025-03-13'),
    (39, 'C-139', 'Tablet',        2, 299.99,  'completed', 'West',  '2025-03-13'),
    (40, 'C-140', 'Cable Kit',     3, 19.99,   'completed', 'East',  '2025-03-13');


-- ============================================================================
-- DAY 5: Friday orders (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.daily_orders VALUES
    (41, 'C-141', 'Laptop',        1, 1499.99, 'completed', 'North', '2025-03-14'),
    (42, 'C-142', 'Desk Lamp',     2, 54.99,   'completed', 'South', '2025-03-14'),
    (43, 'C-143', 'Mouse',         1, 69.99,   'completed', 'East',  '2025-03-14'),
    (44, 'C-144', 'Charger',       2, 24.99,   'completed', 'West',  '2025-03-14'),
    (45, 'C-145', 'Speakers',      1, 199.99,  'completed', 'North', '2025-03-14'),
    (46, 'C-146', 'Phone Case',    3, 24.99,   'completed', 'South', '2025-03-14'),
    (47, 'C-147', 'Keyboard',      1, 89.99,   'completed', 'East',  '2025-03-14'),
    (48, 'C-148', 'Webcam',        1, 119.99,  'completed', 'West',  '2025-03-14'),
    (49, 'C-149', 'Headphones',    2, 79.99,   'completed', 'North', '2025-03-14'),
    (50, 'C-150', 'USB Hub',       1, 29.99,   'completed', 'South', '2025-03-14');


-- ============================================================================
-- DAY 6: Saturday orders (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.daily_orders VALUES
    (51, 'C-151', 'Laptop',        1, 1199.99, 'completed', 'East',  '2025-03-15'),
    (52, 'C-152', 'Monitor',       1, 399.99,  'completed', 'West',  '2025-03-15'),
    (53, 'C-153', 'Tablet',        1, 599.99,  'completed', 'North', '2025-03-15'),
    (54, 'C-154', 'Mouse',         2, 44.99,   'completed', 'South', '2025-03-15'),
    (55, 'C-155', 'Desk Chair',    1, 279.99,  'completed', 'East',  '2025-03-15'),
    (56, 'C-156', 'Cable Kit',     4, 14.99,   'completed', 'West',  '2025-03-15'),
    (57, 'C-157', 'Charger',       1, 34.99,   'completed', 'North', '2025-03-15'),
    (58, 'C-158', 'Desk Lamp',     1, 74.99,   'completed', 'South', '2025-03-15'),
    (59, 'C-159', 'Headphones',    1, 129.99,  'completed', 'East',  '2025-03-15'),
    (60, 'C-160', 'Speakers',      1, 99.99,   'completed', 'West',  '2025-03-15');


-- ============================================================================
-- DAY 7: Sunday orders (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.daily_orders VALUES
    (61, 'C-161', 'Laptop',        1, 849.99,  'completed', 'South', '2025-03-16'),
    (62, 'C-162', 'Keyboard',      2, 64.99,   'completed', 'North', '2025-03-16'),
    (63, 'C-163', 'USB Hub',       1, 39.99,   'completed', 'East',  '2025-03-16'),
    (64, 'C-164', 'Phone Case',    2, 29.99,   'completed', 'West',  '2025-03-16'),
    (65, 'C-165', 'Webcam',        1, 99.99,   'completed', 'South', '2025-03-16'),
    (66, 'C-166', 'Mouse',         1, 59.99,   'completed', 'North', '2025-03-16'),
    (67, 'C-167', 'Monitor',       1, 479.99,  'completed', 'East',  '2025-03-16'),
    (68, 'C-168', 'Tablet',        1, 449.99,  'completed', 'West',  '2025-03-16'),
    (69, 'C-169', 'Desk Lamp',     2, 49.99,   'completed', 'South', '2025-03-16'),
    (70, 'C-170', 'Charger',       1, 44.99,   'completed', 'North', '2025-03-16');


-- ============================================================================
-- DAY 8: Monday orders — new week (10 rows)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.daily_orders VALUES
    (71, 'C-171', 'Laptop',        1, 1349.99, 'completed', 'West',  '2025-03-17'),
    (72, 'C-172', 'Headphones',    1, 169.99,  'completed', 'East',  '2025-03-17'),
    (73, 'C-173', 'Speakers',      2, 119.99,  'completed', 'South', '2025-03-17'),
    (74, 'C-174', 'Desk Chair',    1, 319.99,  'completed', 'North', '2025-03-17'),
    (75, 'C-175', 'Mouse',         3, 34.99,   'completed', 'West',  '2025-03-17'),
    (76, 'C-176', 'Cable Kit',     2, 29.99,   'completed', 'East',  '2025-03-17'),
    (77, 'C-177', 'Keyboard',      1, 109.99,  'completed', 'South', '2025-03-17'),
    (78, 'C-178', 'Monitor',       1, 599.99,  'completed', 'North', '2025-03-17'),
    (79, 'C-179', 'Webcam',        2, 79.99,   'completed', 'West',  '2025-03-17'),
    (80, 'C-180', 'Tablet',        1, 349.99,  'completed', 'East',  '2025-03-17');


-- ============================================================================
-- STEP 3: UPDATE — 5 cancellations (creates additional file fragmentation)
-- ============================================================================
-- Orders 8, 23, 37, 56, 64 cancelled. Each UPDATE rewrites affected files
-- via copy-on-write, creating more orphaned files.
UPDATE {{zone_name}}.delta_demos.daily_orders
SET status = 'cancelled'
WHERE order_id IN (8, 23, 37, 56, 64);


-- ============================================================================
-- STEP 4: UPDATE — 3 partial refunds
-- ============================================================================
-- Orders 15, 39, 72 refunded. More file fragmentation.
UPDATE {{zone_name}}.delta_demos.daily_orders
SET status = 'refunded'
WHERE order_id IN (15, 39, 72);
