-- ============================================================================
-- Delta Convert to Delta — Migrating from Raw Parquet — Setup Script
-- ============================================================================
-- Creates the Delta table and inserts baseline data:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE (the "converted" table)
--   3. INSERT 40 rows — "migrated" legacy data (migrated_flag=1)
--   4. INSERT 10 rows — new data added post-migration (migrated_flag=0)
--
-- The queries.sql script then performs Delta-exclusive DML operations
-- (UPDATE, DELETE, OPTIMIZE) to demonstrate capabilities that raw
-- Parquet files cannot provide.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: legacy_data — migrated reporting data
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.legacy_data (
    id                INT,
    customer_name     VARCHAR,
    order_total       DOUBLE,
    product_category  VARCHAR,
    payment_method    VARCHAR,
    order_status      VARCHAR,
    created_date      VARCHAR,
    migrated_flag     INT
) LOCATION 'delta-convert-to-delta/legacy_data';


-- ============================================================================
-- STEP 2: INSERT 40 rows — migrated legacy data (migrated_flag=1)
-- ============================================================================
-- These represent records that existed as flat Parquet files and are now
-- being loaded into a Delta table with transaction log support.
-- Some payment_method values use legacy codes: 'cc' and 'pp'
INSERT INTO {{zone_name}}.delta_demos.legacy_data VALUES
    (1,  'Alice Johnson',    125.50, 'Electronics', 'cc',            'completed', '2024-01-15', 1),
    (2,  'Bob Smith',         89.99, 'Clothing',    'pp',            'completed', '2024-01-16', 1),
    (3,  'Charlie Brown',     45.00, 'Grocery',     'cash',          'completed', '2024-01-17', 1),
    (4,  'Diana Prince',     299.99, 'Electronics', 'cc',            'shipped',   '2024-01-18', 1),
    (5,  'Eve Martinez',      67.50, 'Home',        'bank_transfer', 'completed', '2024-01-19', 1),
    (6,  'Frank Castle',     150.00, 'Sports',      'cc',            'completed', '2024-01-20', 1),
    (7,  'Grace Lee',         34.99, 'Clothing',    'pp',            'returned',  '2024-01-21', 1),
    (8,  'Hank Pym',         210.00, 'Electronics', 'bank_transfer', 'completed', '2024-01-22', 1),
    (9,  'Iris West',         55.25, 'Grocery',     'cash',          'completed', '2024-01-23', 1),
    (10, 'Jack Ryan',        175.00, 'Home',        'cc',            'shipped',   '2024-01-24', 1),
    (11, 'Karen Page',        92.50, 'Clothing',    'pp',            'completed', '2024-01-25', 1),
    (12, 'Leo Messi',        320.00, 'Sports',      'cc',            'completed', '2024-01-26', 1),
    (13, 'Mia Wong',          78.99, 'Electronics', 'pp',            'completed', '2024-01-27', 1),
    (14, 'Nathan Drake',      42.00, 'Grocery',     'cash',          'completed', '2024-01-28', 1),
    (15, 'Olivia Pope',      115.75, 'Home',        'cc',            'completed', '2024-01-29', 1),
    (16, 'Peter Parker',     189.99, 'Electronics', 'pp',            'shipped',   '2024-01-30', 1),
    (17, 'Quinn Hughes',      63.00, 'Clothing',    'bank_transfer', 'completed', '2024-01-31', 1),
    (18, 'Rachel Green',      28.50, 'Grocery',     'cash',          'completed', '2024-02-01', 1),
    (19, 'Sam Wilson',       245.00, 'Sports',      'cc',            'completed', '2024-02-02', 1),
    (20, 'Tina Fey',          97.25, 'Home',        'pp',            'completed', '2024-02-03', 1),
    (21, 'Uma Thurman',      155.50, 'Electronics', 'cc',            'completed', '2024-02-04', 1),
    (22, 'Victor Stone',      71.00, 'Clothing',    'pp',            'returned',  '2024-02-05', 1),
    (23, 'Wanda Vision',      88.75, 'Grocery',     'bank_transfer', 'completed', '2024-02-06', 1),
    (24, 'Xavier Charles',   410.00, 'Electronics', 'cc',            'completed', '2024-02-07', 1),
    (25, 'Yara Shahidi',      53.99, 'Home',        'cash',          'shipped',   '2024-02-08', 1),
    (26, 'Zoe Saldana',      132.00, 'Sports',      'pp',            'completed', '2024-02-09', 1),
    (27, 'Aaron Judge',       76.50, 'Clothing',    'cc',            'completed', '2024-02-10', 1),
    (28, 'Bella Swan',        39.99, 'Grocery',     'cash',          'completed', '2024-02-11', 1),
    (29, 'Clark Kent',       285.00, 'Electronics', 'bank_transfer', 'completed', '2024-02-12', 1),
    (30, 'Donna Troy',        95.00, 'Home',        'pp',            'completed', '2024-02-13', 1),
    (31, 'Ethan Hunt',       160.00, 'Sports',      'cc',            'shipped',   '2024-02-14', 1),
    (32, 'Fiona Apple',       48.25, 'Clothing',    'bank_transfer', 'completed', '2024-02-15', 1),
    (33, 'George Lucas',     199.99, 'Electronics', 'cc',            'completed', '2024-02-16', 1),
    (34, 'Holly Berry',       62.00, 'Grocery',     'cash',          'completed', '2024-02-17', 1),
    (35, 'Ivan Drago',       225.00, 'Sports',      'pp',            'completed', '2024-02-18', 1),
    (36, 'Jane Foster',      108.50, 'Home',        'cc',            'completed', '2024-02-19', 1),
    (37, 'Kyle Reese',        84.99, 'Clothing',    'pp',            'shipped',   '2024-02-20', 1),
    (38, 'Luna Lovegood',     33.00, 'Grocery',     'bank_transfer', 'completed', '2024-02-21', 1),
    (39, 'Max Power',        178.50, 'Electronics', 'cc',            'completed', '2024-02-22', 1),
    (40, 'Nora Allen',        57.75, 'Home',        'cash',          'completed', '2024-02-23', 1);


-- ============================================================================
-- STEP 3: INSERT 10 rows — new data added post-migration (migrated_flag=0)
-- ============================================================================
-- These records were created after the Delta table was established,
-- benefiting from ACID transactions and schema enforcement.
INSERT INTO {{zone_name}}.delta_demos.legacy_data VALUES
    (41, 'Oscar Wilde',      142.00, 'Electronics', 'cc',            'completed', '2024-03-01', 0),
    (42, 'Penny Lane',        66.50, 'Clothing',    'pp',            'completed', '2024-03-02', 0),
    (43, 'Reed Richards',    315.00, 'Electronics', 'bank_transfer', 'shipped',   '2024-03-03', 0),
    (44, 'Sara Connor',       49.99, 'Grocery',     'cash',          'completed', '2024-03-04', 0),
    (45, 'Tony Stark',       475.00, 'Electronics', 'cc',            'completed', '2024-03-05', 0),
    (46, 'Ursula Major',      88.00, 'Home',        'pp',            'completed', '2024-03-06', 0),
    (47, 'Vince Carter',     195.00, 'Sports',      'cc',            'shipped',   '2024-03-07', 0),
    (48, 'Wendy Darling',     73.25, 'Clothing',    'bank_transfer', 'completed', '2024-03-08', 0),
    (49, 'Xena Warrior',    112.50, 'Sports',       'pp',            'completed', '2024-03-09', 0),
    (50, 'Yuri Gagarin',      37.99, 'Grocery',     'cash',          'completed', '2024-03-10', 0);

