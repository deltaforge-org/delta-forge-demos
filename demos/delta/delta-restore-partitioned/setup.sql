-- ============================================================================
-- Delta Recovery Strategy — RESTORE vs Partition Replace — Setup Script
-- ============================================================================
-- Creates the quarterly_revenue table partitioned by quarter for a retail
-- chain with 5 stores. Inserts 40 seed rows (10 per quarter, Q1–Q4).
-- All DML operations (V2–V5) are in queries.sql so users can step through
-- the recovery workflow.
--
-- Tables created:
--   1. quarterly_revenue — 40 revenue records partitioned by quarter (V1)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- V0: CREATE + V1: INSERT 40 quarterly revenue records
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.quarterly_revenue (
    id         INT,
    quarter    VARCHAR,
    store_id   VARCHAR,
    product    VARCHAR,
    units      INT,
    unit_price DOUBLE,
    tax_rate   DOUBLE,
    total      DOUBLE
) PARTITIONED BY (quarter)
  LOCATION 'delta-restore-partitioned/quarterly_revenue';


INSERT INTO {{zone_name}}.delta_demos.quarterly_revenue VALUES
    -- Q1: 10 rows, stores STR01-STR05, tax_rate = 0.08
    (1,  'Q1', 'STR01', 'Laptop',      5,  999.99, 0.08, 5399.95),
    (2,  'Q1', 'STR01', 'Monitor',    12,  349.99, 0.08, 4535.87),
    (3,  'Q1', 'STR02', 'Keyboard',   50,   79.99, 0.08, 4319.46),
    (4,  'Q1', 'STR02', 'Mouse',      45,   49.99, 0.08, 2429.51),
    (5,  'Q1', 'STR03', 'Headphones', 30,  149.99, 0.08, 4859.68),
    (6,  'Q1', 'STR03', 'Webcam',     20,   89.99, 0.08, 1943.78),
    (7,  'Q1', 'STR04', 'Tablet',     15,  449.99, 0.08, 7289.84),
    (8,  'Q1', 'STR04', 'Charger',    60,   29.99, 0.08, 1943.35),
    (9,  'Q1', 'STR05', 'USB Hub',    40,   39.99, 0.08, 1727.57),
    (10, 'Q1', 'STR05', 'Cable Pack', 80,   19.99, 0.08, 1727.14),
    -- Q2: 10 rows, stores STR01-STR05, tax_rate = 0.08
    (11, 'Q2', 'STR01', 'Laptop',      8,  999.99, 0.08, 8639.91),
    (12, 'Q2', 'STR01', 'Monitor',    15,  349.99, 0.08, 5669.84),
    (13, 'Q2', 'STR02', 'Keyboard',   55,   79.99, 0.08, 4751.41),
    (14, 'Q2', 'STR02', 'Mouse',      50,   49.99, 0.08, 2699.46),
    (15, 'Q2', 'STR03', 'Headphones', 35,  149.99, 0.08, 5669.62),
    (16, 'Q2', 'STR03', 'Webcam',     25,   89.99, 0.08, 2429.73),
    (17, 'Q2', 'STR04', 'Tablet',     18,  449.99, 0.08, 8747.81),
    (18, 'Q2', 'STR04', 'Charger',    70,   29.99, 0.08, 2267.24),
    (19, 'Q2', 'STR05', 'USB Hub',    45,   39.99, 0.08, 1943.51),
    (20, 'Q2', 'STR05', 'Cable Pack', 90,   19.99, 0.08, 1943.03),
    -- Q3: 10 rows, stores STR01-STR05, tax_rate = 0.08
    (21, 'Q3', 'STR01', 'Laptop',     10,  999.99, 0.08, 10799.89),
    (22, 'Q3', 'STR01', 'Monitor',    18,  349.99, 0.08, 6803.81),
    (23, 'Q3', 'STR02', 'Keyboard',   60,   79.99, 0.08, 5183.35),
    (24, 'Q3', 'STR02', 'Mouse',      55,   49.99, 0.08, 2969.41),
    (25, 'Q3', 'STR03', 'Headphones', 40,  149.99, 0.08, 6479.57),
    (26, 'Q3', 'STR03', 'Webcam',     28,   89.99, 0.08, 2721.30),
    (27, 'Q3', 'STR04', 'Tablet',     20,  449.99, 0.08, 9719.78),
    (28, 'Q3', 'STR04', 'Charger',    75,   29.99, 0.08, 2429.19),
    (29, 'Q3', 'STR05', 'USB Hub',    50,   39.99, 0.08, 2159.46),
    (30, 'Q3', 'STR05', 'Cable Pack', 100,  19.99, 0.08, 2158.92),
    -- Q4: 10 rows, stores STR01-STR05, tax_rate = 0.08
    (31, 'Q4', 'STR01', 'Laptop',     12,  999.99, 0.08, 12959.87),
    (32, 'Q4', 'STR01', 'Monitor',    20,  349.99, 0.08, 7559.78),
    (33, 'Q4', 'STR02', 'Keyboard',   65,   79.99, 0.08, 5615.30),
    (34, 'Q4', 'STR02', 'Mouse',      60,   49.99, 0.08, 3239.35),
    (35, 'Q4', 'STR03', 'Headphones', 45,  149.99, 0.08, 7289.51),
    (36, 'Q4', 'STR03', 'Webcam',     30,   89.99, 0.08, 2915.68),
    (37, 'Q4', 'STR04', 'Tablet',     22,  449.99, 0.08, 10691.76),
    (38, 'Q4', 'STR04', 'Charger',    80,   29.99, 0.08, 2591.14),
    (39, 'Q4', 'STR05', 'USB Hub',    55,   39.99, 0.08, 2375.41),
    (40, 'Q4', 'STR05', 'Cable Pack', 110,  19.99, 0.08, 2374.81);
