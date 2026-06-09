-- ============================================================================
-- Retail Sales Rep Leaderboard — Setup Script
-- ============================================================================
-- Creates a sales_reps table with 10 reps × 4 quarters = 40 rows.
-- Revenue values are deterministic with deliberate ties for DENSE_RANK testing.
--
-- Reps span 4 regions (North, South, East, West) with varied performance:
--   - Carol Wu (South) is the top performer across all quarters
--   - Hank Davis (West) is the lowest performer
--   - Alice Chen, David Kim, and James Ortiz are tied at $175K in Q4
--   - Bob Martinez and Irene Novak are tied at $130K in Q4
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';
-- ============================================================================
-- TABLE: sales_reps — 40 rows (10 reps × 4 quarters)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.sales_reps (
    rep_id        INT,
    rep_name      VARCHAR,
    region        VARCHAR,
    quarter       VARCHAR,
    revenue       DOUBLE,
    deals_closed  INT,
    quota         DOUBLE
) LOCATION 'delta-window-functions-advanced/sales_reps';

-- Rep 1: Alice Chen — North, strong performer
-- Rep 2: Bob Martinez — North, average
-- Rep 3: Carol Wu — South, top performer
-- Rep 4: David Kim — South, mid-tier
-- Rep 5: Eve Johnson — East, high performer
-- Rep 6: Frank Lee — East, below average
-- Rep 7: Grace Park — West, strong
-- Rep 8: Hank Davis — West, lowest
-- Rep 9: Irene Novak — North, mid
-- Rep 10: James Ortiz — South, decent
INSERT INTO {{zone_name}}.delta_demos.sales_reps VALUES
    (1,  'Alice Chen',     'North', 'Q1', 120000.00, 15, 100000.00),
    (1,  'Alice Chen',     'North', 'Q2', 135000.00, 18, 110000.00),
    (1,  'Alice Chen',     'North', 'Q3', 150000.00, 20, 120000.00),
    (1,  'Alice Chen',     'North', 'Q4', 175000.00, 22, 130000.00),
    (2,  'Bob Martinez',   'North', 'Q1',  95000.00, 12, 100000.00),
    (2,  'Bob Martinez',   'North', 'Q2', 105000.00, 14, 110000.00),
    (2,  'Bob Martinez',   'North', 'Q3', 110000.00, 15, 120000.00),
    (2,  'Bob Martinez',   'North', 'Q4', 130000.00, 16, 130000.00),
    (3,  'Carol Wu',       'South', 'Q1', 200000.00, 25, 150000.00),
    (3,  'Carol Wu',       'South', 'Q2', 220000.00, 28, 160000.00),
    (3,  'Carol Wu',       'South', 'Q3', 250000.00, 30, 170000.00),
    (3,  'Carol Wu',       'South', 'Q4', 310000.00, 35, 180000.00),
    (4,  'David Kim',      'South', 'Q1', 140000.00, 17, 150000.00),
    (4,  'David Kim',      'South', 'Q2', 155000.00, 19, 160000.00),
    (4,  'David Kim',      'South', 'Q3', 160000.00, 21, 170000.00),
    (4,  'David Kim',      'South', 'Q4', 175000.00, 23, 180000.00),
    (5,  'Eve Johnson',    'East',  'Q1', 180000.00, 22, 140000.00),
    (5,  'Eve Johnson',    'East',  'Q2', 195000.00, 24, 150000.00),
    (5,  'Eve Johnson',    'East',  'Q3', 210000.00, 26, 160000.00),
    (5,  'Eve Johnson',    'East',  'Q4', 245000.00, 29, 170000.00),
    (6,  'Frank Lee',      'East',  'Q1',  75000.00, 10, 140000.00),
    (6,  'Frank Lee',      'East',  'Q2',  80000.00, 11, 150000.00),
    (6,  'Frank Lee',      'East',  'Q3',  85000.00, 12, 160000.00),
    (6,  'Frank Lee',      'East',  'Q4',  95000.00, 13, 170000.00),
    (7,  'Grace Park',     'West',  'Q1', 160000.00, 20, 130000.00),
    (7,  'Grace Park',     'West',  'Q2', 175000.00, 22, 140000.00),
    (7,  'Grace Park',     'West',  'Q3', 190000.00, 24, 150000.00),
    (7,  'Grace Park',     'West',  'Q4', 225000.00, 27, 160000.00),
    (8,  'Hank Davis',     'West',  'Q1',  60000.00,  8, 130000.00),
    (8,  'Hank Davis',     'West',  'Q2',  65000.00,  9, 140000.00),
    (8,  'Hank Davis',     'West',  'Q3',  70000.00, 10, 150000.00),
    (8,  'Hank Davis',     'West',  'Q4',  80000.00, 11, 160000.00),
    (9,  'Irene Novak',    'North', 'Q1', 110000.00, 14, 100000.00),
    (9,  'Irene Novak',    'North', 'Q2', 125000.00, 16, 110000.00),
    (9,  'Irene Novak',    'North', 'Q3', 130000.00, 17, 120000.00),
    (9,  'Irene Novak',    'North', 'Q4', 130000.00, 18, 130000.00),
    (10, 'James Ortiz',    'South', 'Q1', 115000.00, 14, 150000.00),
    (10, 'James Ortiz',    'South', 'Q2', 130000.00, 16, 160000.00),
    (10, 'James Ortiz',    'South', 'Q3', 145000.00, 18, 170000.00),
    (10, 'James Ortiz',    'South', 'Q4', 175000.00, 20, 180000.00);

