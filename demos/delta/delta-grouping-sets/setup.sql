-- ============================================================================
-- Manufacturing Production Reporting — Setup Script
-- ============================================================================
-- Creates a production_runs table with 3 lines x 3 shifts x 4 dates = 36 rows.
-- All values are deterministic for verifiable aggregations.
--
-- Production Lines: Line-A, Line-B, Line-C
-- Shifts: Morning, Afternoon, Night
-- Dates: 2025-03-01 through 2025-03-04
--
-- Value formulas (deterministic):
--   units_produced = base_per_line + shift_modifier + date_modifier
--   defect_count   = base_per_line + date_modifier + shift_modifier
--   runtime_hours  = base_per_line + date_modifier + shift_modifier
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables — manufacturing production reporting demo';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';
-- ============================================================================
-- TABLE: production_runs — 36 rows (3 lines x 3 shifts x 4 dates)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.production_runs (
    run_id           INT,
    production_line  VARCHAR,
    shift            VARCHAR,
    run_date         DATE,
    units_produced   INT,
    defect_count     INT,
    runtime_hours    DOUBLE,
    operator_name    VARCHAR
) LOCATION 'delta-grouping-sets/production_runs';


-- Line-A: 12 rows (3 shifts x 4 dates)
INSERT INTO {{zone_name}}.delta_demos.production_runs VALUES
    (1,  'Line-A', 'Morning',   '2025-03-01', 1050, 5,  7.7, 'Alice'),
    (2,  'Line-A', 'Morning',   '2025-03-02', 1070, 7,  8.0, 'Alice'),
    (3,  'Line-A', 'Morning',   '2025-03-03', 1040, 6,  7.5, 'Alice'),
    (4,  'Line-A', 'Morning',   '2025-03-04', 1080, 8,  8.2, 'Alice'),
    (5,  'Line-A', 'Afternoon', '2025-03-01', 1000, 6,  7.5, 'Bob'),
    (6,  'Line-A', 'Afternoon', '2025-03-02', 1020, 8,  7.8, 'Bob'),
    (7,  'Line-A', 'Afternoon', '2025-03-03', 990,  7,  7.3, 'Bob'),
    (8,  'Line-A', 'Afternoon', '2025-03-04', 1030, 9,  8.0, 'Bob'),
    (9,  'Line-A', 'Night',     '2025-03-01', 950,  7,  7.2, 'Charlie'),
    (10, 'Line-A', 'Night',     '2025-03-02', 970,  9,  7.5, 'Charlie'),
    (11, 'Line-A', 'Night',     '2025-03-03', 940,  8,  7.0, 'Charlie'),
    (12, 'Line-A', 'Night',     '2025-03-04', 980,  10, 7.7, 'Charlie');

-- Line-B: 12 rows (3 shifts x 4 dates)
INSERT INTO {{zone_name}}.delta_demos.production_runs VALUES
    (13, 'Line-B', 'Morning',   '2025-03-01', 1000, 7,  8.2, 'Diana'),
    (14, 'Line-B', 'Morning',   '2025-03-02', 1020, 9,  8.5, 'Diana'),
    (15, 'Line-B', 'Morning',   '2025-03-03', 990,  8,  8.0, 'Diana'),
    (16, 'Line-B', 'Morning',   '2025-03-04', 1030, 10, 8.7, 'Diana'),
    (17, 'Line-B', 'Afternoon', '2025-03-01', 950,  8,  8.0, 'Eric'),
    (18, 'Line-B', 'Afternoon', '2025-03-02', 970,  10, 8.3, 'Eric'),
    (19, 'Line-B', 'Afternoon', '2025-03-03', 940,  9,  7.8, 'Eric'),
    (20, 'Line-B', 'Afternoon', '2025-03-04', 980,  11, 8.5, 'Eric'),
    (21, 'Line-B', 'Night',     '2025-03-01', 900,  9,  7.7, 'Fiona'),
    (22, 'Line-B', 'Night',     '2025-03-02', 920,  11, 8.0, 'Fiona'),
    (23, 'Line-B', 'Night',     '2025-03-03', 890,  10, 7.5, 'Fiona'),
    (24, 'Line-B', 'Night',     '2025-03-04', 930,  12, 8.2, 'Fiona');

-- Line-C: 12 rows (3 shifts x 4 dates)
INSERT INTO {{zone_name}}.delta_demos.production_runs VALUES
    (25, 'Line-C', 'Morning',   '2025-03-01', 1100, 4,  8.0, 'George'),
    (26, 'Line-C', 'Morning',   '2025-03-02', 1120, 6,  8.3, 'George'),
    (27, 'Line-C', 'Morning',   '2025-03-03', 1090, 5,  7.8, 'George'),
    (28, 'Line-C', 'Morning',   '2025-03-04', 1130, 7,  8.5, 'George'),
    (29, 'Line-C', 'Afternoon', '2025-03-01', 1050, 5,  7.8, 'Hannah'),
    (30, 'Line-C', 'Afternoon', '2025-03-02', 1070, 7,  8.1, 'Hannah'),
    (31, 'Line-C', 'Afternoon', '2025-03-03', 1040, 6,  7.6, 'Hannah'),
    (32, 'Line-C', 'Afternoon', '2025-03-04', 1080, 8,  8.3, 'Hannah'),
    (33, 'Line-C', 'Night',     '2025-03-01', 1000, 6,  7.5, 'Ivan'),
    (34, 'Line-C', 'Night',     '2025-03-02', 1020, 8,  7.8, 'Ivan'),
    (35, 'Line-C', 'Night',     '2025-03-03', 990,  7,  7.3, 'Ivan'),
    (36, 'Line-C', 'Night',     '2025-03-04', 1030, 9,  8.0, 'Ivan');
