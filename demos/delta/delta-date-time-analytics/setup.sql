-- ============================================================================
-- SETUP: Employee Attendance Tracking — Date/Time Analytics
-- ============================================================================
-- An HR department tracks employee clock-in/clock-out times across two weeks.
-- 5 employees × 10 work days (Mon–Fri, 2024-03-04 to 2024-03-15) = 50 records.
-- Clock-in times range from 08:00 to 09:30; clock-out from 16:30 to 18:00.
-- Some arrivals are "late" (clock_in at 09:00 or after).
-- Mix of remote and onsite work.
--
-- Table created:
--   1. attendance_records — 50 rows of employee attendance data
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE with TIMESTAMP and DATE columns
--   3. INSERT 50 rows in 5 batches (10 per employee)
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables — employee attendance tracking demo';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';
-- ============================================================================
-- TABLE: attendance_records — employee clock-in/clock-out tracking
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.attendance_records (
    record_id       INT,
    employee_id     INT,
    employee_name   VARCHAR,
    department      VARCHAR,
    clock_in        TIMESTAMP,
    clock_out       TIMESTAMP,
    record_date     DATE,
    is_remote       BOOLEAN
) LOCATION 'delta-date-time-analytics/attendance_records';

-- ============================================================================
-- INSERT: Alice Chen — Engineering (record_ids 1-10)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.attendance_records VALUES
    (1,  1, 'Alice Chen', 'Engineering', '2024-03-04 08:15:00', '2024-03-04 17:15:00', '2024-03-04', false),
    (2,  1, 'Alice Chen', 'Engineering', '2024-03-05 08:00:00', '2024-03-05 17:00:00', '2024-03-05', false),
    (3,  1, 'Alice Chen', 'Engineering', '2024-03-06 09:10:00', '2024-03-06 17:40:00', '2024-03-06', false),
    (4,  1, 'Alice Chen', 'Engineering', '2024-03-07 08:30:00', '2024-03-07 16:45:00', '2024-03-07', true),
    (5,  1, 'Alice Chen', 'Engineering', '2024-03-08 08:45:00', '2024-03-08 17:00:00', '2024-03-08', false),
    (6,  1, 'Alice Chen', 'Engineering', '2024-03-11 08:00:00', '2024-03-11 17:15:00', '2024-03-11', true),
    (7,  1, 'Alice Chen', 'Engineering', '2024-03-12 09:00:00', '2024-03-12 17:30:00', '2024-03-12', false),
    (8,  1, 'Alice Chen', 'Engineering', '2024-03-13 08:20:00', '2024-03-13 16:50:00', '2024-03-13', false),
    (9,  1, 'Alice Chen', 'Engineering', '2024-03-14 08:10:00', '2024-03-14 17:40:00', '2024-03-14', true),
    (10, 1, 'Alice Chen', 'Engineering', '2024-03-15 08:45:00', '2024-03-15 17:00:00', '2024-03-15', false);
-- ============================================================================
-- INSERT: Bob Martinez — Sales (record_ids 11-20)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.attendance_records VALUES
    (11, 2, 'Bob Martinez', 'Sales', '2024-03-04 08:30:00', '2024-03-04 17:00:00', '2024-03-04', false),
    (12, 2, 'Bob Martinez', 'Sales', '2024-03-05 09:15:00', '2024-03-05 17:45:00', '2024-03-05', false),
    (13, 2, 'Bob Martinez', 'Sales', '2024-03-06 08:00:00', '2024-03-06 16:30:00', '2024-03-06', true),
    (14, 2, 'Bob Martinez', 'Sales', '2024-03-07 08:45:00', '2024-03-07 17:15:00', '2024-03-07', false),
    (15, 2, 'Bob Martinez', 'Sales', '2024-03-08 09:00:00', '2024-03-08 17:00:00', '2024-03-08', true),
    (16, 2, 'Bob Martinez', 'Sales', '2024-03-11 08:10:00', '2024-03-11 17:10:00', '2024-03-11', false),
    (17, 2, 'Bob Martinez', 'Sales', '2024-03-12 08:30:00', '2024-03-12 16:45:00', '2024-03-12', true),
    (18, 2, 'Bob Martinez', 'Sales', '2024-03-13 09:20:00', '2024-03-13 17:50:00', '2024-03-13', false),
    (19, 2, 'Bob Martinez', 'Sales', '2024-03-14 08:00:00', '2024-03-14 17:00:00', '2024-03-14', false),
    (20, 2, 'Bob Martinez', 'Sales', '2024-03-15 08:15:00', '2024-03-15 16:30:00', '2024-03-15', false);
-- ============================================================================
-- INSERT: Carol Johnson — Marketing (record_ids 21-30)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.attendance_records VALUES
    (21, 3, 'Carol Johnson', 'Marketing', '2024-03-04 08:00:00', '2024-03-04 16:30:00', '2024-03-04', true),
    (22, 3, 'Carol Johnson', 'Marketing', '2024-03-05 08:30:00', '2024-03-05 17:00:00', '2024-03-05', false),
    (23, 3, 'Carol Johnson', 'Marketing', '2024-03-06 08:15:00', '2024-03-06 16:45:00', '2024-03-06', false),
    (24, 3, 'Carol Johnson', 'Marketing', '2024-03-07 09:05:00', '2024-03-07 17:20:00', '2024-03-07', true),
    (25, 3, 'Carol Johnson', 'Marketing', '2024-03-08 08:00:00', '2024-03-08 16:30:00', '2024-03-08', false),
    (26, 3, 'Carol Johnson', 'Marketing', '2024-03-11 08:45:00', '2024-03-11 17:15:00', '2024-03-11', false),
    (27, 3, 'Carol Johnson', 'Marketing', '2024-03-12 08:10:00', '2024-03-12 16:40:00', '2024-03-12', true),
    (28, 3, 'Carol Johnson', 'Marketing', '2024-03-13 09:00:00', '2024-03-13 17:15:00', '2024-03-13', false),
    (29, 3, 'Carol Johnson', 'Marketing', '2024-03-14 08:20:00', '2024-03-14 16:50:00', '2024-03-14', false),
    (30, 3, 'Carol Johnson', 'Marketing', '2024-03-15 08:30:00', '2024-03-15 17:00:00', '2024-03-15', true);
-- ============================================================================
-- INSERT: David Kim — Support (record_ids 31-40)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.attendance_records VALUES
    (31, 4, 'David Kim', 'Support', '2024-03-04 08:00:00', '2024-03-04 17:00:00', '2024-03-04', false),
    (32, 4, 'David Kim', 'Support', '2024-03-05 08:15:00', '2024-03-05 16:45:00', '2024-03-05', false),
    (33, 4, 'David Kim', 'Support', '2024-03-06 09:30:00', '2024-03-06 17:30:00', '2024-03-06', true),
    (34, 4, 'David Kim', 'Support', '2024-03-07 08:00:00', '2024-03-07 17:00:00', '2024-03-07', false),
    (35, 4, 'David Kim', 'Support', '2024-03-08 08:30:00', '2024-03-08 16:30:00', '2024-03-08', false),
    (36, 4, 'David Kim', 'Support', '2024-03-11 09:00:00', '2024-03-11 17:00:00', '2024-03-11', false),
    (37, 4, 'David Kim', 'Support', '2024-03-12 08:00:00', '2024-03-12 17:30:00', '2024-03-12', true),
    (38, 4, 'David Kim', 'Support', '2024-03-13 08:45:00', '2024-03-13 17:15:00', '2024-03-13', false),
    (39, 4, 'David Kim', 'Support', '2024-03-14 08:15:00', '2024-03-14 16:45:00', '2024-03-14', false),
    (40, 4, 'David Kim', 'Support', '2024-03-15 09:15:00', '2024-03-15 17:15:00', '2024-03-15', true);
-- ============================================================================
-- INSERT: Eva Schmidt — Engineering (record_ids 41-50)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.attendance_records VALUES
    (41, 5, 'Eva Schmidt', 'Engineering', '2024-03-04 08:00:00', '2024-03-04 17:30:00', '2024-03-04', false),
    (42, 5, 'Eva Schmidt', 'Engineering', '2024-03-05 08:20:00', '2024-03-05 17:20:00', '2024-03-05', false),
    (43, 5, 'Eva Schmidt', 'Engineering', '2024-03-06 08:10:00', '2024-03-06 17:10:00', '2024-03-06', true),
    (44, 5, 'Eva Schmidt', 'Engineering', '2024-03-07 09:00:00', '2024-03-07 17:30:00', '2024-03-07', false),
    (45, 5, 'Eva Schmidt', 'Engineering', '2024-03-08 08:30:00', '2024-03-08 17:00:00', '2024-03-08', false),
    (46, 5, 'Eva Schmidt', 'Engineering', '2024-03-11 08:15:00', '2024-03-11 17:45:00', '2024-03-11', true),
    (47, 5, 'Eva Schmidt', 'Engineering', '2024-03-12 08:00:00', '2024-03-12 17:00:00', '2024-03-12', false),
    (48, 5, 'Eva Schmidt', 'Engineering', '2024-03-13 09:10:00', '2024-03-13 17:40:00', '2024-03-13', false),
    (49, 5, 'Eva Schmidt', 'Engineering', '2024-03-14 08:30:00', '2024-03-14 17:30:00', '2024-03-14', true),
    (50, 5, 'Eva Schmidt', 'Engineering', '2024-03-15 08:00:00', '2024-03-15 16:30:00', '2024-03-15', false);
