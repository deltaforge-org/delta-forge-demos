-- ============================================================================
-- RESTORE + Time Travel — Inspect Before You Recover — Setup Script
-- ============================================================================
-- Creates the course_grades table with 25 grades (5 students × 5 courses).
-- All version operations (V2–V4) are in queries.sql so users can step
-- through the inspection and recovery workflow interactively.
--
-- Tables created:
--   1. course_grades — 25 student grade records (V1 baseline)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- V0: CREATE + V1: INSERT 25 student grades
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.course_grades (
    record_id    INT,
    student_name VARCHAR,
    course       VARCHAR,
    grade        INT,
    semester     VARCHAR
) LOCATION 'delta-restore-time-travel/course_grades';


INSERT INTO {{zone_name}}.delta_demos.course_grades VALUES
    (1,  'Alice', 'CS101',    82, 'Fall-2025'),
    (2,  'Bob',   'CS101',    71, 'Fall-2025'),
    (3,  'Carol', 'CS101',    90, 'Fall-2025'),
    (4,  'David', 'CS101',    65, 'Fall-2025'),
    (5,  'Eve',   'CS101',    88, 'Fall-2025'),
    (6,  'Alice', 'CS201',    78, 'Fall-2025'),
    (7,  'Bob',   'CS201',    84, 'Fall-2025'),
    (8,  'Carol', 'CS201',    73, 'Fall-2025'),
    (9,  'David', 'CS201',    91, 'Fall-2025'),
    (10, 'Eve',   'CS201',    69, 'Fall-2025'),
    (11, 'Alice', 'MATH101',  95, 'Fall-2025'),
    (12, 'Bob',   'MATH101',  62, 'Fall-2025'),
    (13, 'Carol', 'MATH101',  88, 'Fall-2025'),
    (14, 'David', 'MATH101',  74, 'Fall-2025'),
    (15, 'Eve',   'MATH101',  81, 'Fall-2025'),
    (16, 'Alice', 'PHYS101',  70, 'Fall-2025'),
    (17, 'Bob',   'PHYS101',  86, 'Fall-2025'),
    (18, 'Carol', 'PHYS101',  77, 'Fall-2025'),
    (19, 'David', 'PHYS101',  83, 'Fall-2025'),
    (20, 'Eve',   'PHYS101',  92, 'Fall-2025'),
    (21, 'Alice', 'ENG101',   88, 'Fall-2025'),
    (22, 'Bob',   'ENG101',   79, 'Fall-2025'),
    (23, 'Carol', 'ENG101',   94, 'Fall-2025'),
    (24, 'David', 'ENG101',   68, 'Fall-2025'),
    (25, 'Eve',   'ENG101',   85, 'Fall-2025');
