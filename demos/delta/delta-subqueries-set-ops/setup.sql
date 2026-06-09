-- ============================================================================
-- University Course Enrollment Analytics — Setup Script
-- ============================================================================
-- Creates two tables for a university enrollment system:
--   1. students    — 15 students across 5 majors (CS, Math, Physics, Bio)
--   2. enrollments — 60 course enrollments (4 per student across 2 semesters)
--
-- Data characteristics:
--   - 13 students enrolled in both Fall2024 and Spring2025
--   - 2 students Fall-only (Frank Lopez, Leo Thompson)
--   - 2 students Spring-only (Henry Brown, Nathan Clark)
--   - 8 students enrolled in multiple departments
--   - Fall2024 courses have letter grades; Spring2025 grades are NULL (current)
--   - GPAs range from 2.15 to 3.95
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables — university enrollment demo';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';
-- ============================================================================
-- TABLE 1: students — 15 rows
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.students (
    student_id      INT,
    student_name    VARCHAR,
    major           VARCHAR,
    gpa             DOUBLE,
    enrollment_year INT
) LOCATION 'delta-subqueries-set-ops/students';


INSERT INTO {{zone_name}}.delta_demos.students VALUES
    (1,  'Alice Chen',       'Computer Science', 3.85, 2022),
    (2,  'Bob Martinez',     'Mathematics',      3.42, 2022),
    (3,  'Carol Johnson',    'Computer Science', 3.91, 2023),
    (4,  'David Kim',        'Physics',          2.78, 2021),
    (5,  'Eva Patel',        'Mathematics',      3.65, 2023),
    (6,  'Frank Lopez',      'Biology',          2.15, 2021),
    (7,  'Grace Wang',       'Computer Science', 3.72, 2022),
    (8,  'Henry Brown',      'Physics',          3.10, 2023),
    (9,  'Irene Davis',      'Biology',          3.48, 2022),
    (10, 'Jack Wilson',      'Mathematics',      2.95, 2021),
    (11, 'Karen Lee',        'Computer Science', 3.58, 2023),
    (12, 'Leo Thompson',     'Physics',          2.33, 2022),
    (13, 'Maria Garcia',     'Biology',          3.77, 2021),
    (14, 'Nathan Clark',     'Mathematics',      3.20, 2023),
    (15, 'Olivia Scott',     'Computer Science', 3.95, 2022);
-- ============================================================================
-- TABLE 2: enrollments — 60 rows (4 per student across 2 semesters)
-- ============================================================================
-- Fall2024 courses have letter grades (completed).
-- Spring2025 courses have NULL grades (in progress).
-- Some students span multiple departments to enable cross-department queries.
-- Frank Lopez (6) and Leo Thompson (12) are Fall-only (4 Fall courses).
-- Henry Brown (8) and Nathan Clark (14) are Spring-only (4 Spring courses).
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.enrollments (
    enrollment_id   INT,
    student_id      INT,
    course_code     VARCHAR,
    department      VARCHAR,
    semester        VARCHAR,
    grade           VARCHAR,
    credits         INT
) LOCATION 'delta-subqueries-set-ops/enrollments';


-- Alice Chen (1) — CS major, takes CS + Math courses
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (1,  1,  'CS101',   'Computer Science', 'Fall2024',   'A',   3),
    (2,  1,  'CS201',   'Computer Science', 'Fall2024',   'A-',  3),
    (3,  1,  'MATH301', 'Mathematics',      'Spring2025', NULL,  4),
    (4,  1,  'CS301',   'Computer Science', 'Spring2025', NULL,  3);

-- Bob Martinez (2) — Math only
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (5,  2,  'MATH101', 'Mathematics',      'Fall2024',   'B+',  3),
    (6,  2,  'MATH201', 'Mathematics',      'Fall2024',   'B',   3),
    (7,  2,  'MATH301', 'Mathematics',      'Spring2025', NULL,  4),
    (8,  2,  'MATH401', 'Mathematics',      'Spring2025', NULL,  3);

-- Carol Johnson (3) — CS + Physics
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (9,  3,  'CS101',   'Computer Science', 'Fall2024',   'A',   3),
    (10, 3,  'PHYS101', 'Physics',          'Fall2024',   'A-',  4),
    (11, 3,  'CS201',   'Computer Science', 'Spring2025', NULL,  3),
    (12, 3,  'PHYS201', 'Physics',          'Spring2025', NULL,  4);

-- David Kim (4) — Physics + Math
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (13, 4,  'PHYS101', 'Physics',          'Fall2024',   'C+',  4),
    (14, 4,  'MATH101', 'Mathematics',      'Fall2024',   'C',   3),
    (15, 4,  'PHYS201', 'Physics',          'Spring2025', NULL,  4),
    (16, 4,  'MATH201', 'Mathematics',      'Spring2025', NULL,  3);

-- Eva Patel (5) — Math + CS
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (17, 5,  'MATH201', 'Mathematics',      'Fall2024',   'A-',  3),
    (18, 5,  'CS101',   'Computer Science', 'Fall2024',   'B+',  3),
    (19, 5,  'MATH301', 'Mathematics',      'Spring2025', NULL,  4),
    (20, 5,  'CS201',   'Computer Science', 'Spring2025', NULL,  3);

-- Frank Lopez (6) — Biology only, FALL-ONLY (4 Fall courses)
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (21, 6,  'BIO101',  'Biology',          'Fall2024',   'D',   4),
    (22, 6,  'BIO201',  'Biology',          'Fall2024',   'D+',  3),
    (23, 6,  'BIO301',  'Biology',          'Fall2024',   'C',   4),
    (24, 6,  'BIO401',  'Biology',          'Fall2024',   'C+',  3);

-- Grace Wang (7) — CS + Biology
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (25, 7,  'CS201',   'Computer Science', 'Fall2024',   'A-',  3),
    (26, 7,  'BIO101',  'Biology',          'Fall2024',   'B',   4),
    (27, 7,  'CS301',   'Computer Science', 'Spring2025', NULL,  3),
    (28, 7,  'BIO201',  'Biology',          'Spring2025', NULL,  3);

-- Henry Brown (8) — Physics only, SPRING-ONLY (4 Spring courses)
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (29, 8,  'PHYS101', 'Physics',          'Spring2025', NULL,  4),
    (30, 8,  'PHYS201', 'Physics',          'Spring2025', NULL,  4),
    (31, 8,  'PHYS301', 'Physics',          'Spring2025', NULL,  3),
    (32, 8,  'PHYS401', 'Physics',          'Spring2025', NULL,  3);

-- Irene Davis (9) — Biology only
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (33, 9,  'BIO101',  'Biology',          'Fall2024',   'B+',  4),
    (34, 9,  'BIO201',  'Biology',          'Fall2024',   'A-',  3),
    (35, 9,  'BIO301',  'Biology',          'Spring2025', NULL,  4),
    (36, 9,  'BIO401',  'Biology',          'Spring2025', NULL,  3);

-- Jack Wilson (10) — Math + Physics
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (37, 10, 'MATH101', 'Mathematics',      'Fall2024',   'C+',  3),
    (38, 10, 'PHYS101', 'Physics',          'Fall2024',   'C',   4),
    (39, 10, 'MATH201', 'Mathematics',      'Spring2025', NULL,  3),
    (40, 10, 'PHYS201', 'Physics',          'Spring2025', NULL,  4);

-- Karen Lee (11) — CS + Math
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (41, 11, 'CS101',   'Computer Science', 'Fall2024',   'A-',  3),
    (42, 11, 'MATH101', 'Mathematics',      'Fall2024',   'B+',  3),
    (43, 11, 'CS201',   'Computer Science', 'Spring2025', NULL,  3),
    (44, 11, 'MATH201', 'Mathematics',      'Spring2025', NULL,  3);

-- Leo Thompson (12) — Physics only, FALL-ONLY (4 Fall courses)
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (45, 12, 'PHYS101', 'Physics',          'Fall2024',   'D+',  4),
    (46, 12, 'PHYS201', 'Physics',          'Fall2024',   'D',   4),
    (47, 12, 'PHYS301', 'Physics',          'Fall2024',   'C',   3),
    (48, 12, 'PHYS401', 'Physics',          'Fall2024',   'C+',  3);

-- Maria Garcia (13) — Biology + CS
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (49, 13, 'BIO201',  'Biology',          'Fall2024',   'A',   3),
    (50, 13, 'CS101',   'Computer Science', 'Fall2024',   'A-',  3),
    (51, 13, 'BIO301',  'Biology',          'Spring2025', NULL,  4),
    (52, 13, 'CS201',   'Computer Science', 'Spring2025', NULL,  3);

-- Nathan Clark (14) — Math only, SPRING-ONLY (4 Spring courses)
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (53, 14, 'MATH101', 'Mathematics',      'Spring2025', NULL,  3),
    (54, 14, 'MATH201', 'Mathematics',      'Spring2025', NULL,  3),
    (55, 14, 'MATH301', 'Mathematics',      'Spring2025', NULL,  4),
    (56, 14, 'MATH401', 'Mathematics',      'Spring2025', NULL,  3);

-- Olivia Scott (15) — CS only
INSERT INTO {{zone_name}}.delta_demos.enrollments VALUES
    (57, 15, 'CS101',   'Computer Science', 'Fall2024',   'A',   3),
    (58, 15, 'CS201',   'Computer Science', 'Fall2024',   'A',   3),
    (59, 15, 'CS301',   'Computer Science', 'Spring2025', NULL,  3),
    (60, 15, 'CS401',   'Computer Science', 'Spring2025', NULL,  3);
