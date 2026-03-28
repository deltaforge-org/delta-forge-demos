-- ============================================================================
-- University Course Enrollment Analytics — Educational Queries
-- ============================================================================
-- WHAT: Subqueries (scalar, correlated, IN, EXISTS) and set operations
--       (UNION ALL, INTERSECT, EXCEPT) on a university enrollment dataset
-- WHY:  These patterns are essential for comparing datasets, checking
--       membership, and computing per-row aggregates without explicit joins
-- HOW:  Two Delta tables (students + enrollments) with 15 students across
--       4 departments and 2 semesters, enabling cross-reference queries
-- ============================================================================


-- ============================================================================
-- LEARN: Scalar Subquery — Enrollment Count Per Student
-- ============================================================================
-- A scalar subquery in the SELECT list runs once per outer row, returning a
-- single value. Here it counts each student's enrollments without a GROUP BY
-- on the outer query, preserving the per-student row structure.

ASSERT ROW_COUNT = 15
ASSERT VALUE enrollment_count = 4 WHERE student_name = 'Alice Chen'
ASSERT VALUE enrollment_count = 4 WHERE student_name = 'Olivia Scott'
SELECT s.student_id,
       s.student_name,
       s.major,
       s.gpa,
       (SELECT COUNT(*)
        FROM {{zone_name}}.delta_demos.enrollments e
        WHERE e.student_id = s.student_id) AS enrollment_count
FROM {{zone_name}}.delta_demos.students s
ORDER BY s.student_name;


-- ============================================================================
-- LEARN: IN Subquery — Students Enrolled in Computer Science Courses
-- ============================================================================
-- IN (SELECT ...) checks whether a value appears in the result set of a
-- subquery. This is equivalent to an EXISTS with an equality condition but
-- often reads more naturally for membership tests.

ASSERT ROW_COUNT = 7
ASSERT VALUE student_name = 'Alice Chen' WHERE student_id = 1
ASSERT VALUE student_name = 'Olivia Scott' WHERE student_id = 15
SELECT s.student_id,
       s.student_name,
       s.major,
       s.gpa
FROM {{zone_name}}.delta_demos.students s
WHERE s.student_id IN (
    SELECT e.student_id
    FROM {{zone_name}}.delta_demos.enrollments e
    WHERE e.department = 'Computer Science'
)
ORDER BY s.student_name;


-- ============================================================================
-- LEARN: EXISTS — Students Who Completed At Least One Course
-- ============================================================================
-- EXISTS returns TRUE if the correlated subquery produces any rows.
-- It short-circuits on the first match, making it efficient for "has any"
-- checks. Here we find students with at least one non-NULL grade.
-- Henry Brown and Nathan Clark (Spring-only) have no grades yet.

ASSERT ROW_COUNT = 13
ASSERT VALUE student_name = 'Alice Chen' WHERE student_id = 1
ASSERT VALUE student_name = 'Frank Lopez' WHERE student_id = 6
SELECT s.student_id,
       s.student_name,
       s.major
FROM {{zone_name}}.delta_demos.students s
WHERE EXISTS (
    SELECT 1
    FROM {{zone_name}}.delta_demos.enrollments e
    WHERE e.student_id = s.student_id
      AND e.grade IS NOT NULL
)
ORDER BY s.student_name;


-- ============================================================================
-- LEARN: NOT EXISTS — Students With No Mathematics Courses
-- ============================================================================
-- NOT EXISTS is the logical complement: it returns TRUE when the subquery
-- produces zero rows. This finds students who have never enrolled in any
-- Mathematics department course across either semester.

ASSERT ROW_COUNT = 8
ASSERT VALUE student_name = 'Carol Johnson' WHERE student_id = 3
ASSERT VALUE student_name = 'Henry Brown' WHERE student_id = 8
ASSERT VALUE student_name = 'Olivia Scott' WHERE student_id = 15
SELECT s.student_id,
       s.student_name,
       s.major
FROM {{zone_name}}.delta_demos.students s
WHERE NOT EXISTS (
    SELECT 1
    FROM {{zone_name}}.delta_demos.enrollments e
    WHERE e.student_id = s.student_id
      AND e.department = 'Mathematics'
)
ORDER BY s.student_name;


-- ============================================================================
-- LEARN: Correlated Subquery — Best Grade Per Student
-- ============================================================================
-- A correlated subquery references the outer query's columns, executing once
-- per outer row. Here we find each student's best (lexicographically smallest)
-- grade and the course that earned it. Only students with completed courses
-- (non-NULL grades) appear. MIN(grade) works because letter grades sort
-- lexicographically: A < A- < B < B+ < ... < D < D+.

ASSERT ROW_COUNT = 13
ASSERT VALUE best_grade = 'A' WHERE student_name = 'Alice Chen'
ASSERT VALUE best_course = 'CS101' WHERE student_name = 'Alice Chen'
ASSERT VALUE best_grade = 'C' WHERE student_name = 'David Kim'
ASSERT VALUE best_course = 'MATH101' WHERE student_name = 'David Kim'
SELECT s.student_id,
       s.student_name,
       (SELECT MIN(e.grade)
        FROM {{zone_name}}.delta_demos.enrollments e
        WHERE e.student_id = s.student_id
          AND e.grade IS NOT NULL) AS best_grade,
       (SELECT e2.course_code
        FROM {{zone_name}}.delta_demos.enrollments e2
        WHERE e2.student_id = s.student_id
          AND e2.grade = (SELECT MIN(e3.grade)
                          FROM {{zone_name}}.delta_demos.enrollments e3
                          WHERE e3.student_id = s.student_id
                            AND e3.grade IS NOT NULL)
        LIMIT 1) AS best_course
FROM {{zone_name}}.delta_demos.students s
WHERE EXISTS (
    SELECT 1
    FROM {{zone_name}}.delta_demos.enrollments e
    WHERE e.student_id = s.student_id
      AND e.grade IS NOT NULL
)
ORDER BY s.student_name;


-- ============================================================================
-- LEARN: UNION ALL — Combined Enrollment Lists With Semester Label
-- ============================================================================
-- UNION ALL concatenates two result sets without deduplication. Unlike UNION,
-- it preserves all rows including duplicates, and is faster because it skips
-- the dedup step. Here we label each enrollment with its semester for a
-- unified view of all 60 enrollments.

ASSERT ROW_COUNT = 60
ASSERT VALUE semester_label = 'Fall 2024' WHERE enrollment_id = 1
ASSERT VALUE semester_label = 'Spring 2025' WHERE enrollment_id = 3
SELECT e.enrollment_id,
       s.student_name,
       e.course_code,
       e.department,
       'Fall 2024' AS semester_label,
       e.grade,
       e.credits
FROM {{zone_name}}.delta_demos.enrollments e
JOIN {{zone_name}}.delta_demos.students s ON s.student_id = e.student_id
WHERE e.semester = 'Fall2024'
UNION ALL
SELECT e.enrollment_id,
       s.student_name,
       e.course_code,
       e.department,
       'Spring 2025' AS semester_label,
       e.grade,
       e.credits
FROM {{zone_name}}.delta_demos.enrollments e
JOIN {{zone_name}}.delta_demos.students s ON s.student_id = e.student_id
WHERE e.semester = 'Spring2025'
ORDER BY enrollment_id;


-- ============================================================================
-- LEARN: INTERSECT — Students Enrolled in Both Semesters
-- ============================================================================
-- INTERSECT returns only rows that appear in both result sets. Here we find
-- students who enrolled in both Fall 2024 and Spring 2025. Frank Lopez and
-- Leo Thompson (Fall-only) and Henry Brown and Nathan Clark (Spring-only)
-- are excluded, leaving 11 students.

ASSERT ROW_COUNT = 11
ASSERT VALUE student_id = 1
SELECT DISTINCT e.student_id
FROM {{zone_name}}.delta_demos.enrollments e
WHERE e.semester = 'Fall2024'
INTERSECT
SELECT DISTINCT e.student_id
FROM {{zone_name}}.delta_demos.enrollments e
WHERE e.semester = 'Spring2025'
ORDER BY student_id;


-- ============================================================================
-- LEARN: EXCEPT — Students in Fall But Not Spring (Semester Dropouts)
-- ============================================================================
-- EXCEPT returns rows from the first query that do not appear in the second.
-- This identifies students who enrolled in Fall 2024 but did not return for
-- Spring 2025: Frank Lopez (6) and Leo Thompson (12).

ASSERT ROW_COUNT = 2
ASSERT VALUE student_id = 6
SELECT DISTINCT e.student_id
FROM {{zone_name}}.delta_demos.enrollments e
WHERE e.semester = 'Fall2024'
EXCEPT
SELECT DISTINCT e.student_id
FROM {{zone_name}}.delta_demos.enrollments e
WHERE e.semester = 'Spring2025'
ORDER BY student_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Summary verification ensuring the dataset and all query patterns produce
-- expected results.

-- Verify total row counts
ASSERT ROW_COUNT = 15
SELECT * FROM {{zone_name}}.delta_demos.students;

ASSERT ROW_COUNT = 60
SELECT * FROM {{zone_name}}.delta_demos.enrollments;

-- Verify students enrolled in multiple departments
ASSERT VALUE multi_dept_count = 8
SELECT COUNT(*) AS multi_dept_count FROM (
    SELECT e.student_id
    FROM {{zone_name}}.delta_demos.enrollments e
    GROUP BY e.student_id
    HAVING COUNT(DISTINCT e.department) > 1
);

-- Verify NULL grade count (in-progress Spring courses + Spring-only students)
ASSERT VALUE null_grade_count = 30
SELECT COUNT(*) AS null_grade_count
FROM {{zone_name}}.delta_demos.enrollments
WHERE grade IS NULL;

-- Verify total credits across all enrollments
ASSERT VALUE total_credits = 200
SELECT SUM(credits) AS total_credits
FROM {{zone_name}}.delta_demos.enrollments;

-- Verify average GPA across all students
ASSERT VALUE avg_gpa = 3.32
SELECT ROUND(AVG(gpa), 2) AS avg_gpa
FROM {{zone_name}}.delta_demos.students;

-- Verify 4 distinct departments in enrollments
ASSERT VALUE dept_count = 4
SELECT COUNT(DISTINCT department) AS dept_count
FROM {{zone_name}}.delta_demos.enrollments;
