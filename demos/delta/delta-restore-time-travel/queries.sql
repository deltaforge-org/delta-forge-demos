-- ============================================================================
-- RESTORE + Time Travel — Inspect Before You Recover — Educational Queries
-- ============================================================================
-- WHAT: Combine VERSION AS OF (time travel) with RESTORE to safely recover
--       from data corruption by inspecting history before acting.
-- WHY:  In production, you should never RESTORE blindly. Always inspect
--       multiple versions to understand what happened and choose the right
--       target version. VERSION AS OF lets you query any historical snapshot
--       without modifying the table.
-- HOW:  1. Use VERSION AS OF to browse the version history read-only
--       2. Compare the state at different versions
--       3. Identify the best version to restore to
--       4. Execute RESTORE — confident you chose the right target
--
-- Version history we will build:
--   V0: CREATE empty delta table (done in setup.sql)
--   V1: INSERT 25 student grades (done in setup.sql)
--   V2: UPDATE — curve CS101 grades up by 5 points
--   V3: UPDATE — ACCIDENT: script zeros ALL grades
--   V4: RESTORE TO VERSION 2 — recover after inspecting history
-- ============================================================================


-- ============================================================================
-- Query 1: V1 Baseline — 25 Grades Across 5 Courses
-- ============================================================================
-- The setup script inserted 25 grade records (5 students × 5 courses).
-- Each student has exactly one grade per course.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_grade = 79.2 WHERE course = 'CS101'
ASSERT VALUE avg_grade = 79.0 WHERE course = 'CS201'
ASSERT VALUE avg_grade = 80.0 WHERE course = 'MATH101'
ASSERT VALUE avg_grade = 81.6 WHERE course = 'PHYS101'
ASSERT VALUE avg_grade = 82.8 WHERE course = 'ENG101'
SELECT course,
       COUNT(*) AS students,
       ROUND(AVG(grade), 2) AS avg_grade,
       MIN(grade) AS min_grade,
       MAX(grade) AS max_grade
FROM {{zone_name}}.delta_demos.course_grades
GROUP BY course
ORDER BY course;


-- ============================================================================
-- Query 2: V2 — Curve CS101 Grades Up by 5 Points
-- ============================================================================
-- The professor decides the CS101 midterm was too difficult and applies a
-- 5-point curve to all CS101 grades. This creates version 2.

ASSERT ROW_COUNT = 5
UPDATE {{zone_name}}.delta_demos.course_grades
SET grade = grade + 5
WHERE course = 'CS101';


-- ============================================================================
-- Query 3: Verify V2 — CS101 Curved Grades
-- ============================================================================
-- CS101 grades should each be 5 points higher. Other courses are unchanged.

ASSERT ROW_COUNT = 5
ASSERT VALUE grade = 87 WHERE record_id = 1
ASSERT VALUE grade = 76 WHERE record_id = 2
ASSERT VALUE grade = 95 WHERE record_id = 3
ASSERT VALUE grade = 70 WHERE record_id = 4
ASSERT VALUE grade = 93 WHERE record_id = 5
SELECT record_id, student_name, grade
FROM {{zone_name}}.delta_demos.course_grades
WHERE course = 'CS101'
ORDER BY record_id;


-- ============================================================================
-- Query 4: V3 — ACCIDENT: Script Zeros ALL Grades
-- ============================================================================
-- A teaching assistant runs a grade-reset script against the WRONG table.
-- Instead of clearing the staging table, they zero out the live grades.
-- This creates version 3 — every grade is now 0.

ASSERT ROW_COUNT = 25
UPDATE {{zone_name}}.delta_demos.course_grades
SET grade = 0;


-- ============================================================================
-- Query 5: Verify V3 — Everything Is Zeroed
-- ============================================================================
-- Every single grade in the table is now 0. The damage is total.

ASSERT ROW_COUNT = 1
ASSERT VALUE max_grade = 0
ASSERT VALUE min_grade = 0
SELECT MIN(grade) AS min_grade, MAX(grade) AS max_grade
FROM {{zone_name}}.delta_demos.course_grades;


-- ============================================================================
-- Query 6: TIME TRAVEL — Inspect V1 (Original Baseline)
-- ============================================================================
-- Before restoring, let's inspect each version to understand exactly what
-- happened and choose the right target. VERSION AS OF is read-only — it
-- does not modify the table.
--
-- V1 shows the original grades before any changes. CS101 average is 79.2
-- (pre-curve). If we restore here, we lose the legitimate curve.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_grade = 79.2 WHERE course = 'CS101'
ASSERT VALUE avg_grade = 79.0 WHERE course = 'CS201'
SELECT course, ROUND(AVG(grade), 2) AS avg_grade, COUNT(*) AS students
FROM {{zone_name}}.delta_demos.course_grades VERSION AS OF 1
GROUP BY course
ORDER BY course;


-- ============================================================================
-- Query 7: TIME TRAVEL — Inspect V2 (After CS101 Curve)
-- ============================================================================
-- V2 has the CS101 curve applied (avg now 84.2) while all other courses
-- remain at their original values. This is the LAST KNOWN GOOD STATE —
-- the ideal target for RESTORE.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_grade = 84.2 WHERE course = 'CS101'
ASSERT VALUE avg_grade = 79.0 WHERE course = 'CS201'
ASSERT VALUE avg_grade = 80.0 WHERE course = 'MATH101'
SELECT course, ROUND(AVG(grade), 2) AS avg_grade, COUNT(*) AS students
FROM {{zone_name}}.delta_demos.course_grades VERSION AS OF 2
GROUP BY course
ORDER BY course;


-- ============================================================================
-- Query 8: TIME TRAVEL — Inspect V3 (The Accident)
-- ============================================================================
-- V3 confirms the accident: all 25 grades are zero. We now have a clear
-- picture: V2 is the target, not V1 (which would lose the curve).

ASSERT ROW_COUNT = 1
ASSERT VALUE zero_count = 25
SELECT COUNT(*) AS zero_count
FROM {{zone_name}}.delta_demos.course_grades VERSION AS OF 3
WHERE grade = 0;


-- ============================================================================
-- Query 9: RESTORE TO VERSION 2 — Recover the Last Good State
-- ============================================================================
-- Having inspected all versions, we choose V2: it has the original grades
-- plus the legitimate CS101 curve. V1 would work but loses the curve.
-- RESTORE writes a NEW commit (V4) that replicates V2's snapshot.

RESTORE {{zone_name}}.delta_demos.course_grades TO VERSION 2;

-- Verify: current state now matches V2 — curve preserved, no zeroes
ASSERT ROW_COUNT = 5
ASSERT VALUE avg_grade = 84.2 WHERE course = 'CS101'
ASSERT VALUE avg_grade = 79.0 WHERE course = 'CS201'
ASSERT VALUE avg_grade = 80.0 WHERE course = 'MATH101'
ASSERT VALUE avg_grade = 81.6 WHERE course = 'PHYS101'
ASSERT VALUE avg_grade = 82.8 WHERE course = 'ENG101'
SELECT course,
       ROUND(AVG(grade), 2) AS avg_grade,
       COUNT(*) AS students
FROM {{zone_name}}.delta_demos.course_grades
GROUP BY course
ORDER BY course;


-- ============================================================================
-- Query 10: LEARN — The Accident Version Is Still Accessible
-- ============================================================================
-- RESTORE did not erase V3. It created V4 (a new commit). The version log
-- is append-only: V0 → V1 → V2 → V3 (accident) → V4 (RESTORE).
-- You can still travel to V3 to audit what happened:

ASSERT ROW_COUNT = 1
ASSERT VALUE zero_count = 25
SELECT COUNT(*) AS zero_count
FROM {{zone_name}}.delta_demos.course_grades VERSION AS OF 3
WHERE grade = 0;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_rows: all 25 grade records present
ASSERT ROW_COUNT = 25
SELECT * FROM {{zone_name}}.delta_demos.course_grades;

-- Verify no_zeroes: minimum grade confirms no zeroed values
ASSERT VALUE min_grade = 62
SELECT MIN(grade) AS min_grade FROM {{zone_name}}.delta_demos.course_grades;

-- Verify alice_cs101: curved grade preserved (82 + 5 = 87)
ASSERT VALUE grade = 87
SELECT grade FROM {{zone_name}}.delta_demos.course_grades WHERE record_id = 1;

-- Verify david_cs101: curved grade preserved (65 + 5 = 70)
ASSERT VALUE grade = 70
SELECT grade FROM {{zone_name}}.delta_demos.course_grades WHERE record_id = 4;

-- Verify alice_math: non-CS101 grade unchanged
ASSERT VALUE grade = 95
SELECT grade FROM {{zone_name}}.delta_demos.course_grades WHERE record_id = 11;

-- Verify course_count: all 5 courses present
ASSERT VALUE cnt = 5
SELECT COUNT(DISTINCT course) AS cnt FROM {{zone_name}}.delta_demos.course_grades;

-- Verify overall_avg: matches V2 state exactly
ASSERT VALUE avg = 81.52
SELECT ROUND(AVG(grade), 2) AS avg FROM {{zone_name}}.delta_demos.course_grades;
