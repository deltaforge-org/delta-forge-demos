-- ============================================================================
-- Delta Constraint Enforcement — Educational Queries
-- ============================================================================
-- WHAT: CHECK constraints are boolean expressions stored in the Delta
--       transaction log that every row must satisfy on every commit.
-- WHY:  They prevent invalid data at write time, so downstream consumers
--       never encounter out-of-range ages, negative salaries, or impossible
--       ratings — eliminating an entire class of data quality bugs.
-- HOW:  Constraints are recorded as metadata actions in the Delta log. Any
--       INSERT or UPDATE that would produce a row violating a constraint
--       causes the entire transaction to fail and roll back.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Inspect the Baseline Employee Data
-- ============================================================================
-- The validated_employees table enforces three business rules:
--   1. age BETWEEN 18 AND 70
--   2. salary > 0
--   3. rating BETWEEN 0.0 AND 5.0
-- Let's browse the baseline data before any modifications.

ASSERT ROW_COUNT = 15
SELECT id, name, age, salary, rating, department
FROM {{zone_name}}.delta_demos.validated_employees
ORDER BY department, name
LIMIT 15;


-- ============================================================================
-- LEARN: Verify Constraints Hold on Baseline Data
-- ============================================================================
-- Before any DML, confirm every row satisfies all constraints.
-- A non-zero violation count would indicate bad seed data.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_employees = 50
ASSERT VALUE age_violations = 0
ASSERT VALUE salary_violations = 0
ASSERT VALUE rating_violations = 0
ASSERT VALUE null_id_violations = 0
SELECT
    COUNT(*) AS total_employees,
    COUNT(*) FILTER (WHERE age < 18 OR age > 70) AS age_violations,
    COUNT(*) FILTER (WHERE salary <= 0) AS salary_violations,
    COUNT(*) FILTER (WHERE rating < 0.0 OR rating > 5.0) AS rating_violations,
    COUNT(*) FILTER (WHERE id IS NULL) AS null_id_violations
FROM {{zone_name}}.delta_demos.validated_employees;


-- ============================================================================
-- STEP 1: UPDATE — 10% Salary Raise for All Employees
-- ============================================================================
-- This UPDATE multiplies every salary by 1.10. Because every original salary
-- is positive, the result is always positive — preserving the salary > 0
-- constraint. A constraint-aware engine would verify this before committing.

ASSERT ROW_COUNT = 50
UPDATE {{zone_name}}.delta_demos.validated_employees
SET salary = ROUND(salary * 1.10, 2);


-- ============================================================================
-- LEARN: Verify Salary Constraint After the Raise
-- ============================================================================
-- Confirm no salary dropped to zero or went negative. Alice Morgan (id=1)
-- should have gone from 95,000 to 104,500.

ASSERT ROW_COUNT = 5
ASSERT VALUE salary = 104500.00 WHERE id = 1
SELECT id, name, salary
FROM {{zone_name}}.delta_demos.validated_employees
WHERE id IN (1, 2, 3, 4, 5)
ORDER BY id;

-- Salary distribution after the 10% raise — every band should show
-- positive values only.
ASSERT ROW_COUNT = 4
ASSERT VALUE employee_count = 5 WHERE salary_band = 'Under 70K'
SELECT
    CASE
        WHEN salary < 70000 THEN 'Under 70K'
        WHEN salary < 100000 THEN '70K - 100K'
        WHEN salary < 130000 THEN '100K - 130K'
        ELSE '130K+'
    END AS salary_band,
    COUNT(*) AS employee_count,
    ROUND(AVG(salary), 2) AS avg_salary
FROM {{zone_name}}.delta_demos.validated_employees
GROUP BY
    CASE
        WHEN salary < 70000 THEN 'Under 70K'
        WHEN salary < 100000 THEN '70K - 100K'
        WHEN salary < 130000 THEN '100K - 130K'
        ELSE '130K+'
    END
ORDER BY min(salary);


-- ============================================================================
-- STEP 2: UPDATE — Adjust Ratings for Low-Rated Employees
-- ============================================================================
-- Increase rating by 0.2 for employees with rating < 3.5. The CASE expression
-- caps the result at 5.0, preserving the rating BETWEEN 0.0 AND 5.0
-- constraint. This is a common pattern: constraint-safe DML uses bounded
-- arithmetic to guarantee invariants are never violated.

ASSERT ROW_COUNT = 11
UPDATE {{zone_name}}.delta_demos.validated_employees
SET rating = CASE WHEN rating + 0.2 > 5.0 THEN 5.0 ELSE ROUND(rating + 0.2, 1) END
WHERE rating < 3.5;


-- ============================================================================
-- LEARN: Verify Rating Constraint After Adjustments
-- ============================================================================
-- Examine employees whose ratings were adjusted (originally < 3.5).
-- After the +0.2 bump, all should still be within [0.0, 5.0].

ASSERT ROW_COUNT = 7
SELECT id, name, rating, department,
       CASE
           WHEN rating <= 3.4 THEN 'Was adjusted (originally < 3.3)'
           ELSE 'Boundary case (originally 3.3-3.4)'
       END AS adjustment_note
FROM {{zone_name}}.delta_demos.validated_employees
WHERE rating <= 3.4
ORDER BY rating;


-- ============================================================================
-- STEP 3: DELETE — Remove 5 Employees
-- ============================================================================
-- DELETEs cannot violate CHECK constraints because they only remove rows —
-- they never create new data. In the Delta log, a DELETE produces "remove"
-- actions for the affected data files (or deletion vectors if DVs are
-- enabled). We remove 5 youngest/newest hires: ids 12, 19, 29, 34, 40.

ASSERT ROW_COUNT = 5
DELETE FROM {{zone_name}}.delta_demos.validated_employees
WHERE id IN (12, 19, 29, 34, 40);


-- ============================================================================
-- LEARN: Confirm Deleted Employees Are Gone
-- ============================================================================
-- This query should return zero rows — the deleted employees no longer exist.

ASSERT ROW_COUNT = 0
SELECT id, name
FROM {{zone_name}}.delta_demos.validated_employees
WHERE id IN (12, 19, 29, 34, 40);


-- ============================================================================
-- EXPLORE: Department-Level Data Quality Summary
-- ============================================================================
-- Aggregating by department shows how constraints ensure uniform data quality
-- across organizational boundaries. Every department's min/max values must
-- fall within the declared constraint ranges.

ASSERT ROW_COUNT = 5
ASSERT VALUE headcount = 7 WHERE department = 'HR'
SELECT department,
       COUNT(*) AS headcount,
       MIN(age) AS min_age,
       MAX(age) AS max_age,
       ROUND(MIN(salary), 2) AS min_salary,
       ROUND(MAX(salary), 2) AS max_salary,
       MIN(rating) AS min_rating,
       MAX(rating) AS max_rating
FROM {{zone_name}}.delta_demos.validated_employees
GROUP BY department
ORDER BY department;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count
ASSERT ROW_COUNT = 45
SELECT * FROM {{zone_name}}.delta_demos.validated_employees;

-- Verify age constraint holds
ASSERT VALUE age_violations = 0
SELECT COUNT(*) AS age_violations FROM {{zone_name}}.delta_demos.validated_employees WHERE age < 18 OR age > 70;

-- Verify salary constraint holds
ASSERT VALUE salary_violations = 0
SELECT COUNT(*) AS salary_violations FROM {{zone_name}}.delta_demos.validated_employees WHERE salary <= 0;

-- Verify rating constraint holds
ASSERT VALUE rating_violations = 0
SELECT COUNT(*) AS rating_violations FROM {{zone_name}}.delta_demos.validated_employees WHERE rating < 0.0 OR rating > 5.0;

-- Verify no null IDs
ASSERT VALUE null_id_count = 0
SELECT COUNT(*) AS null_id_count FROM {{zone_name}}.delta_demos.validated_employees WHERE id IS NULL;

-- Verify Alice's salary after 10% raise
ASSERT VALUE salary = 104500.0
SELECT salary FROM {{zone_name}}.delta_demos.validated_employees WHERE id = 1;

-- Verify Erik's rating after adjustment
ASSERT VALUE rating = 3.0
SELECT rating FROM {{zone_name}}.delta_demos.validated_employees WHERE id = 31;

-- Verify deleted employees are gone
ASSERT VALUE deleted_count = 0
SELECT COUNT(*) AS deleted_count FROM {{zone_name}}.delta_demos.validated_employees WHERE id IN (12, 19, 29, 34, 40);
