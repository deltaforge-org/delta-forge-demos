-- ============================================================================
-- Iceberg Native Schema Evolution — Queries
-- ============================================================================
-- Demonstrates native Iceberg format-version 2 table reading with schema
-- evolution: ADD COLUMN (title, location), RENAME COLUMN (dept→department),
-- field-id stability, and NULL handling for late-added columns.
-- All queries are read-only.
-- ============================================================================


-- ============================================================================
-- Query 1: Baseline — Total Row Count
-- ============================================================================
-- Verifies that Delta Forge correctly resolves 360 rows across 3 data files
-- via the Iceberg metadata chain, including the merge-on-read UPDATE that
-- replaced 60 rows with location-enriched versions.

ASSERT ROW_COUNT = 360
ASSERT VALUE full_name = 'Alice Smith' WHERE emp_id = 1
ASSERT VALUE department = 'Engineering' WHERE emp_id = 1
ASSERT VALUE salary = 105775.22 WHERE emp_id = 1
ASSERT VALUE full_name = 'Eve Miller' WHERE emp_id = 360
ASSERT VALUE department = 'HR' WHERE emp_id = 360
ASSERT VALUE title = 'Data Scientist' WHERE emp_id = 360
ASSERT VALUE location = 'New York' WHERE emp_id = 360
SELECT * FROM {{zone_name}}.iceberg.employee_directory;


-- ============================================================================
-- Query 2: Verify All Columns Present (Evolved Schema)
-- ============================================================================
-- The final schema has 7 columns: emp_id, full_name, department (renamed
-- from dept), salary, hire_date, title (added), location (added).
-- Exercises Iceberg field-id mapping across all schema versions.

ASSERT ROW_COUNT = 360
ASSERT VALUE full_name = 'Alice Smith' WHERE emp_id = 1
ASSERT VALUE salary = 105775.22 WHERE emp_id = 1
ASSERT VALUE title IS NULL WHERE emp_id = 1
ASSERT VALUE location IS NULL WHERE emp_id = 1
ASSERT VALUE full_name = 'Tina Allen' WHERE emp_id = 200
ASSERT VALUE department = 'Finance' WHERE emp_id = 200
ASSERT VALUE salary = 67289.26 WHERE emp_id = 200
ASSERT VALUE full_name = 'Eve Miller' WHERE emp_id = 360
ASSERT VALUE salary = 69124.63 WHERE emp_id = 360
ASSERT VALUE title = 'Data Scientist' WHERE emp_id = 360
ASSERT VALUE location = 'New York' WHERE emp_id = 360
SELECT
    emp_id,
    full_name,
    department,
    salary,
    hire_date,
    title,
    location
FROM {{zone_name}}.iceberg.employee_directory
ORDER BY emp_id;


-- ============================================================================
-- Query 3: NULL Check — Rows Where Title IS NULL
-- ============================================================================
-- The original 300 employees were inserted before the title column was added.
-- They should have NULL titles. Only the 60 employees from snapshot 2 have titles.

ASSERT ROW_COUNT = 300
ASSERT VALUE full_name = 'Alice Smith' WHERE emp_id = 1
ASSERT VALUE department = 'Engineering' WHERE emp_id = 1
ASSERT VALUE full_name = 'Jack Allen' WHERE emp_id = 100
ASSERT VALUE department = 'Sales' WHERE emp_id = 100
ASSERT VALUE full_name = 'Derek Allen' WHERE emp_id = 300
ASSERT VALUE department = 'HR' WHERE emp_id = 300
SELECT
    emp_id,
    full_name,
    department,
    title
FROM {{zone_name}}.iceberg.employee_directory
WHERE title IS NULL
ORDER BY emp_id;


-- ============================================================================
-- Query 4: NULL Check — Rows Where Location IS NOT NULL
-- ============================================================================
-- Only 60 employees (emp_id 301-360) were updated with locations in snapshot 3.
-- All others have NULL locations.

ASSERT ROW_COUNT = 60
ASSERT VALUE full_name = 'Frank Davis' WHERE emp_id = 301
ASSERT VALUE department = 'Engineering' WHERE emp_id = 301
ASSERT VALUE location = 'San Francisco' WHERE emp_id = 301
ASSERT VALUE full_name = 'Eve Martin' WHERE emp_id = 330
ASSERT VALUE title = 'Data Scientist' WHERE emp_id = 330
ASSERT VALUE location = 'New York' WHERE emp_id = 330
ASSERT VALUE full_name = 'Eve Miller' WHERE emp_id = 360
ASSERT VALUE location = 'New York' WHERE emp_id = 360
SELECT
    emp_id,
    full_name,
    department,
    title,
    location
FROM {{zone_name}}.iceberg.employee_directory
WHERE location IS NOT NULL
ORDER BY emp_id;


-- ============================================================================
-- Query 5: Per-Department Breakdown (Renamed Column)
-- ============================================================================
-- Uses the renamed column 'department' (was 'dept'). Iceberg resolves this
-- via field-id stability. 72 employees per department (300/5=60 initial + 12 new).

ASSERT ROW_COUNT = 5
ASSERT VALUE emp_count = 72 WHERE department = 'Engineering'
ASSERT VALUE emp_count = 72 WHERE department = 'Finance'
ASSERT VALUE emp_count = 72 WHERE department = 'HR'
ASSERT VALUE emp_count = 72 WHERE department = 'Marketing'
ASSERT VALUE emp_count = 72 WHERE department = 'Sales'
SELECT
    department,
    COUNT(*) AS emp_count
FROM {{zone_name}}.iceberg.employee_directory
GROUP BY department
ORDER BY department;


-- ============================================================================
-- Query 6: Salary Aggregations
-- ============================================================================
-- Floating-point aggregation across all 360 employees.

ASSERT ROW_COUNT = 1
ASSERT VALUE avg_salary = 97347.2
ASSERT VALUE min_salary = 50115.82
ASSERT VALUE max_salary = 149715.19
ASSERT VALUE total_salary = 35044993.03
SELECT
    ROUND(AVG(salary), 2) AS avg_salary,
    ROUND(MIN(salary), 2) AS min_salary,
    ROUND(MAX(salary), 2) AS max_salary,
    ROUND(SUM(salary), 2) AS total_salary
FROM {{zone_name}}.iceberg.employee_directory;


-- ============================================================================
-- Query 7: Per-Department Salary Summary
-- ============================================================================
-- Validates salary computation per department using the renamed column.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_salary = 96315.9 WHERE department = 'Engineering'
ASSERT VALUE avg_salary = 94828.12 WHERE department = 'Finance'
ASSERT VALUE avg_salary = 97538.32 WHERE department = 'HR'
ASSERT VALUE avg_salary = 97049.79 WHERE department = 'Marketing'
ASSERT VALUE avg_salary = 101003.89 WHERE department = 'Sales'
SELECT
    department,
    COUNT(*) AS emp_count,
    ROUND(AVG(salary), 2) AS avg_salary,
    ROUND(SUM(salary), 2) AS total_salary
FROM {{zone_name}}.iceberg.employee_directory
GROUP BY department
ORDER BY department;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, schema evolution indicators
-- (null titles, non-null locations), department count, and salary totals.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 360
ASSERT VALUE dept_count = 5
ASSERT VALUE null_title_count = 300
ASSERT VALUE has_title_count = 60
ASSERT VALUE null_location_count = 300
ASSERT VALUE has_location_count = 60
ASSERT VALUE total_salary = 35044993.03
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT department) AS dept_count,
    SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS null_title_count,
    SUM(CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END) AS has_title_count,
    SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS null_location_count,
    SUM(CASE WHEN location IS NOT NULL THEN 1 ELSE 0 END) AS has_location_count,
    ROUND(SUM(salary), 2) AS total_salary
FROM {{zone_name}}.iceberg.employee_directory;
