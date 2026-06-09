-- ============================================================================
-- Delta Column Mapping — Educational Queries
-- ============================================================================
-- WHAT: Column mapping decouples logical column names from physical Parquet
--       column names, enabling schema evolution operations like rename and drop
-- WHY:  Without column mapping, renaming a column requires rewriting every
--       Parquet file. With mapping mode 'name', only the metadata changes
-- HOW:  The Delta protocol tracks a column ID and physical name in the schema
--       metadata. TBLPROPERTIES 'delta.columnMapping.mode' = 'name' enables
--       this, requiring minReaderVersion=2 and minWriterVersion=5. ALTER TABLE
--       ADD/RENAME/DROP COLUMN only updates the transaction log, not data files.
--
-- Setup applied:
--   1. Created employee_directory with column mapping mode 'name' (8 columns)
--   2. Inserted 40 employees across 6 departments
--   3. ALTER TABLE ADD COLUMN location VARCHAR (zero data-file rewrites)
--   4. Updated 5 employees to Senior/Lead titles
--   5. Deactivated 3 employees (is_active = 0)
-- ============================================================================


-- ============================================================================
-- Query 1: Department breakdown across 40 employees
-- ============================================================================
-- The table has 6 departments. Engineering has the most headcount.
-- Average salaries reflect seniority mix per department.

ASSERT ROW_COUNT = 6
ASSERT VALUE headcount = 8 WHERE department = 'Engineering'
ASSERT VALUE headcount = 5 WHERE department = 'Marketing'
ASSERT VALUE avg_salary = 110000 WHERE department = 'Engineering'
ASSERT VALUE avg_salary = 86800 WHERE department = 'Marketing'
SELECT department,
       COUNT(*) AS headcount,
       ROUND(AVG(salary), 0) AS avg_salary
FROM {{zone_name}}.delta_demos.employee_directory
GROUP BY department
ORDER BY headcount DESC;


-- ============================================================================
-- Query 2: New column exists but is NULL for rows inserted before ADD COLUMN
-- ============================================================================
-- The ADD COLUMN location VARCHAR operation in setup wrote zero Parquet bytes.
-- Every row inserted before the schema change reads NULL for location.
-- This is the core benefit of column mapping: instant schema evolution with
-- zero I/O cost on existing data files.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 10
SELECT id, full_name, department,
       location,
       CASE WHEN location IS NULL THEN 'Added after insert - NULL expected'
            ELSE 'Has value' END AS column_mapping_note
FROM {{zone_name}}.delta_demos.employee_directory
ORDER BY id
LIMIT 10;


-- ============================================================================
-- Query 3: Promoted employees — 5 rows with Senior/Lead titles
-- ============================================================================
-- Setup promoted 5 employees via UPDATE. The column mapping ensures columns
-- are referenced by their physical IDs rather than names, so the UPDATE worked
-- correctly even after the schema evolution that added the location column.

ASSERT ROW_COUNT = 5
SELECT id, full_name, department, title, salary, location
FROM {{zone_name}}.delta_demos.employee_directory
WHERE title LIKE '%Senior%' OR title LIKE '%Lead%'
ORDER BY department, full_name;


-- ============================================================================
-- Query 4: Deactivated employees (is_active = 0)
-- ============================================================================
-- Setup deactivated employees 10, 19, and 29 via UPDATE. The physical column
-- ID for is_active is preserved by column mapping regardless of schema changes.

ASSERT NO_FAIL IN result
ASSERT ROW_COUNT = 3
SELECT id, full_name, department, title, is_active
FROM {{zone_name}}.delta_demos.employee_directory
WHERE is_active = 0
ORDER BY id;


-- ============================================================================
-- Query 5: Salary distribution across departments — active employees only
-- ============================================================================
-- In a column-mapped table, column identity is determined by ID, not by name
-- and position in the Parquet schema. This means:
--   - Renaming a column requires updating metadata only
--   - Dropping a column requires updating metadata only
--   - Adding a column at a specific position is supported without rewriting data
--
-- Here we query salary stats for the 37 active employees across all departments.

ASSERT ROW_COUNT = 6
ASSERT VALUE avg_salary = 110000 WHERE department = 'Engineering'
ASSERT VALUE avg_salary = 91286 WHERE department = 'Finance'
ASSERT VALUE avg_salary = 77167 WHERE department = 'HR'
ASSERT VALUE employees = 8 WHERE department = 'Engineering'
ASSERT VALUE employees = 5 WHERE department = 'Operations'
SELECT department,
       ROUND(MIN(salary), 0) AS min_salary,
       ROUND(AVG(salary), 0) AS avg_salary,
       ROUND(MAX(salary), 0) AS max_salary,
       COUNT(*) AS employees
FROM {{zone_name}}.delta_demos.employee_directory
WHERE is_active = 1
GROUP BY department
ORDER BY avg_salary DESC;


-- ============================================================================
-- Query 6: Full Employee Directory with location column
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT id, full_name, department, title, email, start_date, salary, is_active,
       CASE WHEN location IS NULL THEN '(NULL)' ELSE location END AS location
FROM {{zone_name}}.delta_demos.employee_directory
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Verification of row counts, department distribution, promotions, and the
-- NULL pattern from the column-mapped ADD COLUMN operation.

-- Verify total row count
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify department count
ASSERT VALUE department_count = 6
SELECT COUNT(DISTINCT department) AS department_count FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify promoted titles count
ASSERT VALUE promoted_count = 5
SELECT COUNT(*) FILTER (WHERE title LIKE '%Senior%' OR title LIKE '%Lead%') AS promoted_count
FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify inactive employee count
ASSERT VALUE inactive_count = 3
SELECT COUNT(*) FILTER (WHERE is_active = 0) AS inactive_count FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify active employee count
ASSERT VALUE active_count = 37
SELECT COUNT(*) FILTER (WHERE is_active = 1) AS active_count FROM {{zone_name}}.delta_demos.employee_directory;

-- Verify Engineering department count
ASSERT VALUE engineering_count = 8
SELECT COUNT(*) AS engineering_count FROM {{zone_name}}.delta_demos.employee_directory WHERE department = 'Engineering';

-- Verify all locations are NULL (added via column mapping, never populated)
ASSERT VALUE location_null_count = 40
SELECT COUNT(*) AS location_null_count FROM {{zone_name}}.delta_demos.employee_directory WHERE location IS NULL;

-- Verify salary for employee id 1
ASSERT VALUE salary = 115000.00
SELECT salary FROM {{zone_name}}.delta_demos.employee_directory WHERE id = 1;
