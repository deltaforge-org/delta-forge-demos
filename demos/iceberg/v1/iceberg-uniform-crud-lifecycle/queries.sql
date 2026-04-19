-- ============================================================================
-- Iceberg UniForm CRUD Lifecycle — Queries
-- ============================================================================
-- HOW UNIFORM WORKS
-- -----------------
-- All queries below read through the Delta transaction log — standard
-- Delta Forge behaviour. The Iceberg metadata in metadata/ is generated
-- automatically by the post-commit hook and is never read by these queries.
--
-- Each DML operation (INSERT, UPDATE, DELETE) creates:
--   1. A new Delta version in _delta_log/  (what these queries read)
--   2. A new Iceberg snapshot in metadata/ (for external Iceberg engines)
--
-- Both metadata chains describe the same underlying Parquet data files.
-- Time travel (VERSION AS OF N) works identically through either path.
--
-- VERIFYING THE ICEBERG OUTPUT
-- ----------------------------
-- After running this demo, verify each Iceberg snapshot with:
--   python3 verify_iceberg_metadata.py <table_data_path>/employees -v
-- ============================================================================
-- ============================================================================
-- EXPLORE: Baseline State (Version 1 / Snapshot 1)
-- ============================================================================
-- 20 employees, all active, across 4 departments.

ASSERT ROW_COUNT = 20
SELECT * FROM {{zone_name}}.iceberg_demos.employees ORDER BY id;
-- ============================================================================
-- Query 1: Department Summary — Version 1
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE emp_count = 5 WHERE department = 'Engineering'
ASSERT VALUE emp_count = 5 WHERE department = 'Sales'
ASSERT VALUE emp_count = 5 WHERE department = 'Marketing'
ASSERT VALUE emp_count = 5 WHERE department = 'Finance'
ASSERT VALUE total_salary = 745000.00 WHERE department = 'Engineering'
ASSERT VALUE total_salary = 495000.00 WHERE department = 'Sales'
ASSERT VALUE total_salary = 505000.00 WHERE department = 'Marketing'
ASSERT VALUE total_salary = 600000.00 WHERE department = 'Finance'
SELECT
    department,
    COUNT(*) AS emp_count,
    ROUND(SUM(salary), 2) AS total_salary,
    ROUND(AVG(salary), 2) AS avg_salary
FROM {{zone_name}}.iceberg_demos.employees
GROUP BY department
ORDER BY department;
-- ============================================================================
-- LEARN: UPDATE — Salary Adjustment (Version 2 / Snapshot 2)
-- ============================================================================
-- Give Engineering a 15% raise. This creates Delta version 2 and Iceberg
-- snapshot 2. Both metadata chains now have two versions.

UPDATE {{zone_name}}.iceberg_demos.employees
SET salary = ROUND(salary * 1.15, 2)
WHERE department = 'Engineering';
-- ============================================================================
-- Query 2: Verify Salary Update
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE salary = 155250.00 WHERE name = 'Alice Chen'
ASSERT VALUE salary = 178250.00 WHERE name = 'Bob Martinez'
ASSERT VALUE salary = 195500.00 WHERE name = 'Carol Wang'
ASSERT VALUE salary = 109250.00 WHERE name = 'David Kim'
ASSERT VALUE salary = 218500.00 WHERE name = 'Eve Johnson'
SELECT
    name,
    ROUND(salary, 2) AS salary
FROM {{zone_name}}.iceberg_demos.employees
WHERE department = 'Engineering'
ORDER BY id;
-- ============================================================================
-- Query 3: Time Travel — Compare Version 1 vs Version 2
-- ============================================================================
-- VERSION AS OF 1 shows pre-raise salaries. The same time-travel semantics
-- apply in both Delta and Iceberg formats.

ASSERT ROW_COUNT = 5
ASSERT VALUE old_salary = 135000.00 WHERE name = 'Alice Chen'
ASSERT VALUE new_salary = 155250.00 WHERE name = 'Alice Chen'
ASSERT VALUE salary_increase = 20250.00 WHERE name = 'Alice Chen'
SELECT
    c.name,
    ROUND(old.salary, 2) AS old_salary,
    ROUND(c.salary, 2) AS new_salary,
    ROUND(c.salary - old.salary, 2) AS salary_increase
FROM {{zone_name}}.iceberg_demos.employees c
JOIN {{zone_name}}.iceberg_demos.employees VERSION AS OF 1 old
    ON c.id = old.id
WHERE c.department = 'Engineering'
ORDER BY c.id;
-- ============================================================================
-- LEARN: DELETE — Remove Inactive Employees (Version 3 / Snapshot 3)
-- ============================================================================
-- Deactivate two employees, then delete them. Two operations = two versions.

UPDATE {{zone_name}}.iceberg_demos.employees
SET is_active = false
WHERE id IN (8, 13);

-- Version 4 / Snapshot 4
DELETE FROM {{zone_name}}.iceberg_demos.employees
WHERE is_active = false;
-- ============================================================================
-- Query 4: Post-Delete Row Count
-- ============================================================================

ASSERT ROW_COUNT = 18
SELECT * FROM {{zone_name}}.iceberg_demos.employees ORDER BY id;
-- ============================================================================
-- Query 5: Department Counts After Deletion
-- ============================================================================
-- Sales lost Henry Brown (id=8), Marketing lost Mia Patel (id=13).

ASSERT ROW_COUNT = 4
ASSERT VALUE emp_count = 5 WHERE department = 'Engineering'
ASSERT VALUE emp_count = 4 WHERE department = 'Sales'
ASSERT VALUE emp_count = 4 WHERE department = 'Marketing'
ASSERT VALUE emp_count = 5 WHERE department = 'Finance'
SELECT
    department,
    COUNT(*) AS emp_count
FROM {{zone_name}}.iceberg_demos.employees
GROUP BY department
ORDER BY department;
-- ============================================================================
-- LEARN: INSERT — New Hires (Version 5 / Snapshot 5)
-- ============================================================================
-- Add replacement hires. Creates a new Delta version and Iceberg snapshot.

INSERT INTO {{zone_name}}.iceberg_demos.employees VALUES
    (21, 'Uma Foster',    'Sales',       'Sales Rep',        76000.00,  true),
    (22, 'Victor Reyes',  'Marketing',   'SEO Specialist',   82000.00,  true),
    (23, 'Wendy Chang',   'Engineering', 'ML Engineer',      145000.00, true);
-- ============================================================================
-- Query 6: Final Row Count
-- ============================================================================

ASSERT ROW_COUNT = 21
SELECT * FROM {{zone_name}}.iceberg_demos.employees ORDER BY id;
-- ============================================================================
-- Query 7: Final Department Summary
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE emp_count = 6 WHERE department = 'Engineering'
ASSERT VALUE emp_count = 5 WHERE department = 'Sales'
ASSERT VALUE emp_count = 5 WHERE department = 'Marketing'
ASSERT VALUE emp_count = 5 WHERE department = 'Finance'
SELECT
    department,
    COUNT(*) AS emp_count,
    ROUND(SUM(salary), 2) AS total_salary
FROM {{zone_name}}.iceberg_demos.employees
GROUP BY department
ORDER BY department;
-- ============================================================================
-- Query 8: Full Version History
-- ============================================================================
-- Each version maps to an Iceberg snapshot. The history shows:
-- V1: Initial INSERT (20 rows)
-- V2: UPDATE Engineering salaries
-- V3: UPDATE is_active = false for ids 8, 13
-- V4: DELETE inactive rows
-- V5: INSERT new hires (3 rows)

ASSERT WARNING ROW_COUNT >= 5
DESCRIBE HISTORY {{zone_name}}.iceberg_demos.employees;
-- ============================================================================
-- Query 9: Time Travel Across All Versions
-- ============================================================================
-- Demonstrate reading row counts at each version boundary to prove
-- Iceberg snapshots track every mutation.

ASSERT ROW_COUNT = 1
ASSERT VALUE v1_count = 20
ASSERT VALUE v2_count = 20
ASSERT VALUE v4_count = 18
ASSERT VALUE v5_count = 21
SELECT
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.employees VERSION AS OF 1) AS v1_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.employees VERSION AS OF 2) AS v2_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.employees VERSION AS OF 4) AS v4_count,
    (SELECT COUNT(*) FROM {{zone_name}}.iceberg_demos.employees) AS v5_count;
-- ============================================================================
-- Query 10: Grand Total Salary — Current vs Original
-- ============================================================================
-- Proves the cumulative effect of all mutations is correct.

ASSERT ROW_COUNT = 1
ASSERT VALUE original_total = 2345000.00
ASSERT VALUE current_total = 2609750.00
SELECT
    ROUND((SELECT SUM(salary) FROM {{zone_name}}.iceberg_demos.employees VERSION AS OF 1), 2) AS original_total,
    ROUND(SUM(salary), 2) AS current_total
FROM {{zone_name}}.iceberg_demos.employees;
-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check covering the full lifecycle of mutations.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_employees = 21
ASSERT VALUE active_count = 21
ASSERT VALUE department_count = 4
ASSERT VALUE engineering_avg_salary = 166958.33
ASSERT VALUE total_payroll = 2609750.00
SELECT
    COUNT(*) AS total_employees,
    COUNT(*) FILTER (WHERE is_active = true) AS active_count,
    COUNT(DISTINCT department) AS department_count,
    ROUND(AVG(salary) FILTER (WHERE department = 'Engineering'), 2) AS engineering_avg_salary,
    ROUND(SUM(salary), 2) AS total_payroll
FROM {{zone_name}}.iceberg_demos.employees;
-- ============================================================================
-- ICEBERG READ-BACK VERIFICATION
-- ============================================================================
-- Register the same physical location as an external Iceberg table and
-- query it through the Iceberg metadata chain. This proves the UniForm
-- shadow metadata is readable by an Iceberg engine after the full CRUD
-- lifecycle (INSERT → UPDATE → DELETE → INSERT).
--
-- NOTE: Most Iceberg tools (PyIceberg, Spark, Trino, DuckDB) have issues
-- resolving Windows-style paths (e.g. B:\data\...). If running on Windows,
-- use forward-slash paths or UNC paths for the data_path variable.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.employees_iceberg WITH FILES;

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.employees_iceberg
USING ICEBERG
LOCATION '{{data_subdir}}/employees';

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.employees_iceberg TO USER {{current_user}};
-- ============================================================================
-- Iceberg Verify 1: Row Count — 21 Employees After Full Lifecycle
-- ============================================================================

ASSERT ROW_COUNT = 21
SELECT * FROM {{zone_name}}.iceberg_demos.employees_iceberg ORDER BY id;
-- ============================================================================
-- Iceberg Verify 2: Department Counts — Reflect Deletes + New Hires
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE emp_count = 6 WHERE department = 'Engineering'
ASSERT VALUE emp_count = 5 WHERE department = 'Sales'
ASSERT VALUE emp_count = 5 WHERE department = 'Marketing'
ASSERT VALUE emp_count = 5 WHERE department = 'Finance'
SELECT
    department,
    COUNT(*) AS emp_count
FROM {{zone_name}}.iceberg_demos.employees_iceberg
GROUP BY department
ORDER BY department;
-- ============================================================================
-- Iceberg Verify 3: Grand Totals — Must Match Delta Final State
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE total_employees = 21
ASSERT VALUE total_payroll = 2609750.00
ASSERT VALUE department_count = 4
SELECT
    COUNT(*) AS total_employees,
    ROUND(SUM(salary), 2) AS total_payroll,
    COUNT(DISTINCT department) AS department_count
FROM {{zone_name}}.iceberg_demos.employees_iceberg;
