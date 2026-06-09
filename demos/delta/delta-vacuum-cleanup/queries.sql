-- ============================================================================
-- Delta VACUUM — Cleanup Orphaned Files — Educational Queries
-- ============================================================================
-- WHAT: VACUUM removes Parquet data files that are no longer referenced by the
--       current Delta table version, reclaiming disk space.
-- WHY:  Every UPDATE and DELETE in Delta uses copy-on-write, creating new files
--       and leaving old ones orphaned. Without VACUUM, storage grows unboundedly.
-- HOW:  VACUUM scans the _delta_log to find which files are still referenced,
--       then deletes any data files older than the retention period (default 7
--       days) that are NOT in the current file manifest.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Pre-VACUUM state — orphaned files still exist on disk
-- ============================================================================
-- The table has been through: initial INSERT (50 rows), salary UPDATE
-- (Engineering +15%), 3 department transfers, 2 termination DELETEs, and 5 new
-- hires. Each of those operations created new Parquet files via copy-on-write,
-- orphaning the previous versions. The data is correct, but storage contains
-- many unreferenced files.

ASSERT VALUE headcount = 12 WHERE department = 'Engineering'
ASSERT VALUE headcount = 13 WHERE department = 'Marketing'
ASSERT VALUE headcount = 10 WHERE department = 'Finance'
ASSERT ROW_COUNT = 5
SELECT department, COUNT(*) AS headcount,
       ROUND(AVG(salary), 2) AS avg_salary,
       ROUND(MIN(salary), 2) AS min_salary,
       ROUND(MAX(salary), 2) AS max_salary
FROM {{zone_name}}.delta_demos.hr_employees
GROUP BY department
ORDER BY department;


-- ============================================================================
-- EXPLORE: Engineering salary raises — the biggest source of orphaned files
-- ============================================================================
-- The 15% salary adjustment rewrote every file containing Engineering rows.
-- The old files with pre-raise salaries are still on disk as orphaned files.

ASSERT ROW_COUNT = 12
SELECT id, name, department, salary, hire_date
FROM {{zone_name}}.delta_demos.hr_employees
WHERE department = 'Engineering'
ORDER BY salary DESC;


-- ============================================================================
-- EXPLORE: Department transfers — each created orphaned file copies
-- ============================================================================
-- Three employees changed departments. Each UPDATE touched the files containing
-- those rows, creating new files with the updated department value and orphaning
-- the old files. This is Delta's copy-on-write pattern.

ASSERT ROW_COUNT = 3
SELECT id, name, department, salary
FROM {{zone_name}}.delta_demos.hr_employees
WHERE id IN (12, 33, 42)
ORDER BY id;


-- ============================================================================
-- VACUUM — clean up orphaned data files
-- ============================================================================
-- This is the key command. After multiple updates and deletes, old data files
-- remain on storage even though they are no longer referenced by the current
-- table version. VACUUM removes these superseded files to reclaim disk space.
-- The data visible to queries will be IDENTICAL before and after VACUUM runs.

VACUUM {{zone_name}}.delta_demos.hr_employees;


-- ============================================================================
-- LEARN: Post-VACUUM — data integrity is preserved
-- ============================================================================
-- VACUUM only affects the physical files on disk, not the logical table state.
-- The orphaned files are gone, but every query returns the same results as
-- before VACUUM ran. Time travel to versions before VACUUM may break (those
-- old files are deleted), but the current version is always intact.

ASSERT VALUE total_employees = 53
ASSERT VALUE departments = 5
ASSERT VALUE total_payroll = 5304700.0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS total_employees,
       COUNT(DISTINCT department) AS departments,
       ROUND(SUM(salary), 2) AS total_payroll
FROM {{zone_name}}.delta_demos.hr_employees;


-- ============================================================================
-- LEARN: DELETE creates orphaned files, not "holes"
-- ============================================================================
-- When employees 35 and 44 were terminated and deleted, Delta rewrote the
-- affected files WITHOUT those rows. The old files (still containing the
-- deleted rows) were orphaned and have now been cleaned up by VACUUM.
-- The deleted employees are truly gone:

ASSERT VALUE terminated_still_present = 0
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS terminated_still_present
FROM {{zone_name}}.delta_demos.hr_employees
WHERE id IN (35, 44);


-- ============================================================================
-- LEARN: New hires added AFTER older operations
-- ============================================================================
-- The 5 new hires (ids 51-55) were inserted AFTER the salary updates, transfers,
-- and deletions. Their Parquet files are the newest and were definitely NOT
-- orphaned by VACUUM. VACUUM only removes files from PRIOR versions that are
-- no longer referenced.

ASSERT ROW_COUNT = 5
SELECT id, name, department, salary, hire_date
FROM {{zone_name}}.delta_demos.hr_employees
WHERE id BETWEEN 51 AND 55
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 53
ASSERT ROW_COUNT = 53
SELECT * FROM {{zone_name}}.delta_demos.hr_employees;

-- Verify Alice's salary after 15% Engineering raise
ASSERT VALUE salary = 143750.0
SELECT salary FROM {{zone_name}}.delta_demos.hr_employees WHERE id = 1;

-- Verify Leo transferred to Marketing
ASSERT VALUE department = 'Marketing'
SELECT department FROM {{zone_name}}.delta_demos.hr_employees WHERE id = 12;

-- Verify terminated employees (ids 35, 44) are gone
ASSERT VALUE terminated_count = 0
SELECT COUNT(*) AS terminated_count FROM {{zone_name}}.delta_demos.hr_employees WHERE id IN (35, 44);

-- Verify all remaining employees are active
ASSERT VALUE inactive_count = 0
SELECT COUNT(*) AS inactive_count FROM {{zone_name}}.delta_demos.hr_employees WHERE status != 'active';

-- Verify 5 new hires (ids 51-55) are present
ASSERT VALUE new_hires_count = 5
SELECT COUNT(*) AS new_hires_count FROM {{zone_name}}.delta_demos.hr_employees WHERE id BETWEEN 51 AND 55;

-- Verify Engineering department has 12 employees
ASSERT VALUE engineering_count = 12
SELECT COUNT(*) AS engineering_count FROM {{zone_name}}.delta_demos.hr_employees WHERE department = 'Engineering';

-- Verify Sales salary unchanged (no raise applied to Sales)
ASSERT VALUE salary = 105000.0
SELECT salary FROM {{zone_name}}.delta_demos.hr_employees WHERE id = 13;
