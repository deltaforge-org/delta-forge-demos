-- ============================================================================
-- Delta Complex Types — Educational Queries
-- ============================================================================
-- WHAT: Modelling structured, repeated, and semi-structured data in Delta
--       tables using flat columns, delimited strings, and key=value patterns
-- WHY:  Real-world data is often nested (addresses), repeated (skills lists),
--       or semi-structured (metadata tags) — Delta supports these patterns
--       through column design conventions
-- HOW:  Delta stores all data as Parquet columns. Struct-like data uses
--       multiple related columns (address_street, address_city, etc.),
--       array-like data uses comma-delimited VARCHAR, and map-like data
--       uses key=value pairs in VARCHAR. SQL queries use LIKE patterns and
--       string functions to extract values.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Department Overview
-- ============================================================================
-- The employees table has 40 staff across 5 departments, each with
-- struct-like address fields, a comma-delimited skills column, and
-- a key=value metadata column.

-- Verify Engineering has 11 employees (largest department)
ASSERT VALUE headcount = 11 WHERE department = 'Engineering'
ASSERT ROW_COUNT = 5
SELECT department,
       COUNT(*) AS headcount,
       ROUND(AVG(salary), 0) AS avg_salary,
       ROUND(MIN(salary), 0) AS min_salary,
       ROUND(MAX(salary), 0) AS max_salary
FROM {{zone_name}}.delta_demos.employees
GROUP BY department
ORDER BY headcount DESC;


-- ============================================================================
-- LEARN: Struct Pattern — Multi-Column Address Fields
-- ============================================================================
-- Instead of a single nested 'address' struct, Delta tables commonly model
-- structured data as multiple flat columns: address_street, address_city,
-- address_state, address_zip. This approach:
--   - Enables column-level statistics for data skipping
--   - Allows efficient predicate pushdown (WHERE address_state = 'CA')
--   - Works with any SQL engine without special nested-type support
--
-- Let's find all employees in California using the struct-like address columns.

ASSERT ROW_COUNT = 7
SELECT id, name, department,
       address_street, address_city, address_state, address_zip
FROM {{zone_name}}.delta_demos.employees
WHERE address_state = 'CA'
ORDER BY address_city, name;


-- ============================================================================
-- LEARN: Array Pattern — Comma-Delimited Skills
-- ============================================================================
-- The 'skills' column stores multiple values as a comma-delimited string
-- (e.g., 'python,java,sql'). This is a common pattern when the data source
-- provides lists but the table schema uses flat VARCHAR columns.
-- Querying uses LIKE patterns: '%python%' matches any skills list containing
-- 'python'. For more precise matching, you would use string splitting functions.

ASSERT ROW_COUNT = 6
SELECT id, name, department, skills
FROM {{zone_name}}.delta_demos.employees
WHERE skills LIKE '%python%'
ORDER BY department, name;


-- ============================================================================
-- LEARN: Map Pattern — Key=Value Metadata
-- ============================================================================
-- The 'metadata' column stores key=value pairs (e.g., 'team=backend,role=senior').
-- This pattern is useful for tags, labels, and extensible attributes that
-- vary across rows. Querying uses LIKE patterns to match specific keys or values.
--
-- Let's find all employees on the 'backend' team.

ASSERT ROW_COUNT = 3
SELECT id, name, department, metadata, skills
FROM {{zone_name}}.delta_demos.employees
WHERE metadata LIKE '%team=backend%'
ORDER BY name;


-- ============================================================================
-- EXPLORE: Salary Update Verification
-- ============================================================================
-- Engineering salaries were increased by 15%. The UPDATE operation created
-- new Parquet files with the adjusted values. Let's compare Engineering
-- vs non-Engineering salaries to see the effect.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_salary = 134791 WHERE department = 'Engineering'
SELECT department,
       COUNT(*) AS employees,
       ROUND(AVG(salary), 0) AS avg_salary,
       CASE WHEN department = 'Engineering'
            THEN 'Salaries increased 15%'
            ELSE 'Salaries unchanged' END AS note
FROM {{zone_name}}.delta_demos.employees
GROUP BY department
ORDER BY avg_salary DESC;


-- ============================================================================
-- LEARN: Querying Across Complex Type Patterns
-- ============================================================================
-- This query combines all three patterns: filtering by struct field
-- (address_state), searching array values (skills LIKE), and extracting
-- map entries (metadata LIKE). This shows how flat-column patterns compose
-- naturally in standard SQL.

ASSERT ROW_COUNT = 3
SELECT id, name, department, address_state, skills, metadata, level
FROM {{zone_name}}.delta_demos.employees
WHERE address_state IN ('CA', 'WA', 'OR')
  AND skills LIKE '%python%'
ORDER BY name;


-- ============================================================================
-- EXPLORE: Level Distribution and Management Hierarchy
-- ============================================================================
-- The 'level' column (L2-L7) and 'manager_id' column model an organizational
-- hierarchy. NULL manager_id indicates a top-level manager.

ASSERT ROW_COUNT = 6
SELECT level,
       COUNT(*) AS employees,
       COUNT(*) FILTER (WHERE manager_id IS NULL) AS top_level_managers,
       ROUND(AVG(salary), 0) AS avg_salary
FROM {{zone_name}}.delta_demos.employees
GROUP BY level
ORDER BY level;


-- ============================================================================
-- EXPLORE: Full Employee Listing
-- ============================================================================

ASSERT ROW_COUNT = 40
SELECT id, name, department, salary, address_city, address_state, skills, level
FROM {{zone_name}}.delta_demos.employees
ORDER BY department, name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Verification of row counts, department distribution, complex type queries,
-- and salary integrity after the Engineering raise.

-- Verify total row count
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.employees;

-- Verify Engineering department count
ASSERT VALUE engineering_count = 11
SELECT COUNT(*) AS engineering_count FROM {{zone_name}}.delta_demos.employees WHERE department = 'Engineering';

-- Verify California employee count
ASSERT VALUE california_count = 7
SELECT COUNT(*) AS california_count FROM {{zone_name}}.delta_demos.employees WHERE address_state = 'CA';

-- Verify Python-skilled employee count
ASSERT VALUE python_count = 6
SELECT COUNT(*) AS python_count FROM {{zone_name}}.delta_demos.employees WHERE skills LIKE '%python%';

-- Verify backend team count
ASSERT VALUE backend_count = 3
SELECT COUNT(*) AS backend_count FROM {{zone_name}}.delta_demos.employees WHERE metadata LIKE '%team=backend%';

-- Verify Alice's salary after 15% Engineering raise
ASSERT VALUE salary = 143750.0
SELECT salary FROM {{zone_name}}.delta_demos.employees WHERE id = 1;

-- Verify Sales salary unchanged
ASSERT VALUE salary = 105000.0
SELECT salary FROM {{zone_name}}.delta_demos.employees WHERE id = 9;

-- Verify L5 level count
ASSERT VALUE l5_count = 9
SELECT COUNT(*) AS l5_count FROM {{zone_name}}.delta_demos.employees WHERE level = 'L5';
