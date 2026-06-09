-- ============================================================================
-- Delta VACUUM — Cleanup Orphaned Files — Setup Script
-- ============================================================================
-- Prepares a table with multiple DML operations that create orphaned files:
--   1. CREATE DELTA TABLE
--   2. INSERT — 50 employees
--   3. UPDATE — salary adjustments for Engineering (creates old files)
--   4. UPDATE — department transfers (creates more old files)
--   5. DELETE — remove 2 terminated employees (creates tombstones)
--   6. INSERT — add 5 new hires
--
-- After setup, the table has 53 rows and many orphaned Parquet files.
-- The queries.sql script demonstrates VACUUM to clean them up.
--
-- Tables created:
--   1. hr_employees — 53 final rows after multiple DML operations
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: hr_employees — HR workforce management
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.hr_employees (
    id              INT,
    name            VARCHAR,
    department      VARCHAR,
    salary          DOUBLE,
    status          VARCHAR,
    hire_date       VARCHAR
) LOCATION 'delta-vacuum-cleanup/hr_employees';


-- STEP 2: Insert 50 employees across 5 departments
INSERT INTO {{zone_name}}.delta_demos.hr_employees VALUES
    (1,  'Alice Chen',       'Engineering',  125000.00, 'active', '2020-01-15'),
    (2,  'Bob Martinez',     'Engineering',  115000.00, 'active', '2020-03-01'),
    (3,  'Carol Williams',   'Engineering',  135000.00, 'active', '2019-06-10'),
    (4,  'David Kim',        'Engineering',  105000.00, 'active', '2021-02-01'),
    (5,  'Eva Johnson',      'Engineering',  120000.00, 'active', '2020-09-15'),
    (6,  'Frank Lopez',      'Engineering',  110000.00, 'active', '2021-04-01'),
    (7,  'Grace Park',       'Engineering',  128000.00, 'active', '2019-11-20'),
    (8,  'Henry Brown',      'Engineering',  118000.00, 'active', '2020-07-01'),
    (9,  'Irene Davis',      'Engineering',  132000.00, 'active', '2019-03-15'),
    (10, 'Jack Wilson',      'Engineering',  108000.00, 'active', '2022-01-10'),
    (11, 'Karen Miller',     'Sales',         95000.00, 'active', '2020-05-01'),
    (12, 'Leo Zhang',        'Sales',         88000.00, 'active', '2021-01-15'),
    (13, 'Maria Garcia',     'Sales',        105000.00, 'active', '2019-08-01'),
    (14, 'Nick Patel',       'Sales',         78000.00, 'active', '2022-03-01'),
    (15, 'Olivia Taylor',    'Sales',         92000.00, 'active', '2020-10-15'),
    (16, 'Peter Adams',      'Sales',         85000.00, 'active', '2021-06-01'),
    (17, 'Quinn Roberts',    'Sales',         98000.00, 'active', '2019-12-01'),
    (18, 'Rachel Lee',       'Sales',         82000.00, 'active', '2022-05-15'),
    (19, 'Sam Thompson',     'Sales',         90000.00, 'active', '2020-02-01'),
    (20, 'Tina Anderson',    'Sales',         87000.00, 'active', '2021-09-01'),
    (21, 'Uma Krishnan',     'Marketing',     92000.00, 'active', '2020-04-01'),
    (22, 'Victor Nguyen',    'Marketing',     86000.00, 'active', '2021-02-15'),
    (23, 'Wendy Clark',      'Marketing',     98000.00, 'active', '2019-07-01'),
    (24, 'Xavier Reed',      'Marketing',     80000.00, 'active', '2022-01-01'),
    (25, 'Yuki Tanaka',      'Marketing',     94000.00, 'active', '2020-08-15'),
    (26, 'Zara Hussein',     'Marketing',     88000.00, 'active', '2021-05-01'),
    (27, 'Aaron Scott',      'Marketing',     76000.00, 'active', '2022-07-01'),
    (28, 'Beth Morgan',      'Marketing',     90000.00, 'active', '2020-11-01'),
    (29, 'Chris Turner',     'Marketing',     84000.00, 'active', '2021-08-15'),
    (30, 'Diana Foster',     'Marketing',     96000.00, 'active', '2019-10-01'),
    (31, 'Ed Price',         'HR',            82000.00, 'active', '2020-06-01'),
    (32, 'Fiona Campbell',   'HR',            95000.00, 'active', '2019-09-15'),
    (33, 'George White',     'HR',            78000.00, 'active', '2021-03-01'),
    (34, 'Hannah Brooks',    'HR',            88000.00, 'active', '2020-12-01'),
    (35, 'Ian Cooper',       'HR',            72000.00, 'active', '2022-04-01'),
    (36, 'Julia Stewart',    'Finance',      105000.00, 'active', '2019-05-01'),
    (37, 'Kyle Bennett',     'Finance',       95000.00, 'active', '2020-01-01'),
    (38, 'Laura Ramirez',    'Finance',       88000.00, 'active', '2021-07-01'),
    (39, 'Mike Sullivan',    'Finance',      110000.00, 'active', '2019-02-15'),
    (40, 'Nina Hoffman',     'Finance',       82000.00, 'active', '2022-02-01'),
    (41, 'Oscar Diaz',       'Finance',       92000.00, 'active', '2020-09-01'),
    (42, 'Paula Jensen',     'Finance',       86000.00, 'active', '2021-11-01'),
    (43, 'Ray Collins',      'Finance',       98000.00, 'active', '2019-04-01'),
    (44, 'Sara Evans',       'Finance',       78000.00, 'active', '2022-06-01'),
    (45, 'Tom Baker',        'Finance',      102000.00, 'active', '2020-03-15'),
    (46, 'Ursula Grant',     'Engineering',  122000.00, 'active', '2020-05-15'),
    (47, 'Vince Howard',     'Sales',         91000.00, 'active', '2021-10-01'),
    (48, 'Wanda James',      'Marketing',     83000.00, 'active', '2022-08-01'),
    (49, 'Youssef Ali',      'HR',            85000.00, 'active', '2021-01-01'),
    (50, 'Zoe Mitchell',     'Finance',       90000.00, 'active', '2020-07-15');


-- ============================================================================
-- STEP 3: UPDATE — 15% salary increase for Engineering (creates old files)
-- ============================================================================
-- Engineering employees: 1-10, 46 = 11 employees
-- Delta uses copy-on-write: every file containing an Engineering row is
-- rewritten with new salary values. The old files become orphaned.
UPDATE {{zone_name}}.delta_demos.hr_employees
SET salary = ROUND(salary * 1.15, 2)
WHERE department = 'Engineering';


-- ============================================================================
-- STEP 4: UPDATE — transfer 3 employees to new departments (more old files)
-- ============================================================================
-- id=12 (Leo, Sales->Marketing), id=33 (George, HR->Sales), id=42 (Paula, Finance->HR)
-- Each UPDATE rewrites affected files, orphaning the previous versions.
UPDATE {{zone_name}}.delta_demos.hr_employees
SET department = 'Marketing'
WHERE id = 12;

UPDATE {{zone_name}}.delta_demos.hr_employees
SET department = 'Sales'
WHERE id = 33;

UPDATE {{zone_name}}.delta_demos.hr_employees
SET department = 'HR'
WHERE id = 42;


-- ============================================================================
-- STEP 5: DELETE — remove 2 terminated employees (creates tombstones)
-- ============================================================================
-- id=35 (Ian Cooper, HR) and id=44 (Sara Evans, Finance) terminated.
-- Delta rewrites affected files WITHOUT these rows and adds "remove" actions
-- to the transaction log. The old files become orphaned.
UPDATE {{zone_name}}.delta_demos.hr_employees
SET status = 'terminated'
WHERE id IN (35, 44);

DELETE FROM {{zone_name}}.delta_demos.hr_employees
WHERE status = 'terminated';


-- ============================================================================
-- STEP 6: INSERT — add 5 new hires
-- ============================================================================
-- These rows create brand-new Parquet files. They will NOT be orphaned
-- because no subsequent operation supersedes them.
INSERT INTO {{zone_name}}.delta_demos.hr_employees
SELECT * FROM (VALUES
    (51, 'Amy Richards',    'Engineering',  115000.00, 'active', '2025-03-01'),
    (52, 'Ben Carlson',     'Sales',         88000.00, 'active', '2025-03-01'),
    (53, 'Cathy Dunn',      'Marketing',     82000.00, 'active', '2025-03-01'),
    (54, 'Derek Flores',    'HR',            78000.00, 'active', '2025-03-01'),
    (55, 'Elena Vasquez',   'Finance',       92000.00, 'active', '2025-03-01')
) AS t(id, name, department, salary, status, hire_date);
