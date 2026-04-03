-- ============================================================================
-- Iceberg UniForm CRUD Lifecycle — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table and seeds it with 20 employees.
-- Each subsequent DML operation in queries.sql generates both a Delta
-- version and an Iceberg snapshot, allowing time travel across both formats.
--
-- Dataset: 20 employees across 4 departments (Engineering, Sales, Marketing,
-- Finance) with columns: id, name, department, title, salary, is_active.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm enabled
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.employees (
    id          INT,
    name        VARCHAR,
    department  VARCHAR,
    title       VARCHAR,
    salary      DOUBLE,
    is_active   BOOLEAN
) LOCATION '{{data_path}}/employees'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.employees TO USER {{current_user}};

-- STEP 3: Seed 20 employees (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.employees VALUES
    (1,  'Alice Chen',       'Engineering', 'Senior Engineer',      135000.00, true),
    (2,  'Bob Martinez',     'Engineering', 'Staff Engineer',       155000.00, true),
    (3,  'Carol Wang',       'Engineering', 'Tech Lead',            170000.00, true),
    (4,  'David Kim',        'Engineering', 'Junior Engineer',      95000.00,  true),
    (5,  'Eve Johnson',      'Engineering', 'Principal Engineer',   190000.00, true),
    (6,  'Frank Lee',        'Sales',       'Account Executive',    85000.00,  true),
    (7,  'Grace Park',       'Sales',       'Sales Manager',        110000.00, true),
    (8,  'Henry Brown',      'Sales',       'Sales Rep',            72000.00,  true),
    (9,  'Iris Tanaka',      'Sales',       'Sales Director',       140000.00, true),
    (10, 'Jack Wilson',      'Sales',       'Account Executive',    88000.00,  true),
    (11, 'Karen White',      'Marketing',   'Marketing Manager',    115000.00, true),
    (12, 'Leo Garcia',       'Marketing',   'Content Lead',         95000.00,  true),
    (13, 'Mia Patel',        'Marketing',   'SEO Specialist',       78000.00,  true),
    (14, 'Nick Thompson',    'Marketing',   'Brand Director',       135000.00, true),
    (15, 'Olivia Davis',     'Marketing',   'Marketing Analyst',    82000.00,  true),
    (16, 'Paul Robinson',    'Finance',     'Controller',           130000.00, true),
    (17, 'Quinn Adams',      'Finance',     'Financial Analyst',    90000.00,  true),
    (18, 'Rachel Scott',     'Finance',     'CFO',                  200000.00, true),
    (19, 'Sam Harris',       'Finance',     'Accountant',           75000.00,  true),
    (20, 'Tina Brooks',      'Finance',     'Audit Manager',        105000.00, true);
