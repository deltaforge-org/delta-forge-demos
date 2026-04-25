-- ============================================================================
-- Delta Column Mapping — Setup Script
-- ============================================================================
-- Creates the employee_directory table with column mapping mode 'name' and
-- loads baseline data (40 employees across 6 departments).
--
-- Column mapping decouples logical column names from physical Parquet column
-- names, enabling rename, drop, and add operations without rewriting data files.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- Create table with column mapping TBLPROPERTIES
-- ============================================================================
-- Column mapping mode 'name' requires minReaderVersion=2 and minWriterVersion=5.
-- This enables ALTER TABLE ADD/RENAME/DROP COLUMN to update only the transaction
-- log metadata, without rewriting any Parquet data files.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.employee_directory (
    id          INT,
    full_name   VARCHAR,
    department  VARCHAR,
    title       VARCHAR,
    email       VARCHAR,
    start_date  VARCHAR,
    salary      DOUBLE,
    is_active   INT
) LOCATION 'employee_directory'
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
);


-- ============================================================================
-- Insert 30 employees — Engineering, Sales, Marketing, HR, Finance, Operations
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.employee_directory VALUES
    (1,  'Alice Johnson',      'Engineering', 'Software Engineer',      'alice.johnson@acme.com',      '2021-03-15', 115000.00, 1),
    (2,  'Bob Williams',       'Engineering', 'Software Engineer',      'bob.williams@acme.com',       '2022-01-10', 108000.00, 1),
    (3,  'Carol Davis',        'Engineering', 'DevOps Engineer',        'carol.davis@acme.com',        '2020-07-22', 120000.00, 1),
    (4,  'Daniel Martinez',    'Engineering', 'QA Engineer',            'daniel.martinez@acme.com',    '2023-02-01', 95000.00,  1),
    (5,  'Eva Chen',           'Engineering', 'Software Engineer',      'eva.chen@acme.com',           '2021-09-08', 112000.00, 1),
    (6,  'Frank Thompson',     'Sales',       'Account Executive',      'frank.thompson@acme.com',     '2022-04-12', 85000.00,  1),
    (7,  'Grace Kim',          'Sales',       'Sales Representative',   'grace.kim@acme.com',          '2021-11-30', 78000.00,  1),
    (8,  'Henry Patel',        'Sales',       'Account Executive',      'henry.patel@acme.com',        '2023-05-20', 82000.00,  1),
    (9,  'Isabella Rodriguez', 'Sales',       'Sales Manager',          'isabella.rodriguez@acme.com', '2019-08-05', 105000.00, 1),
    (10, 'Jack O''Brien',      'Sales',       'Sales Representative',   'jack.obrien@acme.com',        '2022-10-15', 76000.00,  1),
    (11, 'Karen Lee',          'Marketing',   'Marketing Analyst',      'karen.lee@acme.com',          '2021-06-14', 88000.00,  1),
    (12, 'Liam Scott',         'Marketing',   'Content Strategist',     'liam.scott@acme.com',         '2022-03-28', 82000.00,  1),
    (13, 'Mia Wong',           'Marketing',   'Marketing Analyst',      'mia.wong@acme.com',           '2023-01-09', 85000.00,  1),
    (14, 'Nathan Brooks',      'Marketing',   'Brand Manager',          'nathan.brooks@acme.com',      '2020-12-01', 95000.00,  1),
    (15, 'Olivia Turner',      'Marketing',   'Marketing Analyst',      'olivia.turner@acme.com',      '2022-08-22', 84000.00,  1),
    (16, 'Patrick Hughes',     'HR',          'HR Specialist',          'patrick.hughes@acme.com',     '2021-04-19', 75000.00,  1),
    (17, 'Quinn Foster',       'HR',          'Recruiter',              'quinn.foster@acme.com',       '2022-07-11', 72000.00,  1),
    (18, 'Rachel Adams',       'HR',          'HR Manager',             'rachel.adams@acme.com',       '2019-10-25', 98000.00,  1),
    (19, 'Samuel Green',       'HR',          'Benefits Coordinator',   'samuel.green@acme.com',       '2023-03-14', 70000.00,  1),
    (20, 'Tanya Baker',        'HR',          'HR Specialist',          'tanya.baker@acme.com',        '2021-12-06', 74000.00,  1),
    (21, 'Umar Shah',          'Finance',     'Financial Analyst',      'umar.shah@acme.com',          '2022-02-17', 92000.00,  1),
    (22, 'Victoria Clark',     'Finance',     'Accountant',             'victoria.clark@acme.com',     '2020-09-30', 85000.00,  1),
    (23, 'William Nelson',     'Finance',     'Financial Analyst',      'william.nelson@acme.com',     '2021-05-23', 90000.00,  1),
    (24, 'Xena Ramirez',       'Finance',     'Controller',             'xena.ramirez@acme.com',       '2019-01-14', 110000.00, 1),
    (25, 'Yuki Tanaka',        'Finance',     'Accountant',             'yuki.tanaka@acme.com',        '2023-06-01', 83000.00,  1),
    (26, 'Zach Morgan',        'Operations',  'Operations Analyst',     'zach.morgan@acme.com',        '2022-05-09', 80000.00,  1),
    (27, 'Amy Fitzgerald',     'Operations',  'Logistics Coordinator',  'amy.fitzgerald@acme.com',     '2021-08-18', 76000.00,  1),
    (28, 'Brian Cooper',       'Operations',  'Operations Manager',     'brian.cooper@acme.com',       '2020-03-11', 102000.00, 1),
    (29, 'Chloe Ward',         'Operations',  'Supply Chain Analyst',   'chloe.ward@acme.com',         '2023-04-07', 78000.00,  1),
    (30, 'Derek Bell',         'Operations',  'Operations Analyst',     'derek.bell@acme.com',         '2022-11-20', 79000.00,  1);


-- ============================================================================
-- Insert 10 more employees
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.employee_directory VALUES
    (31, 'Elena Popov',        'Engineering', 'Software Engineer',      'elena.popov@acme.com',        '2024-01-15', 105000.00, 1),
    (32, 'Felix Grant',        'Engineering', 'Platform Engineer',      'felix.grant@acme.com',        '2024-02-20', 118000.00, 1),
    (33, 'Gina Torres',        'Engineering', 'Software Engineer',      'gina.torres@acme.com',        '2024-03-10', 107000.00, 1),
    (34, 'Howard Lim',         'Sales',       'Account Executive',      'howard.lim@acme.com',         '2024-01-08', 84000.00,  1),
    (35, 'Iris Nakamura',      'Sales',       'Sales Representative',   'iris.nakamura@acme.com',      '2024-04-01', 77000.00,  1),
    (36, 'James Russo',        'HR',          'Recruiter',              'james.russo@acme.com',        '2024-02-12', 73000.00,  1),
    (37, 'Kendra Walsh',       'HR',          'Training Specialist',    'kendra.walsh@acme.com',       '2024-03-25', 71000.00,  1),
    (38, 'Leo Fernandez',      'Finance',     'Financial Analyst',      'leo.fernandez@acme.com',      '2024-01-22', 91000.00,  1),
    (39, 'Monica Dunn',        'Finance',     'Auditor',                'monica.dunn@acme.com',        '2024-04-15', 88000.00,  1),
    (40, 'Nolan Perry',        'Operations',  'Logistics Coordinator',  'nolan.perry@acme.com',        '2024-02-28', 75000.00,  1);

