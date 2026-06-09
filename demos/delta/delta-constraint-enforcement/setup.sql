-- ============================================================================
-- Delta Constraint Enforcement — NOT NULL & CHECK — Setup Script
-- ============================================================================
-- Creates the validated_employees table with baseline data.
-- All rows satisfy:
--   - age BETWEEN 18 AND 70
--   - salary > 0
--   - rating BETWEEN 0.0 AND 5.0
--   - id is NOT NULL
--
-- Tables created:
--   1. validated_employees — 50 rows of baseline data
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: validated_employees — Employees with constraint-valid data
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.validated_employees (
    id          INT,
    name        VARCHAR,
    email       VARCHAR,
    age         INT,
    salary      DOUBLE,
    rating      DOUBLE,
    department  VARCHAR,
    hire_date   VARCHAR
) LOCATION 'delta-constraint-enforcement/validated_employees';


-- INSERT 50 employees — all satisfying constraints:
--   age: 18-70, salary > 0, rating: 0.0-5.0, id NOT NULL
INSERT INTO {{zone_name}}.delta_demos.validated_employees VALUES
    (1,  'Alice Morgan',     'alice.morgan@corp.com',     32, 95000.00,  4.5, 'Engineering',  '2020-01-15'),
    (2,  'Bob Fischer',      'bob.fischer@corp.com',      28, 78000.00,  3.8, 'Engineering',  '2021-03-01'),
    (3,  'Carol Reeves',     'carol.reeves@corp.com',     45, 115000.00, 4.9, 'Engineering',  '2015-06-10'),
    (4,  'Daniel Ortiz',     'daniel.ortiz@corp.com',     38, 88000.00,  3.5, 'Engineering',  '2019-02-01'),
    (5,  'Emily Watson',     'emily.watson@corp.com',     25, 72000.00,  4.2, 'Engineering',  '2022-09-15'),
    (6,  'Frank Dubois',     'frank.dubois@corp.com',     52, 125000.00, 4.7, 'Sales',        '2012-04-01'),
    (7,  'Grace Nakamura',   'grace.nakamura@corp.com',   34, 82000.00,  3.9, 'Sales',        '2020-08-15'),
    (8,  'Henry Kowalski',   'henry.kowalski@corp.com',   41, 92000.00,  4.1, 'Sales',        '2018-11-01'),
    (9,  'Irene Svensson',   'irene.svensson@corp.com',   29, 75000.00,  3.6, 'Sales',        '2021-05-10'),
    (10, 'James Okafor',     'james.okafor@corp.com',     55, 110000.00, 4.4, 'Sales',        '2010-01-15'),
    (11, 'Karen Petrova',    'karen.petrova@corp.com',    36, 98000.00,  4.3, 'Marketing',    '2019-07-01'),
    (12, 'Leo Andersen',     'leo.andersen@corp.com',     22, 65000.00,  3.2, 'Marketing',    '2023-01-10'),
    (13, 'Maria Gutierrez',  'maria.gutierrez@corp.com',  48, 105000.00, 4.6, 'Marketing',    '2014-03-15'),
    (14, 'Nathan Brooks',    'nathan.brooks@corp.com',    31, 85000.00,  3.7, 'Marketing',    '2020-10-01'),
    (15, 'Olivia Henriksen', 'olivia.henriksen@corp.com', 27, 70000.00,  4.0, 'Marketing',    '2022-02-15'),
    (16, 'Patrick Lemoine',  'patrick.lemoine@corp.com',  60, 130000.00, 4.8, 'HR',           '2008-05-01'),
    (17, 'Quinn Tanaka',     'quinn.tanaka@corp.com',     33, 82000.00,  3.4, 'HR',           '2020-12-01'),
    (18, 'Rachel Kim',       'rachel.kim@corp.com',       39, 90000.00,  4.2, 'HR',           '2018-06-15'),
    (19, 'Samuel Rivera',    'samuel.rivera@corp.com',    26, 68000.00,  3.1, 'HR',           '2023-04-01'),
    (20, 'Tara McBride',     'tara.mcbride@corp.com',     44, 100000.00, 4.5, 'HR',           '2016-09-01'),
    (21, 'Umar Hassan',      'umar.hassan@corp.com',      50, 118000.00, 4.3, 'Finance',      '2011-02-15'),
    (22, 'Vera Johansson',   'vera.johansson@corp.com',   35, 95000.00,  4.0, 'Finance',      '2019-08-01'),
    (23, 'William Cheng',    'william.cheng@corp.com',    42, 108000.00, 4.6, 'Finance',      '2015-11-01'),
    (24, 'Xia Huang',        'xia.huang@corp.com',        24, 62000.00,  3.3, 'Finance',      '2023-07-15'),
    (25, 'Yusuf Demir',      'yusuf.demir@corp.com',      58, 135000.00, 4.9, 'Finance',      '2009-01-10'),
    (26, 'Zara Patel',       'zara.patel@corp.com',       30, 80000.00,  3.8, 'Engineering',  '2021-06-01'),
    (27, 'Adam Clarke',      'adam.clarke@corp.com',      37, 92000.00,  4.1, 'Engineering',  '2019-03-15'),
    (28, 'Beatrice Novak',   'beatrice.novak@corp.com',   46, 112000.00, 4.4, 'Sales',        '2013-10-01'),
    (29, 'Carlos Mendez',    'carlos.mendez@corp.com',    23, 64000.00,  3.0, 'Sales',        '2023-09-01'),
    (30, 'Diana Frost',      'diana.frost@corp.com',      40, 98000.00,  4.2, 'Marketing',    '2017-04-15'),
    (31, 'Erik Lindgren',    'erik.lindgren@corp.com',     18, 55000.00,  2.8, 'Marketing',    '2024-01-15'),
    (32, 'Fatima Al-Rashid', 'fatima.alrashid@corp.com',  65, 140000.00, 5.0, 'Engineering',  '2005-06-01'),
    (33, 'Gabriel Costa',    'gabriel.costa@corp.com',     43, 102000.00, 4.3, 'HR',           '2016-02-01'),
    (34, 'Helen Park',       'helen.park@corp.com',        19, 52000.00,  2.5, 'HR',           '2024-03-01'),
    (35, 'Ivan Volkov',      'ivan.volkov@corp.com',       54, 120000.00, 4.7, 'Finance',      '2011-08-15'),
    (36, 'Julia Santos',     'julia.santos@corp.com',      29, 76000.00,  3.6, 'Finance',      '2022-01-10'),
    (37, 'Kevin O''Brien',   'kevin.obrien@corp.com',     47, 110000.00, 4.5, 'Engineering',  '2014-07-01'),
    (38, 'Linda Nguyen',     'linda.nguyen@corp.com',     33, 84000.00,  3.9, 'Sales',        '2020-05-15'),
    (39, 'Marco Bianchi',    'marco.bianchi@corp.com',    70, 145000.00, 5.0, 'Finance',      '2003-01-01'),
    (40, 'Nadia Kozlov',     'nadia.kozlov@corp.com',     21, 58000.00,  2.9, 'Marketing',    '2024-02-01'),
    (41, 'Oscar Fernandez',  'oscar.fernandez@corp.com',  56, 125000.00, 4.6, 'Engineering',  '2010-09-15'),
    (42, 'Priya Sharma',     'priya.sharma@corp.com',     35, 90000.00,  4.0, 'HR',           '2019-11-01'),
    (43, 'Remy Laurent',     'remy.laurent@corp.com',     28, 72000.00,  3.4, 'Sales',        '2022-06-01'),
    (44, 'Sofia Rossi',      'sofia.rossi@corp.com',      41, 96000.00,  4.2, 'Marketing',    '2017-08-15'),
    (45, 'Tariq Mansour',    'tariq.mansour@corp.com',    38, 88000.00,  3.7, 'Finance',      '2020-04-01'),
    (46, 'Uma Reddy',        'uma.reddy@corp.com',        49, 115000.00, 4.8, 'Engineering',  '2012-12-01'),
    (47, 'Viktor Novak',     'viktor.novak@corp.com',     26, 68000.00,  3.3, 'HR',           '2023-05-15'),
    (48, 'Wendy Chang',      'wendy.chang@corp.com',      62, 132000.00, 4.9, 'Sales',        '2007-03-01'),
    (49, 'Xavier Reed',      'xavier.reed@corp.com',      31, 82000.00,  3.8, 'Marketing',    '2021-07-01'),
    (50, 'Yasmin Ali',       'yasmin.ali@corp.com',        20, 56000.00,  2.7, 'Finance',      '2024-01-01');

