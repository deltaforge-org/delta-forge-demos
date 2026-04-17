-- ==========================================================================
-- Demo: Customer Loyalty Program — Bloom Filters with UniForm
-- Feature: BLOOM FILTER COLUMNS on UniForm Iceberg tables
-- ==========================================================================

-- --------------------------------------------------------------------------
-- Zone & Schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos COMMENT 'Bloom filters with UniForm';

-- --------------------------------------------------------------------------
-- Members Table — Bloom Filters + UniForm
-- --------------------------------------------------------------------------

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.members (
    member_id      INT,
    full_name      VARCHAR,
    tier           VARCHAR,
    points         INT,
    lifetime_spend DECIMAL(10,2),
    join_date      DATE
) LOCATION '{{data_path}}/members'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
)
BLOOM FILTER COLUMNS (member_id, full_name);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.members TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- Seed Data — 40 loyalty members across 4 tiers
-- --------------------------------------------------------------------------

INSERT INTO {{zone_name}}.iceberg_demos.members VALUES
    (1,  'Sarah Thompson',      'Bronze',   1250,  340.50,   '2023-06-15'),
    (2,  'Michael Brown',       'Silver',   4800,  1250.00,  '2022-11-20'),
    (3,  'Jennifer Lee',        'Gold',     12500, 3800.75,  '2021-03-08'),
    (4,  'David Garcia',        'Platinum', 28000, 8500.00,  '2020-01-15'),
    (5,  'Lisa Wang',           'Bronze',   800,   220.00,   '2024-02-10'),
    (6,  'Robert Martinez',     'Silver',   5200,  1450.25,  '2022-08-05'),
    (7,  'Amanda Johnson',      'Gold',     15000, 4200.50,  '2021-05-22'),
    (8,  'James Wilson',        'Platinum', 32000, 9800.00,  '2019-09-14'),
    (9,  'Emily Davis',         'Bronze',   950,   280.75,   '2024-01-03'),
    (10, 'Christopher Taylor',  'Silver',   3900,  1100.00,  '2023-04-18'),
    (11, 'Jessica Anderson',    'Gold',     11000, 3200.00,  '2021-07-30'),
    (12, 'Daniel Thomas',       'Platinum', 25500, 7600.50,  '2020-04-25'),
    (13, 'Ashley Jackson',      'Bronze',   1500,  410.00,   '2023-09-12'),
    (14, 'Matthew White',       'Silver',   6100,  1680.00,  '2022-06-08'),
    (15, 'Stephanie Harris',    'Gold',     13800, 4050.25,  '2021-02-14'),
    (16, 'Andrew Clark',        'Platinum', 30000, 9200.00,  '2019-12-01'),
    (17, 'Nicole Lewis',        'Bronze',   600,   180.50,   '2024-03-20'),
    (18, 'Joshua Robinson',     'Silver',   4500,  1320.75,  '2022-10-15'),
    (19, 'Lauren Walker',       'Gold',     14200, 4100.00,  '2021-04-09'),
    (20, 'Ryan Hall',           'Platinum', 27500, 8100.25,  '2020-02-28'),
    (21, 'Megan Allen',         'Bronze',   1100,  300.00,   '2023-08-06'),
    (22, 'Kevin Young',         'Silver',   5800,  1580.50,  '2022-07-19'),
    (23, 'Rachel King',         'Gold',     16000, 4600.00,  '2021-01-11'),
    (24, 'Brian Wright',        'Platinum', 35000, 10500.00, '2019-06-23'),
    (25, 'Christina Scott',     'Bronze',   700,   200.25,   '2024-04-05'),
    (26, 'Tyler Green',         'Silver',   4200,  1200.00,  '2023-01-30'),
    (27, 'Samantha Baker',      'Gold',     11500, 3400.75,  '2021-09-17'),
    (28, 'Patrick Adams',       'Platinum', 29000, 8800.00,  '2020-03-12'),
    (29, 'Heather Nelson',      'Bronze',   1800,  490.50,   '2023-05-28'),
    (30, 'Sean Carter',         'Silver',   3600,  1050.00,  '2023-03-14'),
    (31, 'Kimberly Mitchell',   'Gold',     12000, 3600.50,  '2021-06-05'),
    (32, 'Jason Perez',         'Bronze',   450,   130.00,   '2024-05-10'),
    (33, 'Amber Roberts',       'Silver',   5500,  1520.25,  '2022-09-22'),
    (34, 'Brandon Turner',      'Gold',     13500, 3950.00,  '2021-08-15'),
    (35, 'Tiffany Phillips',    'Platinum', 26000, 7900.75,  '2020-05-19'),
    (36, 'Dustin Campbell',     'Bronze',   2000,  550.00,   '2023-07-01'),
    (37, 'Melissa Parker',      'Silver',   4900,  1380.50,  '2022-12-08'),
    (38, 'Eric Evans',          'Gold',     17000, 5100.00,  '2020-11-25'),
    (39, 'Courtney Edwards',    'Platinum', 31000, 9500.25,  '2019-08-30'),
    (40, 'Derek Collins',       'Bronze',   1350,  370.75,   '2023-10-22');
