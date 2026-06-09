-- ============================================================================
-- Customer Survey Data Cleansing — Setup Script
-- ============================================================================
-- Creates a survey_responses table with 30 rows of customer satisfaction data.
-- Many fields are deliberately NULL or contain sentinel values ('N/A', '') to
-- demonstrate NULL-handling SQL patterns.
--
-- NULL distribution:
--   phone              — 8 NULLs (customers who skipped phone)
--   company            — 6 NULLs + 4 empty strings (freelancers / not provided)
--   satisfaction_rating — 5 NULLs (didn't answer)
--   nps_score          — 3 NULLs (didn't answer)
--   feedback_text      — 6 NULLs (no comment left)
--   referral_source    — 5 NULLs + 4 'N/A' sentinels (unknown origin)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE DELTA
    COMMENT 'Delta tables — customer survey data cleansing demo';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';
-- ============================================================================
-- TABLE: survey_responses — 30 rows with deliberate NULL patterns
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.survey_responses (
    response_id         INT,
    customer_name       VARCHAR,
    email               VARCHAR,
    phone               VARCHAR,
    company             VARCHAR,
    satisfaction_rating INT,
    nps_score           INT,
    feedback_text       VARCHAR,
    response_date       VARCHAR,
    referral_source     VARCHAR
) LOCATION 'delta-null-handling/survey_responses';


-- ============================================================================
-- INSERT: 30 survey responses (rows 1-15)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.survey_responses VALUES
    (1,  'Alice Chen',      'alice@example.com',    '555-0101', 'Acme Corp',    5,    9,    'Excellent product!',           '2024-03-01', 'Google Search'),
    (2,  'Bob Martinez',    'bob@example.com',      NULL,       'TechStart',    4,    7,    'Good overall experience',      '2024-03-01', 'Friend Referral'),
    (3,  'Carol White',     'carol@example.com',    '555-0103', NULL,           NULL,  8,    'Needs better docs',            '2024-03-02', NULL),
    (4,  'David Kim',       'david@example.com',    '555-0104', '',             3,    5,    NULL,                           '2024-03-02', 'N/A'),
    (5,  'Eva Johansson',   'eva@example.com',      NULL,       'Nordic AB',    5,    10,   'Love the new features!',       '2024-03-03', 'LinkedIn'),
    (6,  'Frank Brown',     'frank@example.com',    '555-0106', NULL,           4,    7,    'Solid but pricey',             '2024-03-03', NULL),
    (7,  'Grace Liu',       'grace@example.com',    '555-0107', 'DataFlow Inc', NULL,  NULL, NULL,                           '2024-03-04', 'Google Search'),
    (8,  'Hector Ruiz',     'hector@example.com',   NULL,       '',             2,    3,    'Too many bugs',                '2024-03-04', 'N/A'),
    (9,  'Irene Foster',    'irene@example.com',    '555-0109', 'CloudNine',    5,    9,    'Best tool we have used',       '2024-03-05', 'Conference'),
    (10, 'Jake Thompson',   'jake@example.com',     '555-0110', NULL,           3,    6,    'Average experience',           '2024-03-05', NULL),
    (11, 'Karen Patel',     'karen@example.com',    NULL,       'BrightEdge',   4,    8,    NULL,                           '2024-03-06', 'Friend Referral'),
    (12, 'Leo Nakamura',    'leo@example.com',      '555-0112', '',             NULL,  7,    'Interface is confusing',       '2024-03-06', 'N/A'),
    (13, 'Mia Santos',      'mia@example.com',      '555-0113', 'Globex',       5,    10,   'Absolutely fantastic!',        '2024-03-07', 'Google Search'),
    (14, 'Noah Davis',      'noah@example.com',     NULL,       NULL,           1,    2,    'Very disappointed',            '2024-03-07', NULL),
    (15, 'Olivia Chang',    'olivia@example.com',   '555-0115', 'PixelWorks',   4,    8,    'Great customer support',       '2024-03-08', 'LinkedIn');
-- ============================================================================
-- INSERT: 30 survey responses (rows 16-30)
-- ============================================================================
INSERT INTO {{zone_name}}.delta_demos.survey_responses VALUES
    (16, 'Paul Wilson',     'paul@example.com',     '555-0116', NULL,           NULL,  NULL, NULL,                           '2024-03-08', 'Friend Referral'),
    (17, 'Quinn Adams',     'quinn@example.com',    NULL,       'StartupXYZ',   3,    5,    'Could be better',              '2024-03-09', NULL),
    (18, 'Rosa Hernandez',  'rosa@example.com',     '555-0118', '',             4,    7,    'Good value for money',         '2024-03-09', 'Friend Referral'),
    (19, 'Sam OBrien',      'sam@example.com',      '555-0119', 'IronForge',    5,    9,    'Highly recommend',             '2024-03-10', 'Google Search'),
    (20, 'Tina Kowalski',   'tina@example.com',     NULL,       NULL,           2,    4,    'Slow performance',             '2024-03-10', 'Conference'),
    (21, 'Uma Reddy',       'uma@example.com',      '555-0121', 'VistaTech',    4,    8,    NULL,                           '2024-03-11', 'LinkedIn'),
    (22, 'Victor Sato',     'victor@example.com',   NULL,       'MegaSoft',     NULL,  6,    'Missing key features',         '2024-03-11', 'LinkedIn'),
    (23, 'Wendy Fox',       'wendy@example.com',    '555-0123', 'ByteWise',     3,    NULL, 'Decent product',               '2024-03-12', 'Conference'),
    (24, 'Xavier Lopez',    'xavier@example.com',   '555-0124', 'AlphaOmega',   5,    9,    'Will renew subscription',      '2024-03-12', 'Google Search'),
    (25, 'Yuki Tanaka',     'yuki@example.com',     '555-0125', 'SunriseTech',  4,    7,    'Good but room to improve',     '2024-03-13', 'Conference'),
    (26, 'Zara Ahmed',      'zara@example.com',     '555-0126', 'QuickServe',   3,    5,    'Needs mobile app',             '2024-03-13', 'N/A'),
    (27, 'Aaron Brooks',    'aaron@example.com',    '555-0127', 'NetPrime',     4,    8,    'Reliable and fast',            '2024-03-14', 'Google Search'),
    (28, 'Beth Collins',    'beth@example.com',     '555-0128', 'Meridian',     2,    4,    NULL,                           '2024-03-14', 'Friend Referral'),
    (29, 'Chris Dunn',      'chris@example.com',    '555-0129', 'CoreData',     5,    10,   'Perfect for our team',         '2024-03-15', 'LinkedIn'),
    (30, 'Diana Evans',     'diana@example.com',    '555-0130', 'Pinnacle',     4,    7,    'Happy customer',               '2024-03-15', 'Conference');
