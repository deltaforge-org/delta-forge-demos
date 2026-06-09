-- ============================================================================
-- Delta Deletion Vectors — Setup Script
-- ============================================================================
-- Creates the web_sessions table and inserts 60 baseline rows across 3 regions.
-- The queries.sql file demonstrates the full DV lifecycle: DELETE, UPDATE,
-- DESCRIBE DETAIL, OPTIMIZE, DESCRIBE HISTORY, VERSION AS OF, and VACUUM.
--
-- Tables created:
--   1. web_sessions — 60 initial rows (20 per region)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: web_sessions — Web analytics session tracking
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.web_sessions (
    id          INT,
    session_id  VARCHAR,
    user_agent  VARCHAR,
    page_views  INT,
    duration_ms INT,
    status      VARCHAR,
    region      VARCHAR,
    started_at  VARCHAR
) LOCATION 'delta-deletion-vectors/web_sessions';


-- Region 1: us-east (20 sessions)
INSERT INTO {{zone_name}}.delta_demos.web_sessions VALUES
    (1,  'sess-us-001', 'Mozilla/5.0 Chrome/120',    7,  12000,  'active',    'us-east', '2024-06-01 08:00:00'),
    (2,  'sess-us-002', 'Mozilla/5.0 Firefox/121',   5,  9500,   'active',    'us-east', '2024-06-01 08:10:00'),
    (3,  'sess-us-003', 'Mozilla/5.0 Safari/17',     12, 25000,  'active',    'us-east', '2024-06-01 08:20:00'),
    (4,  'sess-us-004', 'Mozilla/5.0 Edge/120',      3,  6000,   'active',    'us-east', '2024-06-01 08:30:00'),
    (5,  'sess-us-005', 'Mozilla/5.0 Chrome/120',    9,  18000,  'active',    'us-east', '2024-06-01 08:40:00'),
    (6,  'sess-us-006', 'Mozilla/5.0 Firefox/121',   4,  8500,   'active',    'us-east', '2024-06-01 08:50:00'),
    (7,  'sess-us-007', 'Mozilla/5.0 Safari/17',     6,  14000,  'active',    'us-east', '2024-06-01 09:00:00'),
    (8,  'sess-us-008', 'Mozilla/5.0 Chrome/120',    8,  20000,  'active',    'us-east', '2024-06-01 09:10:00'),
    (9,  'sess-us-009', 'Mozilla/5.0 Edge/120',      11, 32000,  'completed', 'us-east', '2024-06-01 09:20:00'),
    (10, 'sess-us-010', 'Mozilla/5.0 Chrome/120',    15, 45000,  'completed', 'us-east', '2024-06-01 09:30:00'),
    (11, 'sess-us-011', 'Mozilla/5.0 Firefox/121',   8,  22000,  'completed', 'us-east', '2024-06-01 09:40:00'),
    (12, 'sess-us-012', 'Mozilla/5.0 Safari/17',     6,  18000,  'completed', 'us-east', '2024-06-01 09:50:00'),
    (13, 'sess-us-013', 'Mozilla/5.0 Chrome/120',    10, 30000,  'completed', 'us-east', '2024-06-01 10:00:00'),
    (14, 'sess-us-014', 'Mozilla/5.0 Edge/120',      1,  800,    'bounced',   'us-east', '2024-06-01 10:10:00'),
    (15, 'sess-us-015', 'Mozilla/5.0 Chrome/120',    1,  500,    'bounced',   'us-east', '2024-06-01 10:20:00'),
    (16, 'sess-us-016', 'Mozilla/5.0 Firefox/121',   1,  1200,   'bounced',   'us-east', '2024-06-01 10:30:00'),
    (17, 'sess-us-017', 'Mozilla/5.0 Safari/17',     1,  300,    'bounced',   'us-east', '2024-06-01 10:40:00'),
    (18, 'sess-us-018', 'Mozilla/5.0 Chrome/120',    4,  15000,  'expired',   'us-east', '2024-01-10 14:00:00'),
    (19, 'sess-us-019', 'Mozilla/5.0 Edge/120',      3,  11000,  'expired',   'us-east', '2024-01-15 16:00:00'),
    (20, 'sess-us-020', 'Mozilla/5.0 Firefox/121',   5,  19000,  'expired',   'us-east', '2024-03-20 10:00:00');

-- Region 2: eu-west (20 sessions)
INSERT INTO {{zone_name}}.delta_demos.web_sessions VALUES
    (21, 'sess-eu-001', 'Mozilla/5.0 Chrome/120',    6,  11000,  'active',    'eu-west', '2024-06-01 09:00:00'),
    (22, 'sess-eu-002', 'Mozilla/5.0 Firefox/121',   8,  16000,  'active',    'eu-west', '2024-06-01 09:10:00'),
    (23, 'sess-eu-003', 'Mozilla/5.0 Safari/17',     4,  7500,   'active',    'eu-west', '2024-06-01 09:20:00'),
    (24, 'sess-eu-004', 'Mozilla/5.0 Edge/120',      10, 22000,  'active',    'eu-west', '2024-06-01 09:30:00'),
    (25, 'sess-eu-005', 'Mozilla/5.0 Chrome/120',    3,  5500,   'active',    'eu-west', '2024-06-01 09:40:00'),
    (26, 'sess-eu-006', 'Mozilla/5.0 Firefox/121',   7,  13000,  'active',    'eu-west', '2024-06-01 09:50:00'),
    (27, 'sess-eu-007', 'Mozilla/5.0 Safari/17',     5,  10000,  'active',    'eu-west', '2024-06-01 10:00:00'),
    (28, 'sess-eu-008', 'Mozilla/5.0 Chrome/120',    14, 42000,  'completed', 'eu-west', '2024-06-01 10:10:00'),
    (29, 'sess-eu-009', 'Mozilla/5.0 Edge/120',      9,  27000,  'completed', 'eu-west', '2024-06-01 10:20:00'),
    (30, 'sess-eu-010', 'Mozilla/5.0 Chrome/120',    12, 35000,  'completed', 'eu-west', '2024-06-01 10:30:00'),
    (31, 'sess-eu-011', 'Mozilla/5.0 Firefox/121',   7,  20000,  'completed', 'eu-west', '2024-06-01 10:40:00'),
    (32, 'sess-eu-012', 'Mozilla/5.0 Safari/17',     11, 33000,  'completed', 'eu-west', '2024-06-01 10:50:00'),
    (33, 'sess-eu-013', 'Mozilla/5.0 Chrome/120',    1,  600,    'bounced',   'eu-west', '2024-06-01 11:00:00'),
    (34, 'sess-eu-014', 'Mozilla/5.0 Edge/120',      1,  900,    'bounced',   'eu-west', '2024-06-01 11:10:00'),
    (35, 'sess-eu-015', 'Mozilla/5.0 Firefox/121',   1,  400,    'bounced',   'eu-west', '2024-06-01 11:20:00'),
    (36, 'sess-eu-016', 'Mozilla/5.0 Safari/17',     3,  9000,   'expired',   'eu-west', '2024-01-05 12:00:00'),
    (37, 'sess-eu-017', 'Mozilla/5.0 Chrome/120',    5,  14000,  'expired',   'eu-west', '2024-01-12 15:00:00'),
    (38, 'sess-eu-018', 'Mozilla/5.0 Edge/120',      2,  7000,   'expired',   'eu-west', '2024-01-28 11:00:00'),
    (39, 'sess-eu-019', 'Mozilla/5.0 Firefox/121',   6,  17000,  'expired',   'eu-west', '2024-03-15 13:00:00'),
    (40, 'sess-eu-020', 'Mozilla/5.0 Chrome/120',    4,  12000,  'expired',   'eu-west', '2024-06-01 14:00:00');

-- Region 3: ap-south (20 sessions)
INSERT INTO {{zone_name}}.delta_demos.web_sessions VALUES
    (41, 'sess-ap-001', 'Mozilla/5.0 Chrome/120',    5,  10000,  'active',    'ap-south', '2024-06-01 05:00:00'),
    (42, 'sess-ap-002', 'Mozilla/5.0 Firefox/121',   9,  21000,  'active',    'ap-south', '2024-06-01 05:10:00'),
    (43, 'sess-ap-003', 'Mozilla/5.0 Safari/17',     3,  6500,   'active',    'ap-south', '2024-06-01 05:20:00'),
    (44, 'sess-ap-004', 'Mozilla/5.0 Edge/120',      7,  15000,  'active',    'ap-south', '2024-06-01 05:30:00'),
    (45, 'sess-ap-005', 'Mozilla/5.0 Chrome/120',    4,  8000,   'active',    'ap-south', '2024-06-01 05:40:00'),
    (46, 'sess-ap-006', 'Mozilla/5.0 Firefox/121',   13, 40000,  'completed', 'ap-south', '2024-06-01 05:50:00'),
    (47, 'sess-ap-007', 'Mozilla/5.0 Safari/17',     10, 28000,  'completed', 'ap-south', '2024-06-01 06:00:00'),
    (48, 'sess-ap-008', 'Mozilla/5.0 Chrome/120',    8,  24000,  'completed', 'ap-south', '2024-06-01 06:10:00'),
    (49, 'sess-ap-009', 'Mozilla/5.0 Edge/120',      6,  16000,  'completed', 'ap-south', '2024-06-01 06:20:00'),
    (50, 'sess-ap-010', 'Mozilla/5.0 Chrome/120',    11, 31000,  'completed', 'ap-south', '2024-06-01 06:30:00'),
    (51, 'sess-ap-011', 'Mozilla/5.0 Firefox/121',   9,  26000,  'completed', 'ap-south', '2024-06-01 06:40:00'),
    (52, 'sess-ap-012', 'Mozilla/5.0 Safari/17',     1,  700,    'bounced',   'ap-south', '2024-06-01 06:50:00'),
    (53, 'sess-ap-013', 'Mozilla/5.0 Chrome/120',    1,  1100,   'bounced',   'ap-south', '2024-06-01 07:00:00'),
    (54, 'sess-ap-014', 'Mozilla/5.0 Edge/120',      1,  350,    'bounced',   'ap-south', '2024-06-01 07:10:00'),
    (55, 'sess-ap-015', 'Mozilla/5.0 Firefox/121',   2,  5000,   'expired',   'ap-south', '2024-01-02 03:00:00'),
    (56, 'sess-ap-016', 'Mozilla/5.0 Safari/17',     4,  12000,  'expired',   'ap-south', '2024-01-18 07:00:00'),
    (57, 'sess-ap-017', 'Mozilla/5.0 Chrome/120',    3,  8500,   'expired',   'ap-south', '2024-01-25 09:00:00'),
    (58, 'sess-ap-018', 'Mozilla/5.0 Edge/120',      5,  13000,  'expired',   'ap-south', '2024-02-10 06:00:00'),
    (59, 'sess-ap-019', 'Mozilla/5.0 Chrome/120',    7,  19000,  'expired',   'ap-south', '2024-04-01 08:00:00'),
    (60, 'sess-ap-020', 'Mozilla/5.0 Firefox/121',   6,  16000,  'expired',   'ap-south', '2024-06-10 04:00:00');

