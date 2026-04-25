-- ============================================================================
-- Delta Change Data Feed — Track Row-Level Changes — Setup Script
-- ============================================================================
-- Creates a CDF-enabled customer_accounts table with 40 baseline rows (V0).
-- The queries.sql script performs DML operations (V1-V4) and demonstrates
-- how CDF tracks each change.
--
-- Tables created:
--   1. customer_accounts — CDF-enabled, 40 initial rows
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: customer_accounts — Customer account management with CDF
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customer_accounts (
    id              INT,
    name            VARCHAR,
    email           VARCHAR,
    tier            VARCHAR,
    balance         DOUBLE,
    status          VARCHAR,
    created_date    VARCHAR
) LOCATION 'customer_accounts'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');


-- V0: Insert 40 customer accounts
INSERT INTO {{zone_name}}.delta_demos.customer_accounts VALUES
    (1,  'Alice Morgan',     'alice.morgan@mail.com',     'silver', 5200.00,  'active', '2023-01-05'),
    (2,  'Bob Fischer',      'bob.fischer@mail.com',      'silver', 3100.00,  'active', '2023-01-10'),
    (3,  'Carol Reeves',     'carol.reeves@mail.com',     'bronze', 1800.00,  'active', '2023-01-15'),
    (4,  'Daniel Ortiz',     'daniel.ortiz@mail.com',     'silver', 4500.00,  'active', '2023-02-01'),
    (5,  'Emily Watson',     'emily.watson@mail.com',     'bronze', 950.00,   'active', '2023-02-10'),
    (6,  'Frank Dubois',     'frank.dubois@mail.com',     'silver', 6700.00,  'active', '2023-02-15'),
    (7,  'Grace Nakamura',   'grace.nakamura@mail.com',   'bronze', 2200.00,  'active', '2023-03-01'),
    (8,  'Henry Kowalski',   'henry.kowalski@mail.com',   'silver', 8100.00,  'active', '2023-03-10'),
    (9,  'Irene Svensson',   'irene.svensson@mail.com',   'bronze', 1500.00,  'active', '2023-03-15'),
    (10, 'James Okafor',     'james.okafor@mail.com',     'silver', 3900.00,  'active', '2023-04-01'),
    (11, 'Karen Petrova',    'karen.petrova@mail.com',    'bronze', 2800.00,  'active', '2023-04-10'),
    (12, 'Leo Andersen',     'leo.andersen@mail.com',     'silver', 7200.00,  'active', '2023-04-15'),
    (13, 'Maria Gutierrez',  'maria.gutierrez@mail.com',  'bronze', 1200.00,  'active', '2023-05-01'),
    (14, 'Nathan Brooks',    'nathan.brooks@mail.com',    'silver', 5800.00,  'active', '2023-05-10'),
    (15, 'Olivia Henriksen', 'olivia.henriksen@mail.com', 'silver', 4100.00,  'active', '2023-05-15'),
    (16, 'Patrick Lemoine',  'patrick.lemoine@mail.com',  'bronze', 900.00,   'active', '2023-06-01'),
    (17, 'Quinn Tanaka',     'quinn.tanaka@mail.com',     'silver', 6300.00,  'active', '2023-06-10'),
    (18, 'Rachel Kim',       'rachel.kim@mail.com',       'bronze', 2100.00,  'active', '2023-06-15'),
    (19, 'Samuel Rivera',    'samuel.rivera@mail.com',    'silver', 4800.00,  'active', '2023-07-01'),
    (20, 'Tara McBride',     'tara.mcbride@mail.com',     'bronze', 1600.00,  'active', '2023-07-10'),
    (21, 'Umar Hassan',      'umar.hassan@mail.com',      'silver', 5500.00,  'active', '2023-07-15'),
    (22, 'Vera Johansson',   'vera.johansson@mail.com',   'bronze', 3300.00,  'active', '2023-08-01'),
    (23, 'William Cheng',    'william.cheng@mail.com',    'silver', 7800.00,  'active', '2023-08-10'),
    (24, 'Xia Huang',        'xia.huang@mail.com',        'bronze', 1400.00,  'active', '2023-08-15'),
    (25, 'Yusuf Demir',      'yusuf.demir@mail.com',      'silver', 4200.00,  'active', '2023-09-01'),
    (26, 'Zara Patel',       'zara.patel@mail.com',       'bronze', 2600.00,  'active', '2023-09-10'),
    (27, 'Adam Clarke',      'adam.clarke@mail.com',      'silver', 5900.00,  'active', '2023-09-15'),
    (28, 'Beatrice Novak',   'beatrice.novak@mail.com',   'bronze', 1100.00,  'active', '2023-10-01'),
    (29, 'Carlos Mendez',    'carlos.mendez@mail.com',    'silver', 6500.00,  'active', '2023-10-10'),
    (30, 'Diana Frost',      'diana.frost@mail.com',      'bronze', 1900.00,  'active', '2023-10-15'),
    (31, 'Erik Lindgren',    'erik.lindgren@mail.com',     'silver', 8500.00,  'active', '2023-11-01'),
    (32, 'Fatima Al-Rashid', 'fatima.alrashid@mail.com',  'bronze', 2400.00,  'active', '2023-11-10'),
    (33, 'Gabriel Costa',    'gabriel.costa@mail.com',     'silver', 7100.00,  'active', '2023-11-15'),
    (34, 'Helen Park',       'helen.park@mail.com',        'bronze', 1700.00,  'active', '2023-12-01'),
    (35, 'Ivan Volkov',      'ivan.volkov@mail.com',       'silver', 4600.00,  'active', '2023-12-10'),
    (36, 'Julia Santos',     'julia.santos@mail.com',      'bronze', 3000.00,  'active', '2023-12-15'),
    (37, 'Kevin O''Brien',   'kevin.obrien@mail.com',     'silver', 5100.00,  'active', '2024-01-05'),
    (38, 'Linda Nguyen',     'linda.nguyen@mail.com',     'bronze', 2700.00,  'active', '2024-01-10'),
    (39, 'Marco Bianchi',    'marco.bianchi@mail.com',    'silver', 6800.00,  'active', '2024-01-15'),
    (40, 'Nadia Kozlov',     'nadia.kozlov@mail.com',     'bronze', 1300.00,  'active', '2024-02-01');

