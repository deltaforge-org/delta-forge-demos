-- ============================================================================
-- Online Retail Customer Lookup with Row-Level Index — Setup Script
-- ============================================================================
-- Builds a customers table for an online retailer's support desk.
-- Customers are inserted across three batches (legacy import, recent
-- signups, VIP migration), creating multiple parquet files. The
-- service desk's lookup workload (point, range, IN) is what motivates
-- the index on customer_id.
--
-- Tables created:
--   1. customers — 60 customers across 3 batches, indexed on customer_id
--
-- Operations performed:
--   1. CREATE DELTA TABLE
--   2. INSERT batch 1 — 30 legacy customers
--   3. INSERT batch 2 — 20 recent signups
--   4. INSERT batch 3 — 10 VIP migrated accounts
--
-- The CREATE INDEX statement lives in queries.sql so the learner sees
-- it side by side with the queries it accelerates.
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: customers — high-cardinality customer_id, ideal for row-level index
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.customers (
    customer_id   BIGINT,
    email         VARCHAR,
    full_name     VARCHAR,
    tier          VARCHAR,
    region        VARCHAR,
    signup_date   VARCHAR,
    lifetime_spend DOUBLE,
    active        BOOLEAN
) LOCATION 'customers';


-- Batch 1 — 30 legacy customers (customer_id 1000..1029)
INSERT INTO {{zone_name}}.delta_demos.customers VALUES
    (1000, 'amelia.brooks@example.com',     'Amelia Brooks',      'standard', 'EU',  '2021-04-12',  482.30, true),
    (1001, 'noah.harper@example.com',       'Noah Harper',        'standard', 'EU',  '2021-04-13',  319.95, true),
    (1002, 'olivia.scott@example.com',      'Olivia Scott',       'standard', 'NA',  '2021-04-15', 1284.50, true),
    (1003, 'liam.morgan@example.com',       'Liam Morgan',        'standard', 'NA',  '2021-04-16',   75.20, true),
    (1004, 'emma.fischer@example.com',      'Emma Fischer',       'standard', 'EU',  '2021-04-19',  942.10, true),
    (1005, 'oliver.dawson@example.com',     'Oliver Dawson',      'standard', 'NA',  '2021-04-20',  157.40, false),
    (1006, 'ava.holloway@example.com',      'Ava Holloway',       'standard', 'AP',  '2021-04-22',  201.85, true),
    (1007, 'william.tanaka@example.com',    'William Tanaka',     'standard', 'AP',  '2021-04-25',  865.00, true),
    (1008, 'sophia.weiss@example.com',      'Sophia Weiss',       'standard', 'EU',  '2021-05-01',   58.99, true),
    (1009, 'james.coleman@example.com',     'James Coleman',      'standard', 'NA',  '2021-05-03',  412.75, true),
    (1010, 'isabella.romero@example.com',   'Isabella Romero',    'standard', 'NA',  '2021-05-05',  723.40, true),
    (1011, 'benjamin.hartley@example.com',  'Benjamin Hartley',   'standard', 'EU',  '2021-05-08',  138.60, false),
    (1012, 'mia.jensen@example.com',        'Mia Jensen',         'standard', 'EU',  '2021-05-10',  349.25, true),
    (1013, 'lucas.reilly@example.com',      'Lucas Reilly',       'standard', 'NA',  '2021-05-12',  916.80, true),
    (1014, 'charlotte.bauer@example.com',   'Charlotte Bauer',    'standard', 'EU',  '2021-05-15',  221.35, true),
    (1015, 'mason.osullivan@example.com',   'Mason O Sullivan',   'standard', 'EU',  '2021-05-18',  611.50, true),
    (1016, 'amelia.kowalski@example.com',   'Amelia Kowalski',    'standard', 'EU',  '2021-05-20',   89.00, true),
    (1017, 'logan.nakamura@example.com',    'Logan Nakamura',     'standard', 'AP',  '2021-05-23',  455.95, true),
    (1018, 'harper.delgado@example.com',    'Harper Delgado',     'standard', 'NA',  '2021-05-25',  167.20, true),
    (1019, 'ethan.fitzgerald@example.com',  'Ethan Fitzgerald',   'standard', 'NA',  '2021-05-28',  802.65, false),
    (1020, 'evelyn.lindgren@example.com',   'Evelyn Lindgren',    'standard', 'EU',  '2021-06-01',  524.40, true),
    (1021, 'aiden.barnes@example.com',      'Aiden Barnes',       'standard', 'NA',  '2021-06-03',   45.99, true),
    (1022, 'abigail.santos@example.com',    'Abigail Santos',     'standard', 'NA',  '2021-06-05',  988.10, true),
    (1023, 'sebastian.dubois@example.com',  'Sebastian Dubois',   'standard', 'EU',  '2021-06-08',  234.80, true),
    (1024, 'ella.murakami@example.com',     'Ella Murakami',      'standard', 'AP',  '2021-06-10',  371.55, true),
    (1025, 'henry.bauerlein@example.com',   'Henry Bauerlein',    'standard', 'EU',  '2021-06-12',  152.00, true),
    (1026, 'scarlett.ng@example.com',       'Scarlett Ng',        'standard', 'AP',  '2021-06-15',  679.30, true),
    (1027, 'jack.peterson@example.com',     'Jack Peterson',      'standard', 'NA',  '2021-06-18',  118.45, false),
    (1028, 'aria.zimmerman@example.com',    'Aria Zimmerman',     'standard', 'EU',  '2021-06-20',  390.70, true),
    (1029, 'owen.castillo@example.com',     'Owen Castillo',      'standard', 'NA',  '2021-06-22',  840.25, true);


-- Batch 2 — 20 recent signups (customer_id 1030..1049)
INSERT INTO {{zone_name}}.delta_demos.customers VALUES
    (1030, 'luna.iverson@example.com',      'Luna Iverson',       'standard', 'EU',  '2024-09-02',   24.50, true),
    (1031, 'leo.brennan@example.com',       'Leo Brennan',        'standard', 'NA',  '2024-09-04',   89.95, true),
    (1032, 'grace.okafor@example.com',      'Grace Okafor',       'standard', 'EU',  '2024-09-07',  142.30, true),
    (1033, 'theo.santiago@example.com',     'Theo Santiago',      'standard', 'NA',  '2024-09-09',   58.20, true),
    (1034, 'zoe.kowalczyk@example.com',     'Zoe Kowalczyk',      'standard', 'EU',  '2024-09-12',   67.40, true),
    (1035, 'kai.henderson@example.com',     'Kai Henderson',      'standard', 'AP',  '2024-09-14',  198.85, true),
    (1036, 'nora.bachmann@example.com',     'Nora Bachmann',      'standard', 'EU',  '2024-09-17',   33.00, true),
    (1037, 'silas.duncan@example.com',      'Silas Duncan',       'standard', 'NA',  '2024-09-19',   72.15, true),
    (1038, 'iris.lefevre@example.com',      'Iris Lefevre',       'standard', 'EU',  '2024-09-21',  104.60, true),
    (1039, 'caleb.osterman@example.com',    'Caleb Osterman',     'standard', 'NA',  '2024-09-24',   18.99, false),
    (1040, 'ruby.holland@example.com',      'Ruby Holland',       'standard', 'EU',  '2024-09-27',   45.40, true),
    (1041, 'milo.archer@example.com',       'Milo Archer',        'standard', 'NA',  '2024-09-29',  221.50, true),
    (1042, 'piper.macedo@example.com',      'Piper Macedo',       'standard', 'NA',  '2024-10-02',   91.85, true),
    (1043, 'rowan.dietrich@example.com',    'Rowan Dietrich',     'standard', 'EU',  '2024-10-04',   12.30, true),
    (1044, 'sage.ostrowski@example.com',    'Sage Ostrowski',     'standard', 'EU',  '2024-10-07',   76.95, true),
    (1045, 'nash.brennan@example.com',      'Nash Brennan',       'standard', 'NA',  '2024-10-09',  154.20, true),
    (1046, 'wren.kovacs@example.com',       'Wren Kovacs',        'standard', 'EU',  '2024-10-12',   38.00, true),
    (1047, 'orion.maeda@example.com',       'Orion Maeda',        'standard', 'AP',  '2024-10-15',  267.30, true),
    (1048, 'juno.callaghan@example.com',    'Juno Callaghan',     'standard', 'EU',  '2024-10-17',   59.55, true),
    (1049, 'beck.thornton@example.com',     'Beck Thornton',      'standard', 'NA',  '2024-10-20',   83.40, true);


-- Batch 3 — 10 VIP migrated accounts (customer_id 9001..9010)
-- (The row-level index itself is created in queries.sql so the
--  CREATE INDEX statement is part of the lesson, not buried in setup.)
INSERT INTO {{zone_name}}.delta_demos.customers VALUES
    (9001, 'eleanor.whitfield@example.com', 'Eleanor Whitfield',  'vip',      'EU',  '2018-03-15', 12480.00, true),
    (9002, 'gideon.aldridge@example.com',   'Gideon Aldridge',    'vip',      'NA',  '2018-04-22', 18900.50, true),
    (9003, 'celeste.varga@example.com',     'Celeste Varga',      'vip',      'EU',  '2018-05-30', 24355.75, true),
    (9004, 'roman.delacroix@example.com',   'Roman Delacroix',    'vip',      'EU',  '2018-07-08', 16240.20, true),
    (9005, 'serena.tachibana@example.com',  'Serena Tachibana',   'vip',      'AP',  '2018-08-19', 31075.40, true),
    (9006, 'declan.fairfield@example.com',  'Declan Fairfield',   'vip',      'NA',  '2018-10-04', 14820.85, true),
    (9007, 'aurora.holloway@example.com',   'Aurora Holloway',    'vip',      'NA',  '2018-11-22', 22190.30, true),
    (9008, 'phoenix.berenson@example.com',  'Phoenix Berenson',   'vip',      'EU',  '2019-01-15', 19560.95, true),
    (9009, 'tarquin.morley@example.com',    'Tarquin Morley',     'vip',      'EU',  '2019-03-02', 27840.10, true),
    (9010, 'isolde.varnavas@example.com',   'Isolde Varnavas',    'vip',      'AP',  '2019-04-18', 21305.65, true);
