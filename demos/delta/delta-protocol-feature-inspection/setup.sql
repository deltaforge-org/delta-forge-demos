-- ============================================================================
-- Delta Protocol Feature Inspection — Setup Script
-- ============================================================================
-- Creates three Delta tables with different TBLPROPERTIES configurations,
-- simulating inherited tables from a departed colleague:
--   1. inherited_plain       — no extra features (baseline Delta)
--   2. inherited_cdc         — enableChangeDataFeed = true
--   3. inherited_constrained — CHECK constraints + enableChangeDataFeed
--
-- The queries.sql file then inspects each table using DESCRIBE DETAIL,
-- SHOW TABLE PROPERTIES, and DESCRIBE HISTORY to determine what protocol
-- features are active and what that means for downstream pipelines.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE 1: inherited_plain — Baseline Delta table (no extra features)
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.inherited_plain (
    id              INT,
    product_name    VARCHAR,
    category        VARCHAR,
    price           DOUBLE,
    in_stock        BOOLEAN,
    last_updated    VARCHAR
) LOCATION 'delta-protocol-feature-inspection/inherited_plain';


INSERT INTO {{zone_name}}.delta_demos.inherited_plain VALUES
    (1,  'Laptop Pro 15',      'electronics',  1299.99, true,  '2025-03-01'),
    (2,  'Wireless Mouse',     'electronics',  29.99,   true,  '2025-03-01'),
    (3,  'Standing Desk',      'furniture',    549.00,  true,  '2025-03-02'),
    (4,  'Monitor Arm',        'furniture',    89.50,   false, '2025-03-02'),
    (5,  'USB-C Hub',          'electronics',  65.00,   true,  '2025-03-03'),
    (6,  'Ergonomic Chair',    'furniture',    899.00,  true,  '2025-03-03'),
    (7,  'Webcam HD',          'electronics',  79.99,   true,  '2025-03-04'),
    (8,  'Desk Lamp',          'furniture',    45.00,   false, '2025-03-04'),
    (9,  'Mechanical Keyboard','electronics',  149.99,  true,  '2025-03-05'),
    (10, 'Cable Organizer',    'accessories',  15.99,   true,  '2025-03-05'),
    (11, 'Laptop Stand',       'accessories',  39.99,   true,  '2025-03-06'),
    (12, 'Noise-Cancel Buds',  'electronics',  199.00,  true,  '2025-03-06'),
    (13, 'Whiteboard 48x36',   'office',       129.00,  false, '2025-03-07'),
    (14, 'Marker Set 12-pk',   'office',       18.50,   true,  '2025-03-07'),
    (15, 'Surge Protector',    'electronics',  34.99,   true,  '2025-03-08');


-- ============================================================================
-- TABLE 2: inherited_cdc — Change Data Feed enabled
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.inherited_cdc (
    id              INT,
    customer_name   VARCHAR,
    email           VARCHAR,
    plan            VARCHAR,
    monthly_spend   DOUBLE,
    signup_date     VARCHAR
) LOCATION 'delta-protocol-feature-inspection/inherited_cdc'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);


INSERT INTO {{zone_name}}.delta_demos.inherited_cdc VALUES
    (1,  'Acme Corp',        'billing@acme.com',       'enterprise', 2500.00, '2024-06-01'),
    (2,  'TechStart Inc',    'admin@techstart.io',     'startup',    199.00,  '2024-07-15'),
    (3,  'Global Logistics',  'ops@globallog.com',     'enterprise', 4200.00, '2024-08-01'),
    (4,  'DataPipe Labs',    'team@datapipe.dev',      'growth',     750.00,  '2024-09-10'),
    (5,  'RetailMax',        'it@retailmax.com',       'enterprise', 3100.00, '2024-10-01'),
    (6,  'CloudNine SaaS',   'support@cloudnine.io',   'growth',     890.00,  '2024-10-20'),
    (7,  'FinServ Partners', 'tech@finserv.com',       'enterprise', 5500.00, '2024-11-01'),
    (8,  'EduLearn',         'admin@edulearn.org',      'startup',    149.00,  '2024-11-15'),
    (9,  'HealthBridge',     'systems@healthbridge.com','enterprise', 6200.00, '2024-12-01'),
    (10, 'SmartFactory',     'iot@smartfactory.ai',     'growth',     1100.00, '2025-01-05'),
    (11, 'MediaFlow',        'eng@mediaflow.tv',        'growth',     980.00,  '2025-01-20'),
    (12, 'SecureNet',        'ops@securenet.io',        'enterprise', 4800.00, '2025-02-01');


-- ============================================================================
-- TABLE 3: inherited_constrained — CHECK constraints + CDC
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.inherited_constrained (
    id              INT,
    item_name       VARCHAR,
    quantity        INT,
    unit_price      DOUBLE,
    discount_pct    DOUBLE,
    warehouse       VARCHAR
) LOCATION 'delta-protocol-feature-inspection/inherited_constrained'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);


INSERT INTO {{zone_name}}.delta_demos.inherited_constrained VALUES
    (1,  'Steel Beam 10m',   200,  85.00,  0.00, 'warehouse-east'),
    (2,  'Copper Wire 50m',  500,  12.50,  5.00, 'warehouse-east'),
    (3,  'PVC Pipe 3m',      800,  4.75,   0.00, 'warehouse-west'),
    (4,  'Concrete Mix 25kg',350,  9.00,  10.00, 'warehouse-east'),
    (5,  'Rebar #4 6m',      600,  7.25,   0.00, 'warehouse-west'),
    (6,  'Glass Panel 1x2m', 120,  45.00,  5.00, 'warehouse-north'),
    (7,  'Insulation Roll',  250,  22.00,  0.00, 'warehouse-west'),
    (8,  'Lumber 2x4 8ft',   1000, 6.50,  15.00, 'warehouse-east'),
    (9,  'Roofing Shingle',  400,  3.25,   0.00, 'warehouse-north'),
    (10, 'Drywall Sheet 4x8',300,  11.00,  0.00, 'warehouse-west');
