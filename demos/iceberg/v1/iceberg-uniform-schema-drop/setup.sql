-- ============================================================================
-- Iceberg UniForm Drop Columns (GDPR PII Removal) — Setup
-- ============================================================================
-- Creates a UniForm-enabled Delta table with 9 columns including PII fields
-- (email, phone, ip_address) and seeds 20 user profiles. The queries.sql
-- file progressively drops PII columns to demonstrate GDPR compliance.
--
-- Dataset: 20 users across 4 countries (US, UK, DE, JP) with subscription
-- tiers: free (7), pro (8), enterprise (5).
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create table with UniForm and column mapping
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.user_profiles (
    user_id            INT,
    username           VARCHAR,
    email              VARCHAR,
    phone              VARCHAR,
    ip_address         VARCHAR,
    country            VARCHAR,
    signup_date        VARCHAR,
    last_login         VARCHAR,
    subscription_tier  VARCHAR
) LOCATION '{{data_path}}/user_profiles'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '2',
    'delta.columnMapping.mode' = 'id'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.user_profiles TO USER {{current_user}};

-- STEP 3: Seed 20 user profiles (Version 1, Iceberg Snapshot 1)
INSERT INTO {{zone_name}}.iceberg_demos.user_profiles VALUES
    -- US users (5)
    (1,  'jdoe',       'jdoe@example.com',       '+1-555-0101',       '192.168.1.10',  'US', '2024-01-15', '2025-03-01', 'pro'),
    (2,  'asmith',     'asmith@example.com',      '+1-555-0102',       '192.168.1.20',  'US', '2024-02-10', '2025-03-10', 'free'),
    (3,  'mjohnson',   'mjohnson@example.com',    '+1-555-0103',       '10.0.0.5',      'US', '2024-03-05', '2025-02-20', 'enterprise'),
    (4,  'kbrown',     'kbrown@example.com',      '+1-555-0104',       '10.0.0.15',     'US', '2024-04-12', '2025-03-15', 'pro'),
    (5,  'lwilson',    'lwilson@example.com',     '+1-555-0105',       '172.16.0.1',    'US', '2024-05-20', '2025-01-30', 'free'),
    -- UK users (5)
    (6,  'ethomas',    'ethomas@example.co.uk',   '+44-20-7946-0101',  '10.10.1.1',     'UK', '2024-01-20', '2025-03-05', 'pro'),
    (7,  'rjones',     'rjones@example.co.uk',    '+44-20-7946-0102',  '10.10.1.2',     'UK', '2024-02-15', '2025-02-28', 'enterprise'),
    (8,  'sdavies',    'sdavies@example.co.uk',   '+44-20-7946-0103',  '10.10.1.3',     'UK', '2024-03-10', '2025-03-12', 'free'),
    (9,  'pwilliams',  'pwilliams@example.co.uk', '+44-20-7946-0104',  '10.10.1.4',     'UK', '2024-04-22', '2025-03-18', 'pro'),
    (10, 'htaylor',    'htaylor@example.co.uk',   '+44-20-7946-0105',  '10.10.1.5',     'UK', '2024-06-01', '2025-02-10', 'free'),
    -- DE users (5)
    (11, 'mmueller',   'mmueller@example.de',     '+49-30-1234-5601',  '172.20.0.1',    'DE', '2024-01-25', '2025-03-08', 'enterprise'),
    (12, 'kschneider', 'kschneider@example.de',   '+49-30-1234-5602',  '172.20.0.2',    'DE', '2024-02-20', '2025-03-14', 'pro'),
    (13, 'jfischer',   'jfischer@example.de',     '+49-30-1234-5603',  '172.20.0.3',    'DE', '2024-03-15', '2025-02-25', 'free'),
    (14, 'aweber',     'aweber@example.de',        '+49-30-1234-5604',  '172.20.0.4',    'DE', '2024-04-30', '2025-03-20', 'enterprise'),
    (15, 'lbecker',    'lbecker@example.de',      '+49-30-1234-5605',  '172.20.0.5',    'DE', '2024-05-15', '2025-01-15', 'pro'),
    -- JP users (5)
    (16, 'ytanaka',    'ytanaka@example.jp',      '+81-3-1234-5601',   '192.168.10.1',  'JP', '2024-02-01', '2025-03-02', 'free'),
    (17, 'ksuzuki',    'ksuzuki@example.jp',      '+81-3-1234-5602',   '192.168.10.2',  'JP', '2024-03-01', '2025-03-11', 'pro'),
    (18, 'tsato',      'tsato@example.jp',        '+81-3-1234-5603',   '192.168.10.3',  'JP', '2024-04-10', '2025-02-22', 'enterprise'),
    (19, 'mwatanabe',  'mwatanabe@example.jp',    '+81-3-1234-5604',   '192.168.10.4',  'JP', '2024-05-25', '2025-03-16', 'free'),
    (20, 'hito',       'hito@example.jp',         '+81-3-1234-5605',   '192.168.10.5',  'JP', '2024-06-15', '2025-03-22', 'pro');
