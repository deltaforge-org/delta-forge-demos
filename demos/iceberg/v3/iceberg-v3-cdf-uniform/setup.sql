-- ============================================================================
-- Iceberg V3 UniForm — CDF Payment Reconciliation — Setup
-- ============================================================================
-- Creates a Delta table with BOTH CDF and Iceberg UniForm V3 enabled.
-- Every commit generates change data records (for incremental ETL) AND
-- Iceberg V3 metadata (for cross-engine compatibility) simultaneously.
--
-- Dataset: 30 payment transactions across 5 merchants, 3 currencies,
-- and 4 payment methods. All transactions start with a mix of statuses
-- (pending, completed, failed). The queries.sql script performs mutations
-- that exercise CDF tracking alongside V3 metadata generation.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.iceberg_demos
    COMMENT 'Iceberg UniForm demo tables';

-- STEP 2: Create the Delta table with UniForm V3 + CDF
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.iceberg_demos.payment_transactions (
    payment_id   INT,
    merchant     VARCHAR,
    customer     VARCHAR,
    amount       DOUBLE,
    currency     VARCHAR,
    status       VARCHAR,
    pay_method   VARCHAR,
    txn_date     VARCHAR
) LOCATION '{{data_path}}/payment_transactions'
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.universalFormat.icebergVersion' = '3',
    'delta.columnMapping.mode' = 'id',
    'delta.enableChangeDataFeed' = 'true'
);

GRANT ADMIN ON TABLE {{zone_name}}.iceberg_demos.payment_transactions TO USER {{current_user}};

-- STEP 3: Seed 30 payment transactions
INSERT INTO {{zone_name}}.iceberg_demos.payment_transactions VALUES
    (1,  'TechGadgets Inc',     'alex.chen@email.com',       299.99, 'USD', 'pending',   'credit_card',     '2024-06-01'),
    (2,  'TechGadgets Inc',     'maria.silva@email.com',    1499.99, 'USD', 'completed', 'credit_card',     '2024-06-01'),
    (3,  'TechGadgets Inc',     'james.wilson@email.com',     89.50, 'USD', 'pending',   'debit_card',      '2024-06-02'),
    (4,  'TechGadgets Inc',     'yuki.tanaka@email.com',     549.00, 'USD', 'pending',   'credit_card',     '2024-06-02'),
    (5,  'TechGadgets Inc',     'priya.patel@email.com',    2199.99, 'USD', 'completed', 'bank_transfer',   '2024-06-03'),
    (6,  'TechGadgets Inc',     'liam.murphy@email.com',      34.99, 'USD', 'pending',   'digital_wallet',  '2024-06-03'),
    (7,  'FreshMart Foods',     'sophie.martin@email.com',    67.80, 'USD', 'pending',   'debit_card',      '2024-06-01'),
    (8,  'FreshMart Foods',     'omar.hassan@email.com',     123.45, 'USD', 'completed', 'credit_card',     '2024-06-01'),
    (9,  'FreshMart Foods',     'anna.kowalski@email.com',    45.20, 'USD', 'pending',   'digital_wallet',  '2024-06-02'),
    (10, 'FreshMart Foods',     'david.kim@email.com',        89.99, 'USD', 'completed', 'debit_card',      '2024-06-02'),
    (11, 'FreshMart Foods',     'elena.popov@email.com',     210.30, 'USD', 'pending',   'credit_card',     '2024-06-03'),
    (12, 'FreshMart Foods',     'carlos.rodriguez@email.com',156.75, 'USD', 'pending',   'bank_transfer',   '2024-06-03'),
    (13, 'CloudSoft SaaS',      'sarah.jones@email.com',     999.00, 'EUR', 'completed', 'bank_transfer',   '2024-06-01'),
    (14, 'CloudSoft SaaS',      'magnus.berg@email.com',     499.00, 'EUR', 'pending',   'bank_transfer',   '2024-06-01'),
    (15, 'CloudSoft SaaS',      'fatima.ali@email.com',      199.00, 'EUR', 'pending',   'credit_card',     '2024-06-02'),
    (16, 'CloudSoft SaaS',      'lucas.mueller@email.com',  2499.00, 'EUR', 'completed', 'bank_transfer',   '2024-06-02'),
    (17, 'CloudSoft SaaS',      'aisha.ibrahim@email.com',   799.00, 'EUR', 'pending',   'bank_transfer',   '2024-06-03'),
    (18, 'CloudSoft SaaS',      'noah.anderson@email.com',   349.00, 'EUR', 'failed',    'credit_card',     '2024-06-03'),
    (19, 'UrbanStyle Apparel',  'emma.taylor@email.com',     175.50, 'GBP', 'pending',   'digital_wallet',  '2024-06-01'),
    (20, 'UrbanStyle Apparel',  'ravi.sharma@email.com',      89.99, 'GBP', 'completed', 'digital_wallet',  '2024-06-01'),
    (21, 'UrbanStyle Apparel',  'chloe.dubois@email.com',   320.00, 'GBP', 'pending',   'credit_card',     '2024-06-02'),
    (22, 'UrbanStyle Apparel',  'kenji.watanabe@email.com',   45.00, 'GBP', 'pending',   'digital_wallet',  '2024-06-02'),
    (23, 'UrbanStyle Apparel',  'nina.petrova@email.com',    265.75, 'GBP', 'pending',   'digital_wallet',  '2024-06-03'),
    (24, 'UrbanStyle Apparel',  'oliver.smith@email.com',    149.99, 'GBP', 'failed',    'debit_card',      '2024-06-03'),
    (25, 'MedPlus Pharmacy',    'lisa.nguyen@email.com',      42.50, 'USD', 'pending',   'debit_card',      '2024-06-01'),
    (26, 'MedPlus Pharmacy',    'ahmed.khalil@email.com',     78.90, 'USD', 'completed', 'credit_card',     '2024-06-01'),
    (27, 'MedPlus Pharmacy',    'julia.santos@email.com',   156.00, 'USD', 'pending',   'bank_transfer',   '2024-06-02'),
    (28, 'MedPlus Pharmacy',    'michael.brown@email.com',    33.25, 'USD', 'pending',   'digital_wallet',  '2024-06-02'),
    (29, 'MedPlus Pharmacy',    'sofia.garcia@email.com',     95.40, 'USD', 'pending',   'credit_card',     '2024-06-03'),
    (30, 'MedPlus Pharmacy',    'peter.johansson@email.com', 210.00, 'USD', 'pending',   'bank_transfer',   '2024-06-03');
