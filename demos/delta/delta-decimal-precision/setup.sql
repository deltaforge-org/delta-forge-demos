-- ============================================================================
-- Delta Decimal Precision & Arithmetic — Setup Script
-- ============================================================================
-- Demonstrates DECIMAL column precision and arithmetic:
--   - DECIMAL(15,4) for transaction amounts
--   - DECIMAL(18,6) for computed balances
--   - DECIMAL(10,8) for exchange rates
--   - Roundtrip read/write fidelity without floating-point drift
--
-- Table created:
--   1. financial_ledger — 40 transactions across 5 currencies
--
-- Operations performed:
--   1. CREATE ZONE + SCHEMA
--   2. CREATE DELTA TABLE with DECIMAL columns
--   3. INSERT 30 rows batch 1 — USD, EUR, GBP transactions
--   5. INSERT 10 rows batch 2 — JPY, CHF edge-case decimals
--   6. UPDATE — exchange rate conversion for 10 rows (EUR + GBP)
--   7. UPDATE — negate amounts for 5 refund transactions
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables — demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: financial_ledger — multinational transaction ledger
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.delta_demos.financial_ledger (
    id                  INT,
    account             VARCHAR,
    description         VARCHAR,
    amount              DECIMAL(15,4),
    balance             DECIMAL(18,6),
    exchange_rate       DECIMAL(10,8),
    currency            VARCHAR
) LOCATION 'delta-decimal-precision/financial_ledger';


-- ============================================================================
-- STEP 3: INSERT batch 1 — 30 rows (USD, EUR, GBP)
-- ============================================================================

-- USD transactions (ids 1-15)
INSERT INTO {{zone_name}}.delta_demos.financial_ledger VALUES
    (1,  'ACC-1001', 'Wire transfer deposit',     10000.0000,    10000.000000, 1.00000000, 'USD'),
    (2,  'ACC-1001', 'Payroll payment',             4500.2500,     4500.250000, 1.00000000, 'USD'),
    (3,  'ACC-1002', 'Client invoice payment',      7250.7500,     7250.750000, 1.00000000, 'USD'),
    (4,  'ACC-1002', 'Office supplies purchase',     325.4900,      325.490000, 1.00000000, 'USD'),
    (5,  'ACC-1003', 'Consulting fee received',    15000.0000,    15000.000000, 1.00000000, 'USD'),
    (6,  'ACC-1003', 'Software license renewal',    3456.7890,     3456.789000, 1.00000000, 'USD'),
    (7,  'ACC-1004', 'Investment return',            1200.0000,     1200.000000, 1.00000000, 'USD'),
    (8,  'ACC-1004', 'Grant funding received',     50000.0000,    50000.000000, 1.00000000, 'USD'),
    (9,  'ACC-1005', 'Equipment purchase',          8750.5000,     8750.500000, 1.00000000, 'USD'),
    (10, 'ACC-1005', 'Service revenue',             6300.0000,     6300.000000, 1.00000000, 'USD'),
    (11, 'ACC-1006', 'Product sales domestic',     22100.0000,    22100.000000, 1.00000000, 'USD'),
    (12, 'ACC-1006', 'Marketing campaign',          1875.2500,     1875.250000, 1.00000000, 'USD'),
    (13, 'ACC-1007', 'Contract milestone',          2340.0000,     2340.000000, 1.00000000, 'USD'),
    (14, 'ACC-1007', 'Warehouse rent',              3500.0000,     3500.000000, 1.00000000, 'USD'),
    (15, 'ACC-1008', 'Dividend income',              875.1234,      875.123400, 1.00000000, 'USD');

-- EUR transactions (ids 16-20) — balance initially zero, computed by UPDATE
INSERT INTO {{zone_name}}.delta_demos.financial_ledger VALUES
    (16, 'ACC-2001', 'EU consulting contract',     12500.0000,        0.000000, 1.08547321, 'EUR'),
    (17, 'ACC-2001', 'Product sales France',        8750.5000,        0.000000, 1.08547321, 'EUR'),
    (18, 'ACC-2002', 'Conference sponsorship',      5000.0000,        0.000000, 1.08547321, 'EUR'),
    (19, 'ACC-2002', 'License royalties',           3300.0000,        0.000000, 1.08547321, 'EUR'),
    (20, 'ACC-2003', 'Training services Berlin',    6200.0000,        0.000000, 1.08547321, 'EUR');

-- GBP transactions (ids 21-25) — balance initially zero, computed by UPDATE
INSERT INTO {{zone_name}}.delta_demos.financial_ledger VALUES
    (21, 'ACC-3001', 'UK client payment',           4500.0000,        0.000000, 1.27145200, 'GBP'),
    (22, 'ACC-3001', 'London office lease',         2250.5000,        0.000000, 1.27145200, 'GBP'),
    (23, 'ACC-3002', 'Manchester project fee',      6800.0000,        0.000000, 1.27145200, 'GBP'),
    (24, 'ACC-3002', 'Catering service',            1100.2500,        0.000000, 1.27145200, 'GBP'),
    (25, 'ACC-3003', 'Annual conference fee',        950.0000,        0.000000, 1.27145200, 'GBP');

-- Refund transactions (ids 26-30, USD) — amounts will be negated by UPDATE
INSERT INTO {{zone_name}}.delta_demos.financial_ledger VALUES
    (26, 'ACC-1009', 'Product return refund',       1875.2500,     1875.250000, 1.00000000, 'USD'),
    (27, 'ACC-1009', 'Service credit issued',       2340.0000,     2340.000000, 1.00000000, 'USD'),
    (28, 'ACC-1010', 'Billing adjustment',           500.0000,      500.000000, 1.00000000, 'USD'),
    (29, 'ACC-1010', 'Duplicate charge reversal',   3100.7500,     3100.750000, 1.00000000, 'USD'),
    (30, 'ACC-1011', 'Warranty claim payout',        750.0000,      750.000000, 1.00000000, 'USD');


-- ============================================================================
-- STEP 5: INSERT batch 2 — 10 rows (JPY, CHF) with edge-case decimals
-- ============================================================================

-- JPY transactions (ids 31-35) — very small exchange rate
INSERT INTO {{zone_name}}.delta_demos.financial_ledger VALUES
    (31, 'ACC-4001', 'Tokyo office revenue',     1250000.0000, 8347.500000, 0.00667800, 'JPY'),
    (32, 'ACC-4001', 'Staff bonuses Tokyo',       350000.0000, 2337.300000, 0.00667800, 'JPY'),
    (33, 'ACC-4002', 'Osaka branch sales',        890000.5000, 5943.423339, 0.00667800, 'JPY'),
    (34, 'ACC-4002', 'Shipping costs domestic',   125000.0000,  834.750000, 0.00667800, 'JPY'),
    (35, 'ACC-4003', 'Tech licensing Japan',     2000000.0000, 13356.000000, 0.00667800, 'JPY');

-- CHF transactions (ids 36-40) — exchange rate close to 1
INSERT INTO {{zone_name}}.delta_demos.financial_ledger VALUES
    (36, 'ACC-5001', 'Swiss consulting',            9500.0000, 10472.277500, 1.10234500, 'CHF'),
    (37, 'ACC-5001', 'Zurich office rent',          2800.0000,  3086.566000, 1.10234500, 'CHF'),
    (38, 'ACC-5002', 'Pharma partnership',         18500.7500, 20394.209259, 1.10234500, 'CHF'),
    (39, 'ACC-5002', 'Lab equipment lease',         4200.0000,  4629.849000, 1.10234500, 'CHF'),
    (40, 'ACC-5003', 'Banking advisory fee',        7650.0000,  8432.939250, 1.10234500, 'CHF');


-- ============================================================================
-- STEP 6: UPDATE — apply exchange rate conversion for EUR + GBP rows
-- Sets balance = ROUND(amount * exchange_rate, 6) for ids 16-25 (10 rows)
-- ============================================================================
-- Expected results:
--   id=16: ROUND(12500.0000 * 1.08547321, 6) = 13568.415125
--   id=17: ROUND(8750.5000  * 1.08547321, 6) =  9498.433324
--   id=18: ROUND(5000.0000  * 1.08547321, 6) =  5427.366050
--   id=19: ROUND(3300.0000  * 1.08547321, 6) =  3582.061593
--   id=20: ROUND(6200.0000  * 1.08547321, 6) =  6729.933902
--   id=21: ROUND(4500.0000  * 1.27145200, 6) =  5721.534000
--   id=22: ROUND(2250.5000  * 1.27145200, 6) =  2861.402726
--   id=23: ROUND(6800.0000  * 1.27145200, 6) =  8645.873600
--   id=24: ROUND(1100.2500  * 1.27145200, 6) =  1398.915063
--   id=25: ROUND(950.0000   * 1.27145200, 6) =  1207.879400
UPDATE {{zone_name}}.delta_demos.financial_ledger
SET balance = ROUND(amount * exchange_rate, 6)
WHERE id BETWEEN 16 AND 25;


-- ============================================================================
-- STEP 7: UPDATE — negate amounts for 5 refund transactions (ids 26-30)
-- ============================================================================
-- Converts positive amounts to negative to represent refunds:
--   id=26:  1875.2500 -> -1875.2500
--   id=27:  2340.0000 -> -2340.0000
--   id=28:   500.0000 ->  -500.0000
--   id=29:  3100.7500 -> -3100.7500
--   id=30:   750.0000 ->  -750.0000
UPDATE {{zone_name}}.delta_demos.financial_ledger
SET amount = -amount
WHERE id BETWEEN 26 AND 30;
