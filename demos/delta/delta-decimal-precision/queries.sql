-- ============================================================================
-- Delta Decimal Precision & Arithmetic — Educational Queries
-- ============================================================================
-- WHAT: DECIMAL (fixed-point) types store exact numeric values with a
--       specified precision and scale, unlike DOUBLE which uses IEEE 754
--       floating-point representation.
-- WHY:  Financial calculations require exact arithmetic — a DOUBLE value
--       like 0.1 cannot be represented exactly in binary, leading to
--       rounding errors that compound across millions of transactions.
-- HOW:  Delta stores DECIMAL columns using Parquet's fixed-length byte
--       array encoding, preserving all declared decimal digits exactly
--       through read/write roundtrips.
-- ============================================================================


-- ============================================================================
-- EXPLORE: Inspect the Financial Ledger
-- ============================================================================
-- This ledger uses three different DECIMAL precisions:
--   DECIMAL(15,4)  for amounts   — up to 15 digits, 4 after decimal
--   DECIMAL(18,6)  for balances  — up to 18 digits, 6 after decimal
--   DECIMAL(10,8)  for rates     — up to 10 digits, 8 after decimal
-- Each precision is chosen to match the domain requirements.

ASSERT ROW_COUNT = 10
ASSERT VALUE amount = 10000.0000 WHERE id = 1
ASSERT VALUE balance = 10000.000000 WHERE id = 1
SELECT id, account, description, amount, balance, exchange_rate, currency
FROM {{zone_name}}.delta_demos.financial_ledger
WHERE currency = 'USD'
ORDER BY id
LIMIT 10;


-- ============================================================================
-- LEARN: Roundtrip Precision Fidelity
-- ============================================================================
-- A key benefit of DECIMAL over DOUBLE is that values survive write-then-read
-- cycles without drift. The exchange rate 1.08547321 has 8 decimal places —
-- with DOUBLE, this might round to 1.0854732100000001 after a roundtrip.
-- With DECIMAL(10,8), it is stored and retrieved exactly.
--
-- Let's verify exact precision preservation for exchange rates across
-- all currencies.

-- Verify USD exchange rate is exactly 1.00000000 (no conversion)
ASSERT VALUE min_rate = 1.00000000 WHERE currency = 'USD'
ASSERT VALUE max_rate = 1.00000000 WHERE currency = 'USD'
ASSERT ROW_COUNT = 5
SELECT currency,
       MIN(exchange_rate) AS min_rate,
       MAX(exchange_rate) AS max_rate,
       COUNT(DISTINCT exchange_rate) AS distinct_rates
FROM {{zone_name}}.delta_demos.financial_ledger
GROUP BY currency
ORDER BY currency;


-- ============================================================================
-- LEARN: Cross-Currency Conversion With DECIMAL Arithmetic
-- ============================================================================
-- For EUR and GBP rows, the balance was computed as:
--   balance = ROUND(amount * exchange_rate, 6)
-- Using DECIMAL arithmetic, this multiplication preserves all significant
-- digits without floating-point error. Let's verify the conversion by
-- recomputing from scratch and checking for any drift.

ASSERT ROW_COUNT = 10
ASSERT VALUE stored_balance = 13568.415125 WHERE id = 16
ASSERT VALUE drift = 0.000000 WHERE id = 16
ASSERT VALUE stored_balance = 5721.534000 WHERE id = 21
ASSERT VALUE drift = 0.000000 WHERE id = 21
SELECT id, currency, amount, exchange_rate,
       balance AS stored_balance,
       ROUND(amount * exchange_rate, 6) AS recomputed_balance,
       balance - ROUND(amount * exchange_rate, 6) AS drift
FROM {{zone_name}}.delta_demos.financial_ledger
WHERE currency IN ('EUR', 'GBP')
ORDER BY currency, id;


-- ============================================================================
-- LEARN: Refund Negation Without Floating-Point Artifacts
-- ============================================================================
-- Refund transactions (ids 26-30) had their amounts negated via:
--   SET amount = -amount
-- With DECIMAL, negation is exact: 1875.2500 becomes exactly -1875.2500.
-- With DOUBLE, there is a risk of tiny artifacts like -1875.2500000000002.

-- Verify exactly 5 refund transactions (ids 26-30) with negative amounts
ASSERT ROW_COUNT = 5
ASSERT VALUE amount = -1875.2500 WHERE id = 26
ASSERT VALUE amount = -3100.7500 WHERE id = 29
SELECT id, description, amount, balance
FROM {{zone_name}}.delta_demos.financial_ledger
WHERE amount < 0
ORDER BY id;


-- ============================================================================
-- EXPLORE: Currency Summary — Totals Across the Ledger
-- ============================================================================
-- DECIMAL SUM operations are exact within the declared precision, unlike
-- DOUBLE where summing many values can accumulate rounding errors.

-- Verify 5 distinct currencies and totals match expected sums
ASSERT VALUE total_amount = 128908.1524 WHERE currency = 'USD'
ASSERT VALUE total_balance_usd = 19835.604789 WHERE currency = 'GBP'
ASSERT ROW_COUNT = 5
SELECT currency,
       COUNT(*) AS transaction_count,
       SUM(amount) AS total_amount,
       SUM(balance) AS total_balance_usd
FROM {{zone_name}}.delta_demos.financial_ledger
GROUP BY currency
ORDER BY currency;


-- ============================================================================
-- EXPLORE: Maximum Precision Usage
-- ============================================================================
-- The JPY exchange rate (0.00667800) uses many leading zeros after the
-- decimal point, exercising the full DECIMAL(10,8) scale. The JPY balance
-- for id=33 (5943.423339) uses all 6 decimal places of DECIMAL(18,6).

ASSERT ROW_COUNT = 5
ASSERT VALUE exchange_rate = 0.00667800 WHERE id = 31
ASSERT VALUE balance = 5943.423339 WHERE id = 33
SELECT id, currency, amount, exchange_rate, balance
FROM {{zone_name}}.delta_demos.financial_ledger
WHERE currency = 'JPY'
ORDER BY id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count
ASSERT ROW_COUNT = 40
SELECT * FROM {{zone_name}}.delta_demos.financial_ledger;

-- Verify USD amount sum
ASSERT VALUE usd_sum = 128908.1524
SELECT SUM(amount) AS usd_sum FROM {{zone_name}}.delta_demos.financial_ledger WHERE currency = 'USD';

-- Verify exchange rate precision
ASSERT VALUE exchange_rate = 1.08547321
SELECT exchange_rate FROM {{zone_name}}.delta_demos.financial_ledger WHERE id = 16;

-- Verify computed balance
ASSERT VALUE balance = 13568.415125
SELECT balance FROM {{zone_name}}.delta_demos.financial_ledger WHERE id = 16;

-- Verify refund negative count
ASSERT VALUE refund_count = 5
SELECT COUNT(*) AS refund_count FROM {{zone_name}}.delta_demos.financial_ledger WHERE amount < 0;

-- Verify max decimal scale balance
ASSERT VALUE balance = 5943.423339
SELECT balance FROM {{zone_name}}.delta_demos.financial_ledger WHERE id = 33;

-- Verify currency count
ASSERT VALUE currency_count = 5
SELECT COUNT(DISTINCT currency) AS currency_count FROM {{zone_name}}.delta_demos.financial_ledger;

-- Verify EUR balance sum
ASSERT VALUE eur_balance_sum = 38806.209994
SELECT SUM(balance) AS eur_balance_sum FROM {{zone_name}}.delta_demos.financial_ledger WHERE currency = 'EUR';

-- Verify GBP balance sum
ASSERT VALUE gbp_balance_sum = 19835.604789
SELECT SUM(balance) AS gbp_balance_sum FROM {{zone_name}}.delta_demos.financial_ledger WHERE currency = 'GBP';

-- Verify zero drift across all converted rows (EUR + GBP)
ASSERT VALUE total_drift = 0.000000
SELECT SUM(balance - ROUND(amount * exchange_rate, 6)) AS total_drift FROM {{zone_name}}.delta_demos.financial_ledger WHERE currency IN ('EUR', 'GBP');
