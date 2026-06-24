-- Cleanup: Card Payment Ledger - Row-Index Cost / Benefit

DROP INDEX IF EXISTS idx_txn ON TABLE {{zone_name}}.delta_demos.payments;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.payments WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.settlements WITH FILES;

-- Remove the per-demo wrapper folder (now empty after the table drops)
DROP FOLDER 'delta-row-index-payment-ledger' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
