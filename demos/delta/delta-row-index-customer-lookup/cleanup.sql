-- Cleanup: Online Retail Customer Lookup with Row-Level Index

DROP INDEX IF EXISTS idx_customer_id ON TABLE {{zone_name}}.delta_demos.customers;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.customers WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-row-index-customer-lookup' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
