-- ==========================================================================
-- Mira's Mercantile — Retail Analytics Chart Gallery (CLEANUP)
-- ==========================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.sales_daily WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.retail.stock_prices WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.retail;
DROP ZONE IF EXISTS {{zone_name}};
