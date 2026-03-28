-- Cleanup: Iceberg V3 UniForm — Investment Portfolio Audit Trail

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.portfolio_holdings_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.portfolio_holdings WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
