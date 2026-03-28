-- Cleanup: Iceberg V2 — Airline Loyalty Window Analytics

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg.loyalty_members WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg;
