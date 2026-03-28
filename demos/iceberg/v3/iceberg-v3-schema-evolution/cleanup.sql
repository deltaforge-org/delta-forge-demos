-- Cleanup: Iceberg V3 UniForm — Pharmaceutical Drug Registry Schema Evolution

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.iceberg_demos.drug_registry_iceberg WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.iceberg_demos.drug_registry WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.iceberg_demos;
