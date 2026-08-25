-- Cleanup: Static Model Deck Ingestion

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.norne WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.norne_all WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.sector WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.static_modelling.static_model WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.static_modelling;
