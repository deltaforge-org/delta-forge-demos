-- Cleanup: Static Model Deck Ingestion

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.model_v1 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.model_v2 WITH FILES;
DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.static_modelling.all_cells WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.static_modelling.static_model WITH FILES;

DROP SCHEMA IF EXISTS {{zone_name}}.static_modelling;
