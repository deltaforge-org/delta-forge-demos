-- ============================================================================
-- Cleanup: Frankfurter FX Rate Catalog
-- ============================================================================
-- Drop the renamed endpoint (not the original stub name — it was
-- RENAMEd in setup), the unchanged latest_eur_basket endpoint, then
-- the connection. historical_stub DROP-IF-EXISTS is a safety net: if
-- a prior failed run left the original name behind, this still cleans
-- up without erroring.
-- ============================================================================

DROP DELTA TABLE IF EXISTS {{zone_name}}.frankfurter_fx.fx_rates_silver WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.frankfurter_fx.fx_rates_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.frankfurter_fx.latest_eur_basket;

DROP API ENDPOINT IF EXISTS {{zone_name}}.frankfurter_fx.euro_launch_day_rates;

-- Defensive — if a prior run failed before the RENAME, the original
-- name may still exist. IF EXISTS keeps this harmless when the RENAME
-- did complete.
DROP API ENDPOINT IF EXISTS {{zone_name}}.frankfurter_fx.historical_stub;

DROP CONNECTION IF EXISTS frankfurter_fx;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'frankfurter-fx-rate-catalog' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.frankfurter_fx;
