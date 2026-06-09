-- Cleanup: E-Commerce Order Tracking — Indexed UPDATE / DELETE / MERGE

DROP INDEX IF EXISTS idx_tracking ON TABLE {{zone_name}}.delta_demos.shipment_orders;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.shipment_orders WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'delta-row-index-order-tracking' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
