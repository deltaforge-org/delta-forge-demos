-- ============================================================================
-- Cleanup: Fleet Dispatch Network — AUTO REFRESH CSR demo
-- ============================================================================
-- Drop order: paired graphs → delta tables (WITH FILES) → schema.
-- The zone is shared with other demos and is not dropped here.

DROP GRAPH IF EXISTS {{zone_name}}.fleet_dispatch.dispatch_batch;
DROP GRAPH IF EXISTS {{zone_name}}.fleet_dispatch.dispatch_live;

DROP DELTA TABLE IF EXISTS {{zone_name}}.fleet_dispatch.routes WITH FILES;
DROP DELTA TABLE IF EXISTS {{zone_name}}.fleet_dispatch.hubs   WITH FILES;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'graph-auto-refresh-csr' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.fleet_dispatch;
