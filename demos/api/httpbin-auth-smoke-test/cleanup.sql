-- ============================================================================
-- Cleanup: Vendor Auth Smoke Test
-- ============================================================================
-- Drops both bronze tables, both endpoints, the connection, the
-- credential, and the explicit credential storage backend. Order is
-- reverse of creation: tables → endpoints → connection → credential →
-- storage → schema. probe_dropped_example was already removed in
-- setup; IF EXISTS on the explicit credential storage keeps this
-- harmless if a prior failed run left half the state behind.
-- ============================================================================

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.httpbin_smoke.uuid_bronze WITH FILES;

DROP EXTERNAL TABLE IF EXISTS {{zone_name}}.httpbin_smoke.headers_bronze WITH FILES;

DROP API ENDPOINT IF EXISTS {{zone_name}}.httpbin_smoke.probe_headers;

DROP API ENDPOINT IF EXISTS {{zone_name}}.httpbin_smoke.probe_uuid;

DROP CONNECTION IF EXISTS httpbin_smoke;

DROP CREDENTIAL IF EXISTS httpbin_smoke_api_key;

-- Remove the per-demo wrapper folder(s) (now empty after table drops)
DROP FOLDER 'httpbin-auth-smoke-test' IF EXISTS IN ZONE {{zone_name}};


DROP SCHEMA IF EXISTS {{zone_name}}.httpbin_smoke;
