-- ============================================================================
-- Demo: Vendor Auth Smoke Test — api_key_header
-- Feature: CREATE CREDENTIAL on the default OS Keychain, auth_mode =
--          'api_key_header', X-API-Key echo round-trip proof,
--          standalone DROP API ENDPOINT
-- ============================================================================
--
-- Real-world story: a DevOps team vets the DeltaForge REST API ingest
-- pipeline against httpbin.org before wiring up a new real vendor. The
-- goal is to prove the credential, auth-mode, and endpoint plumbing
-- all work correctly in isolation — an always-green smoke test the
-- team can run anytime to confirm the machinery is intact.
--
-- Pipeline:
--   1. SHOW CREDENTIAL STORAGES — surfaces the always-on OS Keychain
--                                  default backend (no CREATE needed;
--                                  OS Keychain is auto-provisioned).
--   2. CREDENTIAL         — stored on the default OS Keychain backend.
--   3. Zone + schema
--   4. Connection         — auth_mode = 'api_key_header' binds the
--                            credential so every request gets
--                            `X-API-Key: <secret>` appended.
--   5. Endpoint 1         — /headers (echoes all request headers back)
--   6. Endpoint 2         — /uuid    (returns a random UUID)
--   7. Endpoint 3         — /anything — created then IMMEDIATELY
--                            DROPped as a DROP API ENDPOINT syntax demo
--   8. SHOW API ENDPOINTS — verifies the drop took effect (only 2
--                            survive)
--   9. INVOKE both        — one page each
--   10. External tables   — flatten the /headers response (specifically
--                            the X-API-Key echo) and the /uuid response
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. SHOW CREDENTIAL STORAGES — audit the default OS Keychain backend
-- --------------------------------------------------------------------------
-- OS Keychain is the always-on default credential storage — it's
-- auto-provisioned at catalog migration time and cannot be
-- user-created. CREATE CREDENTIAL STORAGE is reserved for cloud
-- backends (AZURE, AWS, GCP). This SHOW surfaces the default
-- backend so the security reviewer can see it catalogued.

SHOW CREDENTIAL STORAGES;

-- --------------------------------------------------------------------------
-- 2. CREDENTIAL — stored on the default OS Keychain
-- --------------------------------------------------------------------------
-- Omitting `IN CREDENTIAL STORAGE` routes the secret to the default
-- OS Keychain backend. The secret material is inner-sealed into the
-- session token at resolve time — the engine never reads the
-- keychain on the per-page HTTP path.

CREATE CREDENTIAL IF NOT EXISTS vendor_smoke_api_key
    TYPE = CREDENTIAL
    SECRET 'df-smoke-test-abc123'
    DESCRIPTION 'Placeholder vendor API key for httpbin.org smoke testing';

-- --------------------------------------------------------------------------
-- 4. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.vendor_smoke
    COMMENT 'Vendor integration smoke-test landing — ingest pipeline regression';

-- --------------------------------------------------------------------------
-- 5. Connection — auth_mode = 'api_key_header'
-- --------------------------------------------------------------------------
-- `auth_mode = 'api_key_header'` tells the engine to set
-- `X-API-Key: <secret>` on every outbound request. The default header
-- name is `X-API-Key`; a future endpoint-level override could change
-- it per API if a vendor uses a non-standard header. The CREDENTIAL
-- binding is what the control plane resolves at session-token build
-- time — the secret material is inner-sealed into the token so the
-- engine never reads the keychain on the per-page HTTP path.

CREATE CONNECTION IF NOT EXISTS httpbin_smoke
    TYPE = rest_api
    OPTIONS (
        base_url         = 'https://httpbin.org',
        auth_mode        = 'api_key_header',
        auth_header_name = 'X-API-Key',
        storage_zone     = '{{zone_name}}',
        base_path        = 'httpbin_smoke',
        timeout_secs     = '30'
    )
    CREDENTIAL = vendor_smoke_api_key;

-- --------------------------------------------------------------------------
-- 6. Endpoint 1 — /headers echoes back every request header
-- --------------------------------------------------------------------------
-- httpbin's /headers is the single most useful endpoint for auth-path
-- regression: it echoes EVERY header the server received, so the
-- downstream table can literally SELECT the X-API-Key value that
-- travelled on the wire. If the credential didn't resolve, the echoed
-- value will be missing; if the wrong secret is attached, it will be
-- visibly wrong.

CREATE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_headers
    URL '/headers'
    RESPONSE FORMAT JSON
    OPTIONS (
        rate_limit_rps = '1'
    );

-- --------------------------------------------------------------------------
-- 7. Endpoint 2 — /uuid returns a v4 UUID
-- --------------------------------------------------------------------------

CREATE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_uuid
    URL '/uuid'
    RESPONSE FORMAT JSON
    OPTIONS (
        rate_limit_rps = '1'
    );

-- --------------------------------------------------------------------------
-- 8. Endpoint 3 — created and DROPped to demo the DROP syntax
-- --------------------------------------------------------------------------
-- The "typo cleanup" pattern: an endpoint is created, the author
-- realizes it was wrong, and drops it before running an INVOKE. This
-- is the only place in the API-demo suite that exercises
-- `DROP API ENDPOINT` as a standalone statement (every other demo
-- relies on DROP IF EXISTS in cleanup.sql).

CREATE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_dropped_example
    URL '/anything'
    RESPONSE FORMAT JSON;

DROP API ENDPOINT {{zone_name}}.httpbin_smoke.probe_dropped_example;

-- --------------------------------------------------------------------------
-- 9. SHOW API ENDPOINTS — verify only 2 endpoints remain
-- --------------------------------------------------------------------------

SHOW API ENDPOINTS IN CONNECTION {{zone_name}}.httpbin_smoke;

-- --------------------------------------------------------------------------
-- 10. INVOKE both surviving endpoints
-- --------------------------------------------------------------------------
-- Each INVOKE writes one JSON page under its endpoint's subfolder.

INVOKE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_headers;

INVOKE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_uuid;

-- --------------------------------------------------------------------------
-- 11. External table — /headers echo flatten (X-API-Key round-trip proof)
-- --------------------------------------------------------------------------
-- httpbin returns {headers: {Accept, Host, X-Api-Key, User-Agent, ...}}.
-- The flatten's include_paths pull exactly three fields: Host (always
-- httpbin.org), X-Api-Key (the echoed vault secret), and Accept. If
-- X-Api-Key comes back as the exact literal string 'df-smoke-test-
-- abc123', every layer of the auth pipeline worked end-to-end.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.vendor_smoke.headers_bronze
USING JSON
LOCATION 'httpbin_smoke/probe_headers'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.headers.Host",
            "$.headers.X-Api-Key",
            "$.headers.Accept"
        ],
        "column_mappings": {
            "$.headers.Host":      "request_host",
            "$.headers.X-Api-Key": "x_api_key_echo",
            "$.headers.Accept":    "accept_header"
        },
        "max_depth": 3,
        "separator": "_",
        "infer_types": true
    }'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.vendor_smoke.headers_bronze;

-- --------------------------------------------------------------------------
-- 12. External table — /uuid flatten
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.vendor_smoke.uuid_bronze
USING JSON
LOCATION 'httpbin_smoke/probe_uuid'
OPTIONS (
    recursive = 'true',
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": ["$.uuid"],
        "column_mappings": {"$.uuid": "generated_uuid"},
        "max_depth": 2,
        "separator": "_",
        "infer_types": true
    }'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.vendor_smoke.uuid_bronze;
