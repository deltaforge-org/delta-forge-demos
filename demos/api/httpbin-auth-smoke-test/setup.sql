-- ============================================================================
-- Demo: Vendor Auth Smoke Test, api_key_header
-- Feature: CREATE CREDENTIAL on the default OS Keychain, auth_mode =
--          'api_key_header', X-API-Key echo round-trip proof,
--          standalone DROP API ENDPOINT
-- ============================================================================
--
-- Real-world story: a DevOps team vets the DeltaForge REST API ingest
-- pipeline against httpbin.org before wiring up a new real vendor. The
-- goal is to prove the credential, auth-mode, and endpoint plumbing
-- all work correctly in isolation, an always-green smoke test the
-- team can run anytime to confirm the machinery is intact.
--
-- This file declares the catalog objects only (credential, connection,
-- endpoints, bronze tables). The two INVOKE calls, the run audit, the
-- schema detection, and the assertions all live in queries.sql so the
-- user can see the X-API-Key round-trip end to end from a single file.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Credential, stored on the default OS Keychain
-- --------------------------------------------------------------------------
-- Omitting `IN CREDENTIAL STORAGE` routes the secret to the default
-- OS Keychain backend. The secret material is inner-sealed into the
-- session token at resolve time, the engine never reads the
-- keychain on the per-page HTTP path. CREATE CREDENTIAL STORAGE is
-- reserved for cloud backends (AZURE, AWS, GCP); the OS Keychain is
-- always-on and auto-provisioned at catalog migration time.

CREATE CREDENTIAL IF NOT EXISTS httpbin_smoke_api_key
    TYPE = CREDENTIAL
    SECRET 'df-smoke-test-abc123'
    DESCRIPTION 'Placeholder vendor API key for httpbin.org smoke testing';

-- --------------------------------------------------------------------------
-- 2. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'Bronze landing zone for REST API ingests';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.httpbin_smoke
    COMMENT 'Vendor integration smoke-test landing, ingest pipeline regression';

-- --------------------------------------------------------------------------
-- 3. Connection, auth_mode = 'api_key_header'
-- --------------------------------------------------------------------------
-- `auth_mode = 'api_key_header'` tells the engine to set
-- `X-API-Key: <secret>` on every outbound request. The default header
-- name is `X-API-Key`; a future endpoint-level override could change
-- it per API if a vendor uses a non-standard header. The CREDENTIAL
-- binding is what the control plane resolves at session-token build
-- time, the secret material is inner-sealed into the token so the
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
    CREDENTIAL = httpbin_smoke_api_key;

-- --------------------------------------------------------------------------
-- 4. Endpoint 1, /headers echoes back every request header
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
-- 5. Endpoint 2, /uuid returns a v4 UUID
-- --------------------------------------------------------------------------

CREATE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_uuid
    URL '/uuid'
    RESPONSE FORMAT JSON
    OPTIONS (
        rate_limit_rps = '1'
    );

-- --------------------------------------------------------------------------
-- 6. Endpoint 3, created and DROPped to demo the DROP syntax
-- --------------------------------------------------------------------------
-- The "typo cleanup" pattern: an endpoint is created, the author
-- realizes it was wrong, and drops it before running an INVOKE. This
-- is the only place in the API-demo suite that exercises
-- `DROP API ENDPOINT` as a standalone statement (every other demo
-- relies on DROP IF EXISTS in cleanup.sql). Catalog DDL belongs in
-- setup; the dropped endpoint never reaches queries.sql for INVOKE.

CREATE API ENDPOINT {{zone_name}}.httpbin_smoke.probe_dropped_example
    URL '/anything'
    RESPONSE FORMAT JSON;

DROP API ENDPOINT {{zone_name}}.httpbin_smoke.probe_dropped_example;

-- --------------------------------------------------------------------------
-- 7. Bronze external table, /headers echo flatten (X-API-Key round-trip proof)
-- --------------------------------------------------------------------------
-- httpbin returns {headers: {Accept, Host, X-Api-Key, User-Agent, ...}}.
-- The flatten's include_paths pull exactly three fields: Host (always
-- httpbin.org), X-Api-Key (the echoed vault secret), and Accept. If
-- X-Api-Key comes back as the exact literal string 'df-smoke-test-
-- abc123', every layer of the auth pipeline worked end-to-end.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.httpbin_smoke.headers_bronze
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

-- --------------------------------------------------------------------------
-- 8. Bronze external table, /uuid flatten
-- --------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.httpbin_smoke.uuid_bronze
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
