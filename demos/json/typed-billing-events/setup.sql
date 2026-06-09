-- ============================================================================
-- JSON Typed Billing Events — Setup Script
-- ============================================================================
-- Real-world story: a SaaS subscription billing platform's webhook delivers
-- JSON events. Per Stripe-style convention, numeric amounts arrive as quoted
-- strings ("amount": "2999" for $29.99), booleans arrive as quoted strings
-- ("is_active": "true"), and tag arrays arrive as native JSON arrays.
--
-- DeltaForge enforces a strict two-phase typing model:
--   Bronze: every column lands as Utf8. No type coercion in the reader.
--   Silver: explicit TRY_CAST (or TRY_CAST_INT / TRY_CAST_BOOL) at INSERT
--           time promotes each column to its analytical type.
--
-- This demo follows that pattern end-to-end:
--   1. Zone + schema           bronze landing + billing schema
--   2. External table (bronze) JSON over the 5 NDJSON webhook drops; every
--                              flattened column is Utf8. Tag arrays are
--                              kept as JSON-literal strings (default
--                              array_handling = 'to_json').
--   3. Delta table   (silver)  curated copy of bronze, with TRY_CAST in
--                              the INSERT promoting amount to BIGINT,
--                              is_active / is_trialing to BOOLEAN, and
--                              event_timestamp to TIMESTAMP. The tags
--                              column stays as a JSON-array string and is
--                              queried with JSON_ARRAY_LENGTH and LIKE.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables: demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.billing
    COMMENT 'Subscription billing events: webhook landing + curated silver';

-- --------------------------------------------------------------------------
-- 2. Bronze external table over the raw NDJSON webhook drops (Utf8 only)
-- --------------------------------------------------------------------------
-- Every flattened column is Utf8 by design. The tags JSON array is kept as
-- a JSON-literal string via default_array_handling = 'to_json'. Type
-- promotion happens in the silver INSERT, not here.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.billing.events
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    json_flatten_config = '{
        "root_path": "$",
        "include_paths": [
            "$.id",
            "$.customer_id",
            "$.amount",
            "$.currency",
            "$.tags",
            "$.is_active",
            "$.is_trialing",
            "$.event_timestamp",
            "$.event_type",
            "$.plan",
            "$.country"
        ],
        "column_mappings": {
            "$.id":              "id",
            "$.customer_id":     "customer_id",
            "$.amount":          "amount",
            "$.currency":        "currency",
            "$.tags":            "tags",
            "$.is_active":       "is_active",
            "$.is_trialing":     "is_trialing",
            "$.event_timestamp": "event_timestamp",
            "$.event_type":      "event_type",
            "$.plan":            "plan",
            "$.country":         "country"
        },
        "default_array_handling": "to_json",
        "max_depth": 3,
        "separator": "_",
        "infer_types": false
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

-- --------------------------------------------------------------------------
-- 3. Silver Delta table: typed promotion of the bronze landing
-- --------------------------------------------------------------------------
-- Bronze is the Utf8 source-of-truth. Silver is the typed layer downstream
-- consumers (BI, finance, retention) query: same shape, but stored as
-- Delta with proper analytical types so SUM(amount), WHERE is_active,
-- and event_timestamp > TIMESTAMP comparisons work without per-query CAST.

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.billing.events_curated (
    id              STRING,
    customer_id     STRING,
    amount          BIGINT,
    currency        STRING,
    tags            STRING,
    is_active       BOOLEAN,
    is_trialing     BOOLEAN,
    event_timestamp TIMESTAMP,
    event_type      STRING,
    plan            STRING,
    country         STRING
)
LOCATION 'typed-billing-events/silver/events_curated';

-- TRY_CAST returns NULL on parse failure rather than erroring, which is the
-- canonical bronze-to-silver coercion pattern. Tags stays as a Utf8 JSON
-- literal because DeltaForge has no first-class JSON-string-to-ARRAY
-- function; queries use JSON_ARRAY_LENGTH and LIKE against the literal.

INSERT INTO {{zone_name}}.billing.events_curated
SELECT
    id,
    customer_id,
    TRY_CAST(amount AS BIGINT)            AS amount,
    currency,
    tags,
    TRY_CAST(is_active AS BOOLEAN)        AS is_active,
    TRY_CAST(is_trialing AS BOOLEAN)      AS is_trialing,
    TRY_CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
    event_type,
    plan,
    country
FROM {{zone_name}}.billing.events;
