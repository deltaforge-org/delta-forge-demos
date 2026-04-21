-- ============================================================================
-- JSON Typed Billing Events — Setup Script
-- ============================================================================
-- Real-world story: a SaaS subscription billing platform's webhook delivers
-- JSON events. Per Stripe-style convention, numeric amounts arrive as quoted
-- strings ("amount": "2999" for $29.99) and booleans arrive as quoted strings
-- ("is_active": "true"). Tag arrays arrive as native JSON arrays.
--
-- The data team needs the bronze table to expose these as their "natural"
-- analytical types — BIGINT for revenue aggregation, BOOLEAN for filtering,
-- TIMESTAMP for time-bucketing, and Arrow List<Utf8> for tag membership tests
-- — without scattering CAST and json_array_* calls across every query.
--
-- This demo exercises two json_flatten_config features:
--   1. type_hints — path -> Arrow type override. Forces $.amount to int64,
--      $.is_active and $.is_trialing to boolean, $.event_timestamp to
--      timestamp. Without these the columns land as Utf8 and every query
--      needs CAST.
--   2. default_array_handling = 'as_list' — when a flattened path lands on
--      an array, the column is built as a real Arrow ListArray of the
--      inferred element type. Previously this produced a JSON-string column.
--      Now SQL functions like array_length, array_contains, and cardinality
--      work natively against the column.
--
-- Pipeline:
--   1. Zone + schema           — bronze landing + billing schema
--   2. External table (bronze) — JSON over the 5 NDJSON webhook drops with
--                                json_flatten_config carrying the type_hints
--                                and as_list settings
--   3. Delta table   (silver)  — curated copy of bronze, demonstrating that
--                                the typed columns (BIGINT / BOOLEAN /
--                                TIMESTAMP) and the ARRAY<STRING> column
--                                round-trip cleanly into a Delta table for
--                                downstream BI consumers.
-- ============================================================================

-- --------------------------------------------------------------------------
-- 1. Zone + schema
-- --------------------------------------------------------------------------

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.billing
    COMMENT 'Subscription billing events — webhook landing + curated silver';

-- --------------------------------------------------------------------------
-- 2. Bronze external table over the raw NDJSON webhook drops
-- --------------------------------------------------------------------------
-- type_hints forces inferred Arrow types for paths that the JSON-side has
-- as quoted strings. default_array_handling = 'as_list' makes $.tags land
-- as Arrow List<Utf8> instead of a JSON-encoded string.

CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.billing.events
USING JSON
LOCATION '{{data_path}}'
OPTIONS (
    recursive = 'true',
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
        "type_hints": {
            "$.amount":          "int64",
            "$.is_active":       "boolean",
            "$.is_trialing":     "boolean",
            "$.event_timestamp": "timestamp"
        },
        "default_array_handling": "as_list",
        "max_depth": 3,
        "separator": "_",
        "infer_types": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.billing.events;
GRANT ADMIN ON TABLE {{zone_name}}.billing.events TO USER {{current_user}};

-- --------------------------------------------------------------------------
-- 3. Silver Delta table — curated promotion of the bronze landing
-- --------------------------------------------------------------------------
-- Bronze is the raw source-of-truth: every webhook drop adds another file
-- under the landing folder. Silver is the curated layer downstream
-- consumers (BI, finance, retention) query — same shape, but stored as
-- Delta with ACID writes, time travel, schema evolution, OPTIMIZE/VACUUM,
-- and cross-engine portability.
--
-- This INSERT exercises the engine end-to-end: the Arrow List<Utf8> column
-- from the JSON flattener round-trips into a Delta ARRAY<STRING> column,
-- and the BIGINT / BOOLEAN / TIMESTAMP columns land natively without any
-- CAST in this statement (they arrive pre-typed thanks to the type_hints
-- on the bronze side).

CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.billing.events_curated (
    id              STRING,
    customer_id     STRING,
    amount          BIGINT,
    currency        STRING,
    tags            ARRAY<STRING>,
    is_active       BOOLEAN,
    is_trialing     BOOLEAN,
    event_timestamp TIMESTAMP,
    event_type      STRING,
    plan            STRING,
    country         STRING
)
LOCATION 'silver/events_curated';

INSERT INTO {{zone_name}}.billing.events_curated
SELECT
    id,
    customer_id,
    amount,
    currency,
    tags,
    is_active,
    is_trialing,
    event_timestamp,
    event_type,
    plan,
    country
FROM {{zone_name}}.billing.events;

GRANT ADMIN ON TABLE {{zone_name}}.billing.events_curated TO USER {{current_user}};
