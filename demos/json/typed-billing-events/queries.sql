-- ============================================================================
-- JSON Typed Billing Events — Verification Queries
-- ============================================================================
-- Each query proves that the JSON flattener delivered the typed columns the
-- json_flatten_config asked for:
--
--   • amount          — BIGINT  (type_hints: int64)         — Q2
--   • is_active       — BOOLEAN (type_hints: boolean)       — Q3
--   • is_trialing     — BOOLEAN (type_hints: boolean)       — Q3
--   • event_timestamp — TIMESTAMP (type_hints: timestamp)   — Q4
--   • tags            — ARRAY<STRING> (default_array_handling: as_list) — Q5/Q6
--
-- If type_hints did not fire, Q2/Q3/Q4 would error or return wrong values
-- (string comparisons against BOOLEAN literals, no SUM on Utf8).
-- If default_array_handling = 'as_list' did not fire, Q5/Q6 would error
-- because array_length / array_contains can't be called on a Utf8 column.
-- ============================================================================


-- ============================================================================
-- 1. EVENT COUNT — 5 NDJSON files * 10 events each = 50 events
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT *
FROM {{zone_name}}.billing.events;


-- ============================================================================
-- 2. REVENUE AGGREGATION — proves type_hints "$.amount" -> int64 worked
-- ============================================================================
-- Without the type_hint, $.amount would land as Utf8 (the JSON file has it
-- as a quoted string per Stripe convention). SUM(amount) would either error
-- or require CAST(amount AS BIGINT). With the type_hint, amount is BIGINT
-- from the start and SUM works natively.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_cents = 507900
SELECT SUM(amount) AS total_cents
FROM {{zone_name}}.billing.events;


-- ============================================================================
-- 3. BOOLEAN FILTER — proves type_hints "$.is_active" / "$.is_trialing"
--                     -> boolean worked
-- ============================================================================
-- The JSON has "is_active": "true" / "false" as quoted strings. Without the
-- type_hint, "WHERE is_active = true" would not match anything (string vs
-- boolean comparison). With the type_hint these are real BOOLEAN columns
-- and the filter behaves as expected.

ASSERT ROW_COUNT = 1
ASSERT VALUE active_count = 32
SELECT COUNT(*) AS active_count
FROM {{zone_name}}.billing.events
WHERE is_active = true
  AND is_trialing = false;


-- ============================================================================
-- 4. TIMESTAMP FILTER — proves type_hints "$.event_timestamp" -> timestamp
-- ============================================================================
-- ISO-8601 strings in the JSON ("2024-06-15T07:00:00Z") are auto-promoted
-- to TIMESTAMP via the type_hint, enabling direct comparison with TIMESTAMP
-- literals. Half the events fall after 2024-06-01.

ASSERT ROW_COUNT = 1
ASSERT VALUE recent_count = 28
SELECT COUNT(*) AS recent_count
FROM {{zone_name}}.billing.events
WHERE event_timestamp > TIMESTAMP '2024-06-01';


-- ============================================================================
-- 5. ARRAY ARITY — proves default_array_handling = 'as_list' worked
-- ============================================================================
-- The $.tags array lands as Arrow List<Utf8>. array_length(tags, 1) returns
-- the per-row arity. The synthetic data cycles through arities 0,1,2,3,4
-- so 40 of 50 events have a non-empty tag list (10 per non-zero arity)
-- across all 17 distinct customers.

ASSERT ROW_COUNT = 17
ASSERT VALUE max_tag_count >= 1
ASSERT VALUE max_tag_count <= 4
SELECT customer_id,
       MAX(array_length(tags, 1)) AS max_tag_count
FROM {{zone_name}}.billing.events
WHERE array_length(tags, 1) > 0
GROUP BY customer_id
ORDER BY customer_id;


-- ============================================================================
-- 6. ARRAY MEMBERSHIP — proves the List<Utf8> contents are queryable
-- ============================================================================
-- array_contains works element-wise against the Arrow List column. 18 of
-- the 50 events carry the 'enterprise' tag (every enterprise-plan event
-- plus a handful of cross-plan promo events).

ASSERT ROW_COUNT = 1
ASSERT VALUE enterprise_events = 18
SELECT COUNT(*) AS enterprise_events
FROM {{zone_name}}.billing.events
WHERE array_contains(tags, 'enterprise');


-- ============================================================================
-- 7. SILVER ROUND-TRIP — typed columns survive the bronze->silver INSERT
-- ============================================================================
-- The silver Delta table was populated by an INSERT ... SELECT off bronze.
-- Re-running the same revenue + enterprise-tag aggregates against silver
-- must produce the same numbers, proving the BIGINT, BOOLEAN, TIMESTAMP,
-- and ARRAY<STRING> column types all round-trip cleanly through the Delta
-- writer.

ASSERT ROW_COUNT = 1
ASSERT VALUE silver_total_cents = 507900
ASSERT VALUE silver_enterprise = 18
ASSERT VALUE silver_active_nontrialing = 32
SELECT
    SUM(amount)                                                                AS silver_total_cents,
    SUM(CASE WHEN array_contains(tags, 'enterprise') THEN 1 ELSE 0 END)        AS silver_enterprise,
    SUM(CASE WHEN is_active = true AND is_trialing = false THEN 1 ELSE 0 END)  AS silver_active_nontrialing
FROM {{zone_name}}.billing.events_curated;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting summary that exercises ALL FOUR type-hint columns AND the
-- list column in a single query. Every aggregate uses the typed column
-- directly with no CAST or json_* function — that, together with the row
-- counts and totals matching the precomputed ground truth, proves the
-- type_hints + as_list pipeline end-to-end.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_events       = 50
ASSERT VALUE total_amount_cents = 507900
ASSERT VALUE active_nontrialing = 32
ASSERT VALUE after_jun_2024     = 28
ASSERT VALUE with_any_tag       = 40
ASSERT VALUE enterprise_tagged  = 18
SELECT
    COUNT(*)                                                                  AS total_events,
    SUM(amount)                                                               AS total_amount_cents,
    SUM(CASE WHEN is_active = true AND is_trialing = false THEN 1 ELSE 0 END) AS active_nontrialing,
    SUM(CASE WHEN event_timestamp > TIMESTAMP '2024-06-01' THEN 1 ELSE 0 END) AS after_jun_2024,
    SUM(CASE WHEN array_length(tags, 1) > 0 THEN 1 ELSE 0 END)                AS with_any_tag,
    SUM(CASE WHEN array_contains(tags, 'enterprise') THEN 1 ELSE 0 END)       AS enterprise_tagged
FROM {{zone_name}}.billing.events;
