-- ============================================================================
-- JSON Typed Billing Events: Verification Queries
-- ============================================================================
-- Each query proves a different facet of the bronze-to-silver typing pattern:
--
--   * amount          BIGINT     promoted from Utf8 via TRY_CAST           Q3
--   * is_active       BOOLEAN    promoted from Utf8 via TRY_CAST           Q4
--   * is_trialing     BOOLEAN    promoted from Utf8 via TRY_CAST           Q4
--   * event_timestamp TIMESTAMP  promoted from Utf8 via TRY_CAST           Q5
--   * tags            STRING     kept as JSON literal; queried with        Q6, Q7
--                                JSON_ARRAY_LENGTH and LIKE
--
-- All typed queries target the silver table (events_curated). Bronze stays
-- Utf8 by design and is only spot-checked for landing the right row count.
-- ============================================================================


-- ============================================================================
-- 1. BRONZE LANDING: 5 NDJSON files * 10 events each = 50 Utf8 rows
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT *
FROM {{zone_name}}.billing.events;


-- ============================================================================
-- 2. SILVER LANDING: INSERT ... SELECT TRY_CAST round-trips all 50 rows
-- ============================================================================

ASSERT ROW_COUNT = 50
SELECT *
FROM {{zone_name}}.billing.events_curated;


-- ============================================================================
-- 3. REVENUE AGGREGATION: amount typed as BIGINT in silver
-- ============================================================================
-- TRY_CAST(amount AS BIGINT) in the silver INSERT produced a real BIGINT
-- column. SUM works natively without per-query CAST.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_cents = 507900
SELECT SUM(amount) AS total_cents
FROM {{zone_name}}.billing.events_curated;


-- ============================================================================
-- 4. BOOLEAN FILTER: is_active and is_trialing typed as BOOLEAN in silver
-- ============================================================================
-- TRY_CAST(is_active AS BOOLEAN) recognises 'true' / 'false' / 't' / 'f' and
-- a few other tokens (see TRY_CAST_BOOL doc) and returns NULL otherwise.
-- The "WHERE is_active = true AND is_trialing = false" predicate is a real
-- BOOLEAN comparison.

ASSERT ROW_COUNT = 1
ASSERT VALUE active_count = 32
SELECT COUNT(*) AS active_count
FROM {{zone_name}}.billing.events_curated
WHERE is_active = true
  AND is_trialing = false;


-- ============================================================================
-- 5. TIMESTAMP FILTER: event_timestamp typed as TIMESTAMP in silver
-- ============================================================================
-- TRY_CAST(event_timestamp AS TIMESTAMP) parses the ISO-8601 strings
-- ("2024-06-15T07:00:00Z") into real TIMESTAMP values, enabling direct
-- comparison with TIMESTAMP literals.

ASSERT ROW_COUNT = 1
ASSERT VALUE recent_count = 28
SELECT COUNT(*) AS recent_count
FROM {{zone_name}}.billing.events_curated
WHERE event_timestamp > TIMESTAMP '2024-06-01';


-- ============================================================================
-- 6. ARRAY ARITY: tags kept as JSON literal, measured with JSON_ARRAY_LENGTH
-- ============================================================================
-- The synthetic data cycles through arities 0,1,2,3,4 so 40 of 50 events
-- have a non-empty tag list (10 per non-zero arity), spread across all 17
-- distinct customers. JSON_ARRAY_LENGTH('[]') returns 0 and JSON_ARRAY_LENGTH
-- on a non-empty literal returns the element count.

ASSERT ROW_COUNT = 17
ASSERT VALUE max_tag_count >= 1
ASSERT VALUE max_tag_count <= 4
SELECT customer_id,
       MAX(JSON_ARRAY_LENGTH(tags)) AS max_tag_count
FROM {{zone_name}}.billing.events_curated
WHERE JSON_ARRAY_LENGTH(tags) > 0
GROUP BY customer_id
ORDER BY customer_id;


-- ============================================================================
-- 7. ARRAY MEMBERSHIP: LIKE pattern against the JSON literal
-- ============================================================================
-- DeltaForge has no JSON_ARRAY_CONTAINS function. For flat string arrays
-- with no embedded quotes, the canonical workaround is a LIKE pattern that
-- matches the quoted element. 18 of the 50 events carry the 'enterprise'
-- tag (every enterprise-plan event plus a few cross-plan promo events).

ASSERT ROW_COUNT = 1
ASSERT VALUE enterprise_events = 18
SELECT COUNT(*) AS enterprise_events
FROM {{zone_name}}.billing.events_curated
WHERE tags LIKE '%"enterprise"%';


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting summary that exercises every silver column type plus the
-- JSON-literal tag predicates in a single query. Each aggregate hits the
-- typed silver column directly with no per-row CAST. Matching the
-- precomputed totals proves the bronze -> silver typing pipeline end-to-end.

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
    SUM(CASE WHEN JSON_ARRAY_LENGTH(tags) > 0 THEN 1 ELSE 0 END)              AS with_any_tag,
    SUM(CASE WHEN tags LIKE '%"enterprise"%' THEN 1 ELSE 0 END)               AS enterprise_tagged
FROM {{zone_name}}.billing.events_curated;
