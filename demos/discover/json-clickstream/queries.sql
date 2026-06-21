-- ============================================================================
-- Demo: Product Clickstream Auto-Onboarding - Queries
-- ============================================================================
-- The landing folder holds 40 clickstream events across two physical shapes:
--   events_2026_q1.json    - 30 events as a JSON array (quarterly backfill)
--   events_2026_apr.ndjson - 10 events as newline-delimited JSON (increment)
-- DISCOVER registers ONE external table over both. Every assertion value
-- below was precomputed from the data files, not the engine.
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode - review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed
-- files, classifies the text shape (JSON array and NDJSON, both USING JSON),
-- and synthesizes the json_flatten_config by unioning the discovered paths
-- across both files. FILE_METADATA adds the df_file_name / df_row_number
-- provenance columns to the generated table.

DISCOVER {{zone_name}}.discover_demos.clickstream_events
    PATH 'discover-json-clickstream'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode - auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the format agreement across the .json and .ndjson
-- files. This single statement replaces a hand-written CREATE EXTERNAL TABLE
-- + json_flatten_config block.

DISCOVER {{zone_name}}.discover_demos.clickstream_events
    PATH 'discover-json-clickstream'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The unified feed - all 40 events from both shapes
-- ============================================================================
-- One table spans the JSON-array backfill and the NDJSON increment. The
-- discovered schema is the union of both files' paths.

ASSERT ROW_COUNT = 40
SELECT
    event_id,
    event_type,
    country,
    page,
    df_file_name
FROM {{zone_name}}.discover_demos.clickstream_events;

-- ============================================================================
-- Query 4: Event mix - counts per event type
-- ============================================================================
-- Proves the body of every record decoded correctly across both physical
-- shapes. Counts are exact and precomputed from the data.

ASSERT ROW_COUNT = 5
ASSERT VALUE event_count = 18 WHERE event_type = 'page_view'
ASSERT VALUE event_count = 7 WHERE event_type = 'add_to_cart'
ASSERT VALUE event_count = 6 WHERE event_type = 'checkout'
ASSERT VALUE event_count = 5 WHERE event_type = 'logout'
ASSERT VALUE event_count = 4 WHERE event_type = 'sign_up'
SELECT
    event_type,
    COUNT(*) AS event_count
FROM {{zone_name}}.discover_demos.clickstream_events
GROUP BY event_type
ORDER BY event_count DESC;

-- ============================================================================
-- Query 5: Geographic spread - events per country
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE event_count = 11 WHERE country = 'US'
ASSERT VALUE event_count = 9 WHERE country = 'GB'
ASSERT VALUE event_count = 7 WHERE country = 'DE'
ASSERT VALUE event_count = 7 WHERE country = 'IN'
ASSERT VALUE event_count = 6 WHERE country = 'NO'
SELECT
    country,
    COUNT(*) AS event_count
FROM {{zone_name}}.discover_demos.clickstream_events
GROUP BY country
ORDER BY event_count DESC, country;

-- ============================================================================
-- Query 6: Provenance - which physical file each row came from
-- ============================================================================
-- df_file_name comes from the FILE_METADATA = true discovery option. The
-- NDJSON increment contributes exactly 10 rows; the JSON-array backfill
-- contributes the other 30.

ASSERT ROW_COUNT = 2
ASSERT VALUE rows_from_file = 30 WHERE source_shape = 'json-array backfill'
ASSERT VALUE rows_from_file = 10 WHERE source_shape = 'ndjson increment'
SELECT
    CASE
        WHEN df_file_name LIKE '%.ndjson' THEN 'ndjson increment'
        ELSE 'json-array backfill'
    END AS source_shape,
    COUNT(*) AS rows_from_file
FROM {{zone_name}}.discover_demos.clickstream_events
GROUP BY 1
ORDER BY rows_from_file DESC;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total event volume, the
-- conversion-funnel tail (checkout events), and the two-shape union.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_events = 40
ASSERT VALUE checkout_events = 6
ASSERT VALUE distinct_countries = 5
ASSERT VALUE distinct_source_files = 2
SELECT
    COUNT(*)                                              AS total_events,
    COUNT(*) FILTER (WHERE event_type = 'checkout')       AS checkout_events,
    COUNT(DISTINCT country)                               AS distinct_countries,
    COUNT(DISTINCT df_file_name)                          AS distinct_source_files
FROM {{zone_name}}.discover_demos.clickstream_events;
