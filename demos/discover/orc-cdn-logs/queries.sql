-- ============================================================================
-- Demo: CDN Edge Access Logs Auto-Onboarding - Queries
-- ============================================================================
-- The landing folder holds one ORC file of edge access logs:
--   edge_access_2026_06_21.orc - 50 access-log rows, columnar ORC
-- DISCOVER reads the ORC footer (the 'ORC' magic marker, not the extension),
-- takes the embedded schema, and registers ONE external table from it. Every
-- assertion value below was precomputed from the data file, not the engine.
-- ============================================================================

-- ============================================================================
-- Query 1: DISCOVER in PRINT mode - review the generated DDL
-- ============================================================================
-- PRINT returns the exact CREATE EXTERNAL TABLE statement DISCOVER would run
-- WITHOUT registering anything. Detection reads the bytes of the landed file,
-- recognises the trailing 'ORC' footer marker, and lifts the column names and
-- types straight from the embedded schema. FILE_METADATA adds the df_file_name
-- / df_row_number provenance columns to the generated table.

DISCOVER {{zone_name}}.discover_demos.cdn_access_logs
    PATH 'orc-cdn-logs'
    WITH (FILE_METADATA = true)
    PRINT;

-- ============================================================================
-- Query 2: DISCOVER in EXECUTE mode - auto-register the external table
-- ============================================================================
-- The same statement without PRINT performs the registration and returns a
-- summary row (object, location, format, confidence, action, evidence). The
-- evidence column records the ORC footer detection. This single statement
-- replaces a hand-written CREATE EXTERNAL TABLE with an explicit column list.

DISCOVER {{zone_name}}.discover_demos.cdn_access_logs
    PATH 'orc-cdn-logs'
    WITH (FILE_METADATA = true);

-- ============================================================================
-- Query 3: The full feed - all 50 access-log rows
-- ============================================================================
-- One table over the columnar ORC file. The discovered schema is the ORC
-- footer's embedded field set, all read back from one file.

ASSERT ROW_COUNT = 50
ASSERT VALUE distinct_source_files = 1
SELECT
    request_id,
    edge_pop,
    http_method,
    status_code,
    cache_result,
    country,
    COUNT(DISTINCT df_file_name) OVER () AS distinct_source_files
FROM {{zone_name}}.discover_demos.cdn_access_logs;

-- ============================================================================
-- Query 4: Response mix - counts per HTTP status code
-- ============================================================================
-- Proves the int32 status_code column decoded correctly from the ORC schema.
-- Counts are exact and precomputed from the data.

ASSERT ROW_COUNT = 4
ASSERT VALUE hits = 13 WHERE status_code = 200
ASSERT VALUE hits = 13 WHERE status_code = 304
ASSERT VALUE hits = 12 WHERE status_code = 404
ASSERT VALUE hits = 12 WHERE status_code = 500
SELECT
    status_code,
    COUNT(*) AS hits
FROM {{zone_name}}.discover_demos.cdn_access_logs
GROUP BY status_code
ORDER BY status_code;

-- ============================================================================
-- Query 5: Cache effectiveness - counts per cache result
-- ============================================================================
-- The string cache_result column from the ORC footer, grouped exactly.

ASSERT ROW_COUNT = 3
ASSERT VALUE hits = 17 WHERE cache_result = 'HIT'
ASSERT VALUE hits = 17 WHERE cache_result = 'MISS'
ASSERT VALUE hits = 16 WHERE cache_result = 'EXPIRED'
SELECT
    cache_result,
    COUNT(*) AS hits
FROM {{zone_name}}.discover_demos.cdn_access_logs
GROUP BY cache_result
ORDER BY hits DESC, cache_result;

-- ============================================================================
-- Query 6: Edge footprint - requests served per edge PoP
-- ============================================================================
-- The string edge_pop column, grouped per point of presence.

ASSERT ROW_COUNT = 4
ASSERT VALUE hits = 13 WHERE edge_pop = 'IAD'
ASSERT VALUE hits = 13 WHERE edge_pop = 'LHR'
ASSERT VALUE hits = 12 WHERE edge_pop = 'FRA'
ASSERT VALUE hits = 12 WHERE edge_pop = 'SIN'
SELECT
    edge_pop,
    COUNT(*) AS hits
FROM {{zone_name}}.discover_demos.cdn_access_logs
GROUP BY edge_pop
ORDER BY hits DESC, edge_pop;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting invariants for the whole demo: total request volume, the
-- server-error tail (5xx responses), the distinct edge PoPs, and the single
-- source file the table was discovered from.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_requests = 50
ASSERT VALUE server_errors = 12
ASSERT VALUE cache_hits = 17
ASSERT VALUE distinct_edge_pops = 4
ASSERT VALUE distinct_source_files = 1
SELECT
    COUNT(*)                                          AS total_requests,
    COUNT(*) FILTER (WHERE status_code = 500)          AS server_errors,
    COUNT(*) FILTER (WHERE cache_result = 'HIT')       AS cache_hits,
    COUNT(DISTINCT edge_pop)                           AS distinct_edge_pops,
    COUNT(DISTINCT df_file_name)                       AS distinct_source_files
FROM {{zone_name}}.discover_demos.cdn_access_logs;
