-- ============================================================================
-- ORC Server Logs — Verification Queries
-- ============================================================================
-- Each query verifies a specific ORC feature: multi-file reading, schema
-- evolution, LOCATION glob filtering, file_metadata, and self-describing
-- schema with automatic type detection from ORC file footers.
-- ============================================================================


-- ============================================================================
-- 1. TOTAL ROW COUNT — 2,500 requests across 5 server files
-- ============================================================================

ASSERT ROW_COUNT = 2500
SELECT *
FROM {{zone_name}}.orc_demos.all_requests;


-- ============================================================================
-- 2. BROWSE REQUESTS — See column types (self-describing ORC schema)
-- ============================================================================

ASSERT ROW_COUNT = 20
SELECT request_id, server_name, timestamp, method, endpoint,
       status_code, response_time_ms, response_bytes,
       request_body_bytes, cache_hit
FROM {{zone_name}}.orc_demos.all_requests
LIMIT 20;


-- ============================================================================
-- 3. ROWS PER FILE — 500 rows each across 5 files
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE row_count = 500 WHERE df_file_name LIKE '%api-01%'
ASSERT VALUE row_count = 500 WHERE df_file_name LIKE '%web-01%'
SELECT df_file_name, COUNT(*) AS row_count
FROM {{zone_name}}.orc_demos.all_requests
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. SCHEMA EVOLUTION — request_body_bytes is NULL for v1 servers (web-*),
--    non-NULL for v2 servers (api-*)
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE total_rows = 500 WHERE server_name = 'api-01'
ASSERT VALUE total_rows = 500 WHERE server_name = 'api-02'
ASSERT VALUE body_bytes_non_null = 500 WHERE server_name = 'api-01'
ASSERT VALUE body_bytes_non_null = 0 WHERE server_name = 'web-01'
SELECT server_name,
       COUNT(*) AS total_rows,
       COUNT(request_body_bytes) AS body_bytes_non_null,
       COUNT(cache_hit) AS cache_hit_non_null
FROM {{zone_name}}.orc_demos.all_requests
GROUP BY server_name
ORDER BY server_name;


-- ============================================================================
-- 5. LOCATION GLOB — api01_only has exactly 500 rows
-- ============================================================================

ASSERT ROW_COUNT = 500
SELECT *
FROM {{zone_name}}.orc_demos.api01_only;


-- ============================================================================
-- 6. LOCATION GLOB — api01_only has all v2 columns populated
-- ============================================================================

ASSERT VALUE body_bytes_non_null = 500
SELECT COUNT(request_body_bytes) AS body_bytes_non_null
FROM {{zone_name}}.orc_demos.api01_only;

ASSERT VALUE cache_hit_non_null = 500
SELECT COUNT(cache_hit) AS cache_hit_non_null
FROM {{zone_name}}.orc_demos.api01_only;


-- ============================================================================
-- 7. FILE METADATA — All rows have non-NULL df_file_name
-- ============================================================================

ASSERT VALUE metadata_count = 2500
SELECT COUNT(*) AS metadata_count
FROM {{zone_name}}.orc_demos.all_requests
WHERE df_file_name IS NOT NULL;


-- ============================================================================
-- 8. ANALYTICS — HTTP status code distribution
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE request_count = 783 WHERE status_code = 200
SELECT status_code,
       COUNT(*) AS request_count,
       ROUND(AVG(CAST(response_time_ms AS DOUBLE)), 0) AS avg_response_ms,
       ROUND(AVG(CAST(response_bytes AS DOUBLE)), 0) AS avg_response_bytes
FROM {{zone_name}}.orc_demos.all_requests
GROUP BY status_code
ORDER BY request_count DESC;


-- ============================================================================
-- 9. ANALYTICS — Top endpoints by request count
-- ============================================================================

ASSERT ROW_COUNT = 10
ASSERT VALUE request_count = 270 WHERE endpoint = '/api/payments'
SELECT endpoint,
       COUNT(*) AS request_count,
       SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
       ROUND(AVG(CAST(response_time_ms AS DOUBLE)), 0) AS avg_response_ms
FROM {{zone_name}}.orc_demos.all_requests
GROUP BY endpoint
ORDER BY request_count DESC
LIMIT 10;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, schema evolution, glob, and file metadata.

ASSERT ROW_COUNT = 8
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_count_2500'
ASSERT VALUE result = 'PASS' WHERE check_name = 'five_source_files'
ASSERT VALUE result = 'PASS' WHERE check_name = 'schema_v1_null_body_bytes'
ASSERT VALUE result = 'PASS' WHERE check_name = 'schema_v2_has_body_bytes'
ASSERT VALUE result = 'PASS' WHERE check_name = 'glob_api01_count'
ASSERT VALUE result = 'PASS' WHERE check_name = 'file_metadata_populated'
SELECT check_name, result FROM (

    -- Check 1: Total row count = 2,500
    SELECT 'total_count_2500' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_demos.all_requests) = 2500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: 5 distinct source files
    SELECT 'five_source_files' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT df_file_name) FROM {{zone_name}}.orc_demos.all_requests) = 5
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Schema evolution — request_body_bytes NULL for web servers (1,500 rows)
    SELECT 'schema_v1_null_body_bytes' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_demos.all_requests
               WHERE server_name LIKE 'web-%' AND request_body_bytes IS NULL
           ) = 1500 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: Schema evolution — request_body_bytes non-NULL for api servers (1,000 rows)
    SELECT 'schema_v2_has_body_bytes' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_demos.all_requests
               WHERE server_name LIKE 'api-%' AND request_body_bytes IS NOT NULL
           ) = 1000 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: LOCATION glob — api01_only has 500 rows
    SELECT 'glob_api01_count' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_demos.api01_only) = 500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: File metadata populated for all rows
    SELECT 'file_metadata_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_demos.all_requests WHERE df_file_name IS NOT NULL) = 2500
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Self-describing schema — request_id column exists
    SELECT 'schema_request_id_exists' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'orc_demos' AND table_name = 'all_requests'
               AND column_name = 'request_id'
           ) = 1 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Column count — 13 data columns + 2 metadata = 15
    SELECT 'column_count_15' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM information_schema.columns
               WHERE table_schema = 'orc_demos' AND table_name = 'all_requests'
           ) = 15 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
