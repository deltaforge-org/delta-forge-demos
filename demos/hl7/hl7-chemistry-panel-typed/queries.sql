-- ============================================================================
-- HL7 Chemistry Panel — Typed Multi-OBX Ingestion (Queries)
-- ============================================================================
-- 4 ORU^R01 Comprehensive Metabolic Panels (CMP), 14 OBX segments per
-- message. Demonstrates the new HL7 v2 type-inference engine:
--
--   - msh_7 / obr_7 / obx_14   are real Timestamp(Microsecond) columns —
--                              filterable with TIMESTAMP literals, no CAST.
--   - pid_7  (DOB)             is Int64 — SUM/MIN/MAX work natively.
--   - obx_5 / obx_6 / obx_7 /  hold JSON arrays (one element per OBX
--     obx_8 / obx_3 / obx_2    occurrence in the message) thanks to
--                              repeating_segment_mode = 'to_json'.
--
-- Materialized columns of interest (all from setup.sql):
--   msh_7  = Message date/time          (Timestamp)
--   pid_3  = Patient ID                 (string)
--   pid_5  = Patient name               (string, LAST^FIRST^MIDDLE)
--   pid_7  = DOB (YYYYMMDD)             (Int64 via infer_integers)
--   pid_8  = Sex                        (string)
--   obr_4  = Universal service ID       (string)
--   obr_7  = Observation date/time      (Timestamp)
--   obx_2  = Value type                 (JSON array of "NM"... )
--   obx_3  = Observation identifier     (JSON array of "loinc^name^LN")
--   obx_5  = Observation value          (JSON array of "88","14",...)
--   obx_6  = Units                      (JSON array of "mg/dL", "mmol/L"...)
--   obx_7  = Reference range            (JSON array of "70-100", ...)
--   obx_8  = Abnormal flag              (JSON array of "N"|"H"|"L")
--   obx_14 = Result date/time           (JSON array, but timestamps; with
--                                        to_json all repeating columns are
--                                        Utf8 JSON regardless of inference)
-- ============================================================================


-- ============================================================================
-- Query 1: Row count == number of source messages
-- ============================================================================
-- 4 .hl7 files → 4 rows in the table (one row per ORU message).

ASSERT ROW_COUNT = 1
ASSERT VALUE n_messages = 4
SELECT COUNT(*) AS n_messages
FROM {{zone_name}}.hl7_demos.chem_panels_typed;


-- ============================================================================
-- Query 2: msh_7 is a real Timestamp — filter with TIMESTAMP literal
-- ============================================================================
-- Without infer_timestamps the engine would have parsed MSH-7 ("20240301094500")
-- as Utf8 and the comparator below would have been a string compare.
-- With the fix, msh_7 is Timestamp(Microsecond) so the filter is type-correct.
-- 3 of the 4 messages were sent strictly after 2024-03-15 00:00:00.

ASSERT ROW_COUNT = 1
ASSERT VALUE after_mid_march = 3
SELECT COUNT(*) AS after_mid_march
FROM {{zone_name}}.hl7_demos.chem_panels_typed
WHERE msh_7 > TIMESTAMP '2024-03-15 00:00:00';


-- ============================================================================
-- Query 3: pid_7 (DOB) is Int64 — native SUM / MIN / MAX
-- ============================================================================
-- Every PID-7 in the source is an 8-digit YYYYMMDD value. With infer_integers
-- the column is Int64; without the fix it would have been Utf8 and SUM(pid_7)
-- would have raised a type error. We also demonstrate string aggregation on
-- the still-Utf8 sex field for contrast.

ASSERT ROW_COUNT = 1
ASSERT VALUE dob_sum = 78832050
ASSERT VALUE dob_min = 19550911
ASSERT VALUE dob_max = 19880403
ASSERT VALUE male_count = 1
ASSERT VALUE female_count = 3
SELECT
    SUM(pid_7)                                       AS dob_sum,
    MIN(pid_7)                                       AS dob_min,
    MAX(pid_7)                                       AS dob_max,
    SUM(CASE WHEN pid_8 = 'M' THEN 1 ELSE 0 END)     AS male_count,
    SUM(CASE WHEN pid_8 = 'F' THEN 1 ELSE 0 END)     AS female_count
FROM {{zone_name}}.hl7_demos.chem_panels_typed;


-- ============================================================================
-- Query 4: Extract a specific OBX value via JSON path on obx_5
-- ============================================================================
-- repeating_segment_mode = 'to_json' produced obx_5 as a JSON array string
-- like '["88","14","0.9","140","4.1","102","25","9.4","7.0","4.2","0.6",
-- "78","22","28"]'. Element 0 is glucose for our CMP order. We pull element
-- zero per row and assert each patient's expected glucose.

ASSERT ROW_COUNT = 4
ASSERT VALUE first_glucose = '88'  WHERE df_file_name LIKE '%msg20240301a%'
ASSERT VALUE first_glucose = '182' WHERE df_file_name LIKE '%msg20240315b%'
ASSERT VALUE first_glucose = '96'  WHERE df_file_name LIKE '%msg20240402c%'
ASSERT VALUE first_glucose = '92'  WHERE df_file_name LIKE '%msg20240418d%'
SELECT
    df_file_name,
    get_json_object(obx_5, '$[0]') AS first_glucose
FROM {{zone_name}}.hl7_demos.chem_panels_typed
ORDER BY df_file_name;


-- ============================================================================
-- Query 5: array_length(obx_5) == OBX segment count per message
-- ============================================================================
-- Every CMP carries exactly 14 OBX rows. With to_json that means
-- json_array_length(obx_5) == 14 for every input message.

ASSERT ROW_COUNT = 4
ASSERT VALUE n_analytes = 14
SELECT
    df_file_name,
    json_array_length(obx_5) AS n_analytes
FROM {{zone_name}}.hl7_demos.chem_panels_typed
ORDER BY df_file_name;


-- ============================================================================
-- Query 6: Typed-vs-untyped contrast — the "without infer_timestamps" case
-- ============================================================================
-- Without infer_timestamps = 'true' the table would have given us:
--     msh_7  STRING ("20240301094500")
-- and the predicate `msh_7 > TIMESTAMP '2024-03-15 00:00:00'` from Q2 would
-- have either been a string-vs-timestamp coercion (engine-defined) or a
-- silent string compare ('20240301094500' > '2024-03-15 00:00:00' is true,
-- but that's lexicographic luck, not a real timestamp filter).
--
-- This query proves the real type by using a *typed* function (DATE_TRUNC).
-- DATE_TRUNC requires a Timestamp / Date input; if msh_7 were Utf8 the
-- engine would error. We also verify the truncated month for one row.

ASSERT ROW_COUNT = 4
ASSERT VALUE month_start = '2024-03-01T00:00:00' WHERE df_file_name LIKE '%msg20240301a%'
ASSERT VALUE month_start = '2024-04-01T00:00:00' WHERE df_file_name LIKE '%msg20240418d%'
SELECT
    df_file_name,
    CAST(DATE_TRUNC('month', msh_7) AS VARCHAR) AS month_start
FROM {{zone_name}}.hl7_demos.chem_panels_typed
ORDER BY df_file_name;


-- ============================================================================
-- Query 7: Abnormal-flag detection through the to_json column
-- ============================================================================
-- obx_8 is now a JSON array of "N"/"H"/"L" tokens — one per OBX. A simple
-- LIKE '%"H"%' test catches any panel with at least one high analyte.
--   - ANDERSON (msg20240301a):  all-normal     → no row
--   - BERGEN   (msg20240315b):  glucose=H      → 1 row
--   - CHEN     (msg20240402c):  BUN/Cre/K=H    → 1 row
--   - DAVIES   (msg20240418d):  AST/ALT=H      → 1 row

ASSERT ROW_COUNT = 3
ASSERT VALUE has_high = 1 WHERE df_file_name LIKE '%msg20240315b%'
ASSERT VALUE has_high = 1 WHERE df_file_name LIKE '%msg20240402c%'
ASSERT VALUE has_high = 1 WHERE df_file_name LIKE '%msg20240418d%'
SELECT
    df_file_name,
    1 AS has_high
FROM {{zone_name}}.hl7_demos.chem_panels_typed
WHERE obx_8 LIKE '%"H"%'
ORDER BY df_file_name;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting: row count, typed-timestamp evidence, integer aggregation
-- evidence, JSON-array invariants, and time-window evidence — all in one
-- result set so a skeptical reviewer can scan PASS/FAIL at a glance.

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'after_mid_march_3'
ASSERT VALUE result = 'PASS' WHERE check_name = 'all_year_2024'
ASSERT VALUE result = 'PASS' WHERE check_name = 'msg_count_4'
ASSERT VALUE result = 'PASS' WHERE check_name = 'msh7_is_timestamp'
ASSERT VALUE result = 'PASS' WHERE check_name = 'obx5_json_total_56'
ASSERT VALUE result = 'PASS' WHERE check_name = 'pid7_is_integer'
SELECT check_name, result FROM (

    -- 1. 4 ORU messages ingested
    SELECT 'msg_count_4' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.chem_panels_typed) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- 2. msh_7 is a real Timestamp (EXTRACT works only on temporal types)
    SELECT 'msh7_is_timestamp' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.chem_panels_typed
                       WHERE EXTRACT(YEAR FROM msh_7) = 2024) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- 3. pid_7 is Int64 (SUM works only on numeric types)
    SELECT 'pid7_is_integer' AS check_name,
           CASE WHEN (SELECT SUM(pid_7) FROM {{zone_name}}.hl7_demos.chem_panels_typed) = 78832050
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- 4. Every OBX-5 array has 14 elements (4 messages × 14 = 56)
    SELECT 'obx5_json_total_56' AS check_name,
           CASE WHEN (SELECT SUM(json_array_length(obx_5))
                       FROM {{zone_name}}.hl7_demos.chem_panels_typed) = 56
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- 5. 3 messages strictly after 2024-03-15 (typed timestamp filter)
    SELECT 'after_mid_march_3' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.hl7_demos.chem_panels_typed
                       WHERE msh_7 > TIMESTAMP '2024-03-15 00:00:00') = 3
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- 6. All 4 messages were sent in 2024 (proves msh_7 is real Timestamp)
    SELECT 'all_year_2024' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT EXTRACT(YEAR FROM msh_7))
                       FROM {{zone_name}}.hl7_demos.chem_panels_typed) = 1
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
