-- ============================================================================
-- Iceberg V3 — Clinical Lab NULL Edge Cases — Queries
-- ============================================================================
-- Demonstrates NULL handling edge cases on a native Iceberg V3 table.
-- 50 lab results with intentional NULLs across 7 columns. Each query
-- exercises a different NULL-related SQL pattern, proving the engine
-- handles V3 NULL statistics and predicate evaluation correctly.
-- ============================================================================


-- ============================================================================
-- Query 1: Full Table Scan — Baseline with NULL Inventory
-- ============================================================================
-- 50 rows total. Count non-NULL values per column to establish the
-- NULL distribution before testing specific patterns.

ASSERT ROW_COUNT = 1
ASSERT VALUE total = 50
ASSERT VALUE null_result = 5
ASSERT VALUE null_unit = 2
ASSERT VALUE null_ref_low = 3
ASSERT VALUE null_critical = 12
ASSERT VALUE null_technician = 7
ASSERT VALUE null_notes = 37
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN result_value IS NULL THEN 1 ELSE 0 END) AS null_result,
    SUM(CASE WHEN unit IS NULL THEN 1 ELSE 0 END) AS null_unit,
    SUM(CASE WHEN reference_low IS NULL THEN 1 ELSE 0 END) AS null_ref_low,
    SUM(CASE WHEN is_critical IS NULL THEN 1 ELSE 0 END) AS null_critical,
    SUM(CASE WHEN lab_technician IS NULL THEN 1 ELSE 0 END) AS null_technician,
    SUM(CASE WHEN notes IS NULL THEN 1 ELSE 0 END) AS null_notes
FROM {{zone_name}}.iceberg.lab_results;


-- ============================================================================
-- Query 2: COUNT(*) vs COUNT(column) — NULL Exclusion Proof
-- ============================================================================
-- COUNT(*) counts all rows; COUNT(column) excludes NULLs. The difference
-- reveals exactly how many NULLs each column has.

ASSERT ROW_COUNT = 1
ASSERT VALUE count_star = 50
ASSERT VALUE count_result = 45
ASSERT VALUE count_unit = 48
ASSERT VALUE count_notes = 13
SELECT
    COUNT(*) AS count_star,
    COUNT(result_value) AS count_result,
    COUNT(unit) AS count_unit,
    COUNT(notes) AS count_notes
FROM {{zone_name}}.iceberg.lab_results;


-- ============================================================================
-- Query 3: COALESCE — Fill Missing Values with Defaults
-- ============================================================================
-- COALESCE replaces NULLs with sensible defaults. result_value=-1 flags
-- pending tests; lab_technician='Automated' identifies machine runs.

ASSERT ROW_COUNT = 1
ASSERT VALUE coalesced_to_default = 5
ASSERT VALUE automated_count = 7
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN COALESCE(result_value, -1) = -1 THEN 1 ELSE 0 END) AS coalesced_to_default,
    SUM(CASE WHEN COALESCE(lab_technician, 'Automated') = 'Automated' THEN 1 ELSE 0 END) AS automated_count
FROM {{zone_name}}.iceberg.lab_results;


-- ============================================================================
-- Query 4: NULLIF — Convert Zero to NULL
-- ============================================================================
-- NULLIF(is_critical, 0) returns NULL when is_critical=0. Combined with
-- already-NULL is_critical values, this counts both normal and unassessed.

ASSERT ROW_COUNT = 1
ASSERT VALUE nullif_zero_or_null = 26
SELECT
    SUM(CASE WHEN NULLIF(is_critical, 0) IS NULL THEN 1 ELSE 0 END) AS nullif_zero_or_null
FROM {{zone_name}}.iceberg.lab_results;


-- ============================================================================
-- Query 5: IS NULL / IS NOT NULL — Filter Pending Tests
-- ============================================================================
-- Pending tests have NULL result_value. These 5 samples need retesting.

ASSERT ROW_COUNT = 5
SELECT
    sample_id,
    patient_name,
    test_name,
    collected_date,
    notes
FROM {{zone_name}}.iceberg.lab_results
WHERE result_value IS NULL
ORDER BY sample_id;


-- ============================================================================
-- Query 6: Aggregates with NULLs — AVG Ignores NULLs
-- ============================================================================
-- AVG, MIN, MAX, SUM all skip NULL result_values. The average is computed
-- over 45 non-NULL results, not all 50 rows.

ASSERT ROW_COUNT = 1
ASSERT VALUE avg_result = 100.73
ASSERT VALUE min_result = 0.1
ASSERT VALUE max_result = 567.17
ASSERT VALUE sum_result = 4532.72
SELECT
    ROUND(AVG(result_value), 2) AS avg_result,
    ROUND(MIN(result_value), 2) AS min_result,
    ROUND(MAX(result_value), 2) AS max_result,
    ROUND(SUM(result_value), 2) AS sum_result
FROM {{zone_name}}.iceberg.lab_results;


-- ============================================================================
-- Query 7: GROUP BY with NULL — Technician Assignment
-- ============================================================================
-- NULL lab_technician values form their own group in GROUP BY. This shows
-- 7 automated (NULL) runs grouped alongside 5 named technicians.

ASSERT ROW_COUNT = 6
ASSERT VALUE cnt = 7 WHERE lab_technician IS NULL
ASSERT VALUE cnt = 9 WHERE lab_technician = 'Dr. Patel'
ASSERT VALUE cnt = 9 WHERE lab_technician = 'Dr. Smith'
SELECT
    lab_technician,
    COUNT(*) AS cnt
FROM {{zone_name}}.iceberg.lab_results
GROUP BY lab_technician
ORDER BY lab_technician NULLS FIRST;


-- ============================================================================
-- Query 8: CASE with NULLs — Critical Status Classification
-- ============================================================================
-- is_critical has 3 states: 1 (critical), 0 (normal), NULL (unassessed).
-- CASE handles all three explicitly.

ASSERT ROW_COUNT = 1
ASSERT VALUE critical_count = 24
ASSERT VALUE normal_count = 14
ASSERT VALUE unknown_count = 12
SELECT
    SUM(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN is_critical = 0 THEN 1 ELSE 0 END) AS normal_count,
    SUM(CASE WHEN is_critical IS NULL THEN 1 ELSE 0 END) AS unknown_count
FROM {{zone_name}}.iceberg.lab_results;


-- ============================================================================
-- Query 9: Per-Test Stats — NULLs Affect Averages Differently
-- ============================================================================
-- Tests with NULL results have fewer values contributing to AVG.
-- Cholesterol, Creatinine, Glucose, Platelet Count each have NULLs.

ASSERT ROW_COUNT = 10
ASSERT VALUE has_result = 5 WHERE test_name = 'Hemoglobin'
ASSERT VALUE has_result = 4 WHERE test_name = 'Glucose'
ASSERT VALUE has_result = 3 WHERE test_name = 'Platelet Count'
ASSERT VALUE avg_result = 11.87 WHERE test_name = 'Hemoglobin'
ASSERT VALUE avg_result = 168.75 WHERE test_name = 'Glucose'
SELECT
    test_name,
    COUNT(*) AS cnt,
    COUNT(result_value) AS has_result,
    ROUND(AVG(result_value), 2) AS avg_result
FROM {{zone_name}}.iceberg.lab_results
GROUP BY test_name
ORDER BY test_name;


-- ============================================================================
-- VERIFY: Comprehensive NULL Audit
-- ============================================================================
-- Cross-cutting verification combining all NULL-related invariants.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 50
ASSERT VALUE non_null_results = 45
ASSERT VALUE non_null_notes = 13
ASSERT VALUE distinct_tests = 10
ASSERT VALUE distinct_patients = 15
ASSERT VALUE sum_result = 4532.72
ASSERT VALUE critical_count = 24
ASSERT VALUE automated_runs = 7
SELECT
    COUNT(*) AS total_rows,
    COUNT(result_value) AS non_null_results,
    COUNT(notes) AS non_null_notes,
    COUNT(DISTINCT test_name) AS distinct_tests,
    COUNT(DISTINCT patient_name) AS distinct_patients,
    ROUND(SUM(result_value), 2) AS sum_result,
    SUM(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) AS critical_count,
    SUM(CASE WHEN lab_technician IS NULL THEN 1 ELSE 0 END) AS automated_runs
FROM {{zone_name}}.iceberg.lab_results;
