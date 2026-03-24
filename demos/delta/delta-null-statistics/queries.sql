-- ============================================================================
-- NULL Statistics — NULL-Aware Query Optimization — Educational Queries
-- ============================================================================
-- WHAT: Delta tracks per-file NULL counts for every column. Each file's
--       metadata records how many NULLs exist in each column.
-- WHY:  When filtering IS NULL, the engine skips files with zero NULLs.
--       When filtering IS NOT NULL, it skips files where a column is all NULL.
--       This avoids reading irrelevant Parquet data entirely.
-- HOW:  NULL counts are stored in each Add action's stats field in the
--       _delta_log JSON, alongside min/max and row count statistics.
-- ============================================================================


-- ============================================================================
-- Query 1: NULL Distribution Across Batches
-- ============================================================================
-- Three batches represent different stages of patient record completion:
--   Batch 1 (ids 1-15):  Newly admitted — many NULLs
--   Batch 2 (ids 16-30): Partially completed — some NULLs
--   Batch 3 (ids 31-45): Fully completed — zero NULLs
--
-- The engine uses these NULL count differences to skip entire files
-- when querying for NULL or NOT NULL values.

ASSERT VALUE null_diagnosis = 10 WHERE batch = 'Batch 1 (admitted)'
ASSERT VALUE null_discharge = 15 WHERE batch = 'Batch 1 (admitted)'
ASSERT VALUE null_insurance = 12 WHERE batch = 'Batch 1 (admitted)'
ASSERT VALUE null_diagnosis = 0 WHERE batch = 'Batch 2 (partial)'
ASSERT VALUE null_discharge = 7 WHERE batch = 'Batch 2 (partial)'
ASSERT VALUE null_insurance = 12 WHERE batch = 'Batch 2 (partial)'
ASSERT VALUE null_diagnosis = 0 WHERE batch = 'Batch 3 (complete)'
ASSERT VALUE null_discharge = 0 WHERE batch = 'Batch 3 (complete)'
ASSERT VALUE null_insurance = 0 WHERE batch = 'Batch 3 (complete)'
ASSERT ROW_COUNT = 3
SELECT
    CASE
        WHEN id BETWEEN 1 AND 15 THEN 'Batch 1 (admitted)'
        WHEN id BETWEEN 16 AND 30 THEN 'Batch 2 (partial)'
        ELSE 'Batch 3 (complete)'
    END AS batch,
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE diagnosis_code IS NULL) AS null_diagnosis,
    COUNT(*) FILTER (WHERE discharge_date IS NULL) AS null_discharge,
    COUNT(*) FILTER (WHERE secondary_insurance IS NULL) AS null_insurance
FROM {{zone_name}}.null_demos.patient_records
GROUP BY CASE
    WHEN id BETWEEN 1 AND 15 THEN 'Batch 1 (admitted)'
    WHEN id BETWEEN 16 AND 30 THEN 'Batch 2 (partial)'
    ELSE 'Batch 3 (complete)'
END
ORDER BY batch;


-- ============================================================================
-- Query 2: Still Admitted (discharge_date IS NULL)
-- ============================================================================
-- File-level NULL count stats check:
--   - Batch 1 file: discharge null_count = 15 -> READ (has NULLs)
--   - Batch 2 file: discharge null_count = 7  -> READ (has NULLs)
--   - Batch 3 file: discharge null_count = 0  -> SKIP (no NULLs)
--
-- The engine skips the Batch 3 file entirely because it has zero NULLs
-- in discharge_date — no row in that file can match IS NULL.

ASSERT VALUE still_admitted = 22
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS still_admitted
FROM {{zone_name}}.null_demos.patient_records
WHERE discharge_date IS NULL;


-- ============================================================================
-- Query 3: Patients With Secondary Insurance (IS NOT NULL)
-- ============================================================================
-- File-level NULL count stats check for IS NOT NULL:
--   - Batch 1 file: insurance null_count = 12 (of 15) -> READ (3 non-NULL)
--   - Batch 2 file: insurance null_count = 12 (of 15) -> READ (3 non-NULL)
--   - Batch 3 file: insurance null_count = 0  (of 15) -> READ (all non-NULL)
--
-- All files are read because each has at least some non-NULL values.
-- If Batch 1 had ALL NULLs (15/15), the engine could skip it.

ASSERT VALUE has_insurance = 21
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS has_insurance
FROM {{zone_name}}.null_demos.patient_records
WHERE secondary_insurance IS NOT NULL;


-- ============================================================================
-- Query 4: Missing Diagnosis Codes
-- ============================================================================
-- File-level NULL count stats check:
--   - Batch 1 file: diagnosis null_count = 10 -> READ (has NULLs)
--   - Batch 2 file: diagnosis null_count = 0  -> SKIP (no NULLs)
--   - Batch 3 file: diagnosis null_count = 0  -> SKIP (no NULLs)
--
-- Only Batch 1 needs to be read. The engine skips 2 of 3 files.
-- This is the best-case scenario for NULL-based skipping.

ASSERT VALUE missing_diagnosis = 10
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS missing_diagnosis
FROM {{zone_name}}.null_demos.patient_records
WHERE diagnosis_code IS NULL;


-- ============================================================================
-- Query 5: Fully Complete Records
-- ============================================================================
-- Records where every optional field is populated. Batch 3 contributes all 15,
-- plus 1 from Batch 1 (Eva Johnson, id=5) who had all fields on admission.

ASSERT VALUE complete_records = 16
ASSERT ROW_COUNT = 1
SELECT COUNT(*) AS complete_records
FROM {{zone_name}}.null_demos.patient_records
WHERE diagnosis_code IS NOT NULL
  AND discharge_date IS NOT NULL
  AND secondary_insurance IS NOT NULL;


-- ============================================================================
-- Query 6: Ward-Level Admission Status
-- ============================================================================
-- Combines NULL filtering with aggregation to show operational status by ward.

ASSERT VALUE still_admitted = 8 WHERE ward = 'cardiac'
ASSERT VALUE still_admitted = 6 WHERE ward = 'general'
ASSERT VALUE still_admitted = 5 WHERE ward = 'orthopedic'
ASSERT VALUE still_admitted = 3 WHERE ward = 'surgical'
ASSERT ROW_COUNT = 4
SELECT ward,
       COUNT(*) AS total_patients,
       COUNT(*) FILTER (WHERE discharge_date IS NULL) AS still_admitted,
       COUNT(*) FILTER (WHERE discharge_date IS NOT NULL) AS discharged
FROM {{zone_name}}.null_demos.patient_records
GROUP BY ward
ORDER BY ward;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total row count is 45
ASSERT ROW_COUNT = 45
SELECT * FROM {{zone_name}}.null_demos.patient_records;

-- Verify 22 patients still admitted
ASSERT VALUE admitted = 22
SELECT COUNT(*) AS admitted FROM {{zone_name}}.null_demos.patient_records WHERE discharge_date IS NULL;

-- Verify 21 patients have secondary insurance
ASSERT VALUE insured = 21
SELECT COUNT(*) AS insured FROM {{zone_name}}.null_demos.patient_records WHERE secondary_insurance IS NOT NULL;

-- Verify 10 patients missing diagnosis
ASSERT VALUE no_diag = 10
SELECT COUNT(*) AS no_diag FROM {{zone_name}}.null_demos.patient_records WHERE diagnosis_code IS NULL;

-- Verify 16 fully complete records
ASSERT VALUE complete = 16
SELECT COUNT(*) AS complete FROM {{zone_name}}.null_demos.patient_records WHERE diagnosis_code IS NOT NULL AND discharge_date IS NOT NULL AND secondary_insurance IS NOT NULL;

-- Verify ward distribution
ASSERT VALUE cardiac = 13
SELECT COUNT(*) AS cardiac FROM {{zone_name}}.null_demos.patient_records WHERE ward = 'cardiac';

ASSERT VALUE surgical = 8
SELECT COUNT(*) AS surgical FROM {{zone_name}}.null_demos.patient_records WHERE ward = 'surgical';
