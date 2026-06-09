-- ============================================================================
-- Demo: ORC Clinical Trials — Patient Outcome Analysis
-- ============================================================================
-- Proves NULL handling, CASE expressions, and string functions on ORC data
-- with high NULL density. COALESCE, NULLIF, IS NULL, CASE, LIKE, LENGTH.

-- ============================================================================
-- Query 1: Full Scan — 150 patients across 5 trial sites
-- ============================================================================

ASSERT ROW_COUNT = 150
SELECT *
FROM {{zone_name}}.orc_trials.patients;

-- ============================================================================
-- Query 2: NULL Inventory — count NULLs across all nullable columns
-- ============================================================================

ASSERT VALUE null_followup = 47
ASSERT VALUE null_adverse = 50
ASSERT VALUE null_notes = 30
ASSERT VALUE null_bmi = 20
ASSERT VALUE null_gender = 29
SELECT COUNT(*) FILTER (WHERE followup_score IS NULL) AS null_followup,
       COUNT(*) FILTER (WHERE adverse_event IS NULL) AS null_adverse,
       COUNT(*) FILTER (WHERE notes IS NULL) AS null_notes,
       COUNT(*) FILTER (WHERE bmi IS NULL) AS null_bmi,
       COUNT(*) FILTER (WHERE gender IS NULL) AS null_gender
FROM {{zone_name}}.orc_trials.patients;

-- ============================================================================
-- Query 3: COALESCE — fallback to baseline_score for missing followups
-- ============================================================================
-- 47 patients dropped out (NULL followup_score). COALESCE replaces with
-- baseline_score so all 150 patients have an effective score.

ASSERT ROW_COUNT = 150
SELECT patient_id, baseline_score, followup_score,
       COALESCE(followup_score, baseline_score) AS effective_score,
       CASE WHEN followup_score IS NULL THEN 'Dropout' ELSE 'Completed' END AS status
FROM {{zone_name}}.orc_trials.patients
ORDER BY patient_id;

-- ============================================================================
-- Query 3b: COALESCE — verify all 150 patients have an effective score
-- ============================================================================

ASSERT VALUE has_effective_score = 150
SELECT COUNT(*) AS has_effective_score
FROM (
    SELECT COALESCE(followup_score, baseline_score) AS effective_score
    FROM {{zone_name}}.orc_trials.patients
    WHERE COALESCE(followup_score, baseline_score) IS NOT NULL
) sub;

-- ============================================================================
-- Query 4: CASE — classify patient outcomes
-- ============================================================================
-- Improved: followup < baseline (lower = better)
-- Worsened: followup > baseline
-- Dropout: NULL followup

ASSERT ROW_COUNT = 3
ASSERT VALUE patient_count = 57 WHERE outcome = 'Improved'
ASSERT VALUE patient_count = 46 WHERE outcome = 'Worsened'
ASSERT VALUE patient_count = 47 WHERE outcome = 'Dropout'
SELECT CASE
           WHEN followup_score IS NULL THEN 'Dropout'
           WHEN followup_score < baseline_score THEN 'Improved'
           ELSE 'Worsened'
       END AS outcome,
       COUNT(*) AS patient_count
FROM {{zone_name}}.orc_trials.patients
GROUP BY CASE
             WHEN followup_score IS NULL THEN 'Dropout'
             WHEN followup_score < baseline_score THEN 'Improved'
             ELSE 'Worsened'
         END
ORDER BY patient_count DESC;

-- ============================================================================
-- Query 5: NULLIF — treat empty-string notes as NULL
-- ============================================================================
-- 30 NULL notes + 32 empty-string notes = 62 effectively empty.
-- NULLIF(notes, '') converts '' to NULL.

ASSERT VALUE effectively_empty = 62
ASSERT VALUE has_content = 88
SELECT COUNT(*) FILTER (WHERE NULLIF(notes, '') IS NULL) AS effectively_empty,
       COUNT(*) FILTER (WHERE NULLIF(notes, '') IS NOT NULL) AS has_content
FROM {{zone_name}}.orc_trials.patients;

-- ============================================================================
-- Query 6: String Functions — filter notes containing 'responded'
-- ============================================================================

ASSERT ROW_COUNT = 28
SELECT patient_id, trial_site, treatment_arm, notes
FROM {{zone_name}}.orc_trials.patients
WHERE notes LIKE '%responded%'
ORDER BY patient_id;

-- ============================================================================
-- Query 7: BMI Categories — CASE with NULL-safe bucketing
-- ============================================================================

ASSERT ROW_COUNT = 4
ASSERT VALUE patient_count = 39 WHERE bmi_category = 'Normal'
ASSERT VALUE patient_count = 34 WHERE bmi_category = 'Overweight'
ASSERT VALUE patient_count = 57 WHERE bmi_category = 'Obese'
ASSERT VALUE patient_count = 20 WHERE bmi_category = 'Unknown'
SELECT CASE
           WHEN bmi IS NULL THEN 'Unknown'
           WHEN bmi < 25.0 THEN 'Normal'
           WHEN bmi < 30.0 THEN 'Overweight'
           ELSE 'Obese'
       END AS bmi_category,
       COUNT(*) AS patient_count
FROM {{zone_name}}.orc_trials.patients
GROUP BY CASE
             WHEN bmi IS NULL THEN 'Unknown'
             WHEN bmi < 25.0 THEN 'Normal'
             WHEN bmi < 30.0 THEN 'Overweight'
             ELSE 'Obese'
         END
ORDER BY patient_count DESC;

-- ============================================================================
-- Query 8: Adverse Events — non-NULL breakdown
-- ============================================================================

ASSERT ROW_COUNT = 5
ASSERT VALUE ae_count = 28 WHERE adverse_event = 'Fatigue'
ASSERT VALUE ae_count = 26 WHERE adverse_event = 'Dizziness'
ASSERT VALUE ae_count = 21 WHERE adverse_event = 'Nausea'
SELECT adverse_event,
       COUNT(*) AS ae_count
FROM {{zone_name}}.orc_trials.patients
WHERE adverse_event IS NOT NULL
GROUP BY adverse_event
ORDER BY ae_count DESC;

-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

ASSERT ROW_COUNT = 6
ASSERT VALUE result = 'PASS' WHERE check_name = 'total_patients_150'
ASSERT VALUE result = 'PASS' WHERE check_name = 'dropouts_47'
ASSERT VALUE result = 'PASS' WHERE check_name = 'improved_57'
ASSERT VALUE result = 'PASS' WHERE check_name = 'null_bmi_20'
ASSERT VALUE result = 'PASS' WHERE check_name = 'notes_with_content_88'
SELECT check_name, result FROM (

    SELECT 'total_patients_150' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.orc_trials.patients) = 150
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'dropouts_47' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_trials.patients
               WHERE followup_score IS NULL
           ) = 47 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'improved_57' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_trials.patients
               WHERE followup_score IS NOT NULL AND followup_score < baseline_score
           ) = 57 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'null_bmi_20' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_trials.patients WHERE bmi IS NULL
           ) = 20 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'notes_with_content_88' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_trials.patients
               WHERE NULLIF(notes, '') IS NOT NULL
           ) = 88 THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    SELECT 'adverse_events_100' AS check_name,
           CASE WHEN (
               SELECT COUNT(*) FROM {{zone_name}}.orc_trials.patients
               WHERE adverse_event IS NOT NULL
           ) = 100 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
