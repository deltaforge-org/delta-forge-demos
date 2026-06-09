-- ============================================================================
-- Iceberg V3 Equality Delete Files — Queries
-- ============================================================================
-- Demonstrates Iceberg V3 equality delete file handling: a healthcare EHR
-- dataset with 500 original patient visits where 4 patients exercised GDPR
-- right to erasure. Equality deletes keyed on patient_id remove 55 rows,
-- leaving 445 visible visits. Exercises equality delete reader path, V3
-- metadata chain parsing, and post-delete aggregation correctness.
-- ============================================================================


-- ============================================================================
-- Query 1: Post-Delete Row Count
-- ============================================================================
-- Verifies that DeltaForge correctly applies equality deletes. The data
-- file contains 500 rows, but the equality delete file removes all rows
-- matching 4 patient_id values (55 rows total), leaving 445 visible.

ASSERT ROW_COUNT = 445
SELECT * FROM {{zone_name}}.iceberg_demos.patient_visits;


-- ============================================================================
-- Query 2: GDPR Patients Completely Removed
-- ============================================================================
-- Confirms that the 4 GDPR-erased patients have zero rows visible. The
-- equality delete file contains these patient_id values, so every row
-- matching them must be excluded from results.

ASSERT ROW_COUNT = 0
SELECT *
FROM {{zone_name}}.iceberg_demos.patient_visits
WHERE patient_id IN ('P-0012', 'P-0025', 'P-0041', 'P-0067');


-- ============================================================================
-- Query 3: Per-Hospital Visit Counts
-- ============================================================================
-- Post-delete distribution across 5 hospitals. Totals reflect removal of
-- GDPR patients' visits from each hospital they visited.

ASSERT ROW_COUNT = 5
ASSERT VALUE visit_count = 96 WHERE hospital = 'Cleveland-Clinic-OH'
ASSERT VALUE visit_count = 80 WHERE hospital = 'Johns-Hopkins-Baltimore'
ASSERT VALUE visit_count = 93 WHERE hospital = 'Mass-General-Boston'
ASSERT VALUE visit_count = 84 WHERE hospital = 'Mayo-Clinic-Rochester'
ASSERT VALUE visit_count = 92 WHERE hospital = 'Mount-Sinai-NYC'
SELECT
    hospital,
    COUNT(*) AS visit_count
FROM {{zone_name}}.iceberg_demos.patient_visits
GROUP BY hospital
ORDER BY hospital;


-- ============================================================================
-- Query 4: Distinct Patient Count
-- ============================================================================
-- 4 GDPR patients removed from the original 79, leaving 75 distinct patients.

ASSERT VALUE patient_count = 75
SELECT
    COUNT(DISTINCT patient_id) AS patient_count
FROM {{zone_name}}.iceberg_demos.patient_visits;


-- ============================================================================
-- Query 5: Per-Department Distribution
-- ============================================================================
-- Visit counts by department after equality deletes are applied.

ASSERT ROW_COUNT = 8
ASSERT VALUE visit_count = 52 WHERE department = 'Cardiology'
ASSERT VALUE visit_count = 58 WHERE department = 'Dermatology'
ASSERT VALUE visit_count = 50 WHERE department = 'Emergency'
ASSERT VALUE visit_count = 63 WHERE department = 'Neurology'
ASSERT VALUE visit_count = 59 WHERE department = 'Oncology'
ASSERT VALUE visit_count = 63 WHERE department = 'Orthopedics'
ASSERT VALUE visit_count = 55 WHERE department = 'Pediatrics'
ASSERT VALUE visit_count = 45 WHERE department = 'Radiology'
SELECT
    department,
    COUNT(*) AS visit_count
FROM {{zone_name}}.iceberg_demos.patient_visits
GROUP BY department
ORDER BY department;


-- ============================================================================
-- Query 6: Emergency Visits by Hospital
-- ============================================================================
-- Filters on boolean is_emergency column combined with hospital grouping.

ASSERT ROW_COUNT = 5
ASSERT VALUE emergency_count = 10 WHERE hospital = 'Cleveland-Clinic-OH'
ASSERT VALUE emergency_count = 9 WHERE hospital = 'Johns-Hopkins-Baltimore'
ASSERT VALUE emergency_count = 16 WHERE hospital = 'Mass-General-Boston'
ASSERT VALUE emergency_count = 5 WHERE hospital = 'Mayo-Clinic-Rochester'
ASSERT VALUE emergency_count = 10 WHERE hospital = 'Mount-Sinai-NYC'
SELECT
    hospital,
    COUNT(*) AS emergency_count
FROM {{zone_name}}.iceberg_demos.patient_visits
WHERE is_emergency = true
GROUP BY hospital
ORDER BY hospital;


-- ============================================================================
-- Query 7: Average Treatment Cost by Hospital
-- ============================================================================
-- Floating-point aggregation per hospital after equality deletes.

ASSERT ROW_COUNT = 5
ASSERT VALUE avg_cost = 11490.21 WHERE hospital = 'Cleveland-Clinic-OH'
ASSERT VALUE avg_cost = 14292.6 WHERE hospital = 'Johns-Hopkins-Baltimore'
ASSERT VALUE avg_cost = 13725.55 WHERE hospital = 'Mass-General-Boston'
ASSERT VALUE avg_cost = 12337.53 WHERE hospital = 'Mayo-Clinic-Rochester'
ASSERT VALUE avg_cost = 14129.32 WHERE hospital = 'Mount-Sinai-NYC'
SELECT
    hospital,
    ROUND(AVG(treatment_cost), 2) AS avg_cost
FROM {{zone_name}}.iceberg_demos.patient_visits
GROUP BY hospital
ORDER BY hospital;


-- ============================================================================
-- Query 8: Total and Average Cost
-- ============================================================================
-- Grand totals across all 445 remaining visits.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_cost = 5859194.13
ASSERT VALUE avg_cost = 13166.73
SELECT
    ROUND(SUM(treatment_cost), 2) AS total_cost,
    ROUND(AVG(treatment_cost), 2) AS avg_cost
FROM {{zone_name}}.iceberg_demos.patient_visits;


-- ============================================================================
-- Query 9: Top 5 Physicians by Visit Count
-- ============================================================================
-- Attending physician workload after GDPR patient removal.

ASSERT ROW_COUNT = 5
ASSERT VALUE visit_count = 65 WHERE attending_physician = 'Dr-Chen'
ASSERT VALUE visit_count = 55 WHERE attending_physician = 'Dr-Jackson'
ASSERT VALUE visit_count = 47 WHERE attending_physician = 'Dr-Huang'
ASSERT VALUE visit_count = 46 WHERE attending_physician = 'Dr-Becker'
ASSERT VALUE visit_count = 42 WHERE attending_physician = 'Dr-Ivanova'
SELECT
    attending_physician,
    COUNT(*) AS visit_count
FROM {{zone_name}}.iceberg_demos.patient_visits
GROUP BY attending_physician
ORDER BY visit_count DESC, attending_physician
LIMIT 5;


-- ============================================================================
-- Query 10: Diagnosis Code Distribution
-- ============================================================================
-- ICD-10 code frequency across all remaining visits.

ASSERT ROW_COUNT = 10
ASSERT VALUE visit_count = 53 WHERE diagnosis_code = 'C34.9'
ASSERT VALUE visit_count = 36 WHERE diagnosis_code = 'E11.9'
ASSERT VALUE visit_count = 43 WHERE diagnosis_code = 'F32.9'
ASSERT VALUE visit_count = 44 WHERE diagnosis_code = 'G43.9'
ASSERT VALUE visit_count = 39 WHERE diagnosis_code = 'I25.1'
ASSERT VALUE visit_count = 51 WHERE diagnosis_code = 'J06.9'
ASSERT VALUE visit_count = 46 WHERE diagnosis_code = 'K21.0'
ASSERT VALUE visit_count = 47 WHERE diagnosis_code = 'L30.9'
ASSERT VALUE visit_count = 41 WHERE diagnosis_code = 'M54.5'
ASSERT VALUE visit_count = 45 WHERE diagnosis_code = 'N39.0'
SELECT
    diagnosis_code,
    COUNT(*) AS visit_count
FROM {{zone_name}}.iceberg_demos.patient_visits
GROUP BY diagnosis_code
ORDER BY diagnosis_code;


-- ============================================================================
-- Query 11: Distinct Entity Counts
-- ============================================================================
-- Exercises COUNT(DISTINCT ...) across the post-delete dataset.

ASSERT ROW_COUNT = 1
ASSERT VALUE visits = 445
ASSERT VALUE patients = 75
ASSERT VALUE hospitals = 5
ASSERT VALUE departments = 8
ASSERT VALUE physicians = 10
SELECT
    COUNT(DISTINCT visit_id) AS visits,
    COUNT(DISTINCT patient_id) AS patients,
    COUNT(DISTINCT hospital) AS hospitals,
    COUNT(DISTINCT department) AS departments,
    COUNT(DISTINCT attending_physician) AS physicians
FROM {{zone_name}}.iceberg_demos.patient_visits;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: total rows, entity counts, GDPR compliance,
-- and emergency visit total. A user who runs only this query can verify the
-- Iceberg V3 equality delete reader works correctly.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_rows = 445
ASSERT VALUE hospital_count = 5
ASSERT VALUE department_count = 8
ASSERT VALUE patient_count = 75
ASSERT VALUE gdpr_patient_rows = 0
ASSERT VALUE total_emergency = 50
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT hospital) AS hospital_count,
    COUNT(DISTINCT department) AS department_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    SUM(CASE WHEN patient_id IN ('P-0012', 'P-0025', 'P-0041', 'P-0067') THEN 1 ELSE 0 END) AS gdpr_patient_rows,
    SUM(CASE WHEN is_emergency = true THEN 1 ELSE 0 END) AS total_emergency
FROM {{zone_name}}.iceberg_demos.patient_visits;
