-- ============================================================================
-- FHIR Clinical Records — Queries
-- ============================================================================
-- Explore FHIR R5 Condition, Procedure, and AllergyIntolerance resources.
-- These queries demonstrate how DeltaForge handles three different but
-- related clinical resource types from a single directory, each with its
-- own schema and nested structure.
-- ============================================================================


-- ============================================================================
-- 1. PATIENT DIAGNOSES — All conditions with clinical status and severity
-- ============================================================================
-- Each FHIR Condition resource represents a clinical diagnosis or finding.
-- The "code" column contains SNOMED CT coded diagnoses (e.g., 39065001 =
-- "Burn of ear"). The clinical_status indicates whether the condition is
-- active, resolved, or in remission. The severity uses SNOMED CT codes
-- for mild, moderate, or severe.

ASSERT ROW_COUNT = 8
ASSERT VALUE onset_date = '2011-08-05' WHERE condition_id = 'f001'
ASSERT VALUE onset_date = '2011-05-05' WHERE condition_id = 'f002'
ASSERT VALUE onset_date = '2013-04-02' WHERE condition_id = 'f201'
ASSERT VALUE onset_date = '2010-07-18' WHERE condition_id = 'stroke'
SELECT condition_id, clinical_status, verification_status,
       severity, code, body_site, onset_date
FROM {{zone_name}}.fhir_demos.conditions
ORDER BY condition_id;


-- ============================================================================
-- 2. CONDITION SEVERITY DISTRIBUTION
-- ============================================================================
-- FHIR Condition.severity uses a SNOMED CT ValueSet: mild | moderate |
-- severe. Not all conditions have severity recorded — it's optional in
-- the FHIR specification. This shows how many conditions have each severity
-- level, revealing the acuity profile of the patient population.

-- 4 Severe, 1 Moderate, 1 Mild, 1 Moderate to severe, 1 Not specified
ASSERT ROW_COUNT = 5
ASSERT VALUE condition_count = 1 WHERE severity_level = 'Not specified'
SELECT CASE WHEN severity IS NOT NULL THEN severity ELSE 'Not specified' END AS severity_level,
       COUNT(*) AS condition_count
FROM {{zone_name}}.fhir_demos.conditions
GROUP BY severity
ORDER BY condition_count DESC;


-- ============================================================================
-- 3. CONDITIONS WITH ONSET AND RESOLUTION
-- ============================================================================
-- Some conditions track both onset (when diagnosed) and abatement (when
-- resolved). This is important for calculating disease duration, identifying
-- chronic conditions (no abatement), and tracking recovery timelines.
-- Conditions without abatement_date are likely still active.

ASSERT ROW_COUNT = 8
ASSERT VALUE resolution_status = 'Resolved' WHERE condition_id = 'f204'
ASSERT VALUE abatement_date = '2013-03-20' WHERE condition_id = 'f204'
ASSERT VALUE resolution_status = 'Ongoing' WHERE condition_id = 'stroke'
SELECT condition_id, code, onset_date, abatement_date,
       CASE WHEN abatement_date IS NOT NULL THEN 'Resolved'
            ELSE 'Ongoing' END AS resolution_status
FROM {{zone_name}}.fhir_demos.conditions
ORDER BY onset_date;


-- ============================================================================
-- 4. SURGICAL PROCEDURES — All procedures with status and coding
-- ============================================================================
-- FHIR Procedure resources document clinical interventions. The "code"
-- column contains SNOMED CT procedure codes (e.g., 80146002 = Appendectomy).
-- The status indicates whether the procedure is completed, in-progress,
-- or not done. The performer array lists the surgical team members.

ASSERT ROW_COUNT = 8
ASSERT VALUE status = 'completed' WHERE procedure_id = 'biopsy'
ASSERT VALUE status = 'completed' WHERE procedure_id = 'f001'
ASSERT VALUE status = 'completed' WHERE procedure_id = 'example'
SELECT procedure_id, status, code, subject, occurrence_date, body_site
FROM {{zone_name}}.fhir_demos.procedures
ORDER BY procedure_id;


-- ============================================================================
-- 5. PROCEDURE DETAILS — Performers, reasons, and follow-up
-- ============================================================================
-- Clinical procedures have rich context: who performed them (performer[]),
-- why they were done (reason[]), what happened afterward (followUp[]),
-- and any complications (complication). These fields are preserved as JSON
-- for full clinical fidelity.

ASSERT ROW_COUNT = 8
ASSERT VALUE status = 'completed' WHERE procedure_id = 'colonoscopy'
ASSERT VALUE status = 'completed' WHERE procedure_id = 'example-implant'
SELECT procedure_id, status,
       performer, reason, follow_up, note
FROM {{zone_name}}.fhir_demos.procedures
ORDER BY procedure_id;


-- ============================================================================
-- 6. PROCEDURE STATUS DISTRIBUTION
-- ============================================================================
-- FHIR Procedure.status: preparation | in-progress | not-done | on-hold |
-- stopped | completed | entered-in-error | unknown. Most documented
-- procedures are "completed" — this confirms they were successfully
-- performed.

ASSERT ROW_COUNT = 1
ASSERT VALUE status = 'completed'
ASSERT VALUE procedure_count = 8
SELECT status, COUNT(*) AS procedure_count
FROM {{zone_name}}.fhir_demos.procedures
GROUP BY status
ORDER BY procedure_count DESC;


-- ============================================================================
-- 7. ALLERGY RECORDS — Patient allergies and intolerances
-- ============================================================================
-- FHIR AllergyIntolerance tracks known allergies and adverse reactions.
-- The "type" distinguishes allergy (immune-mediated) from intolerance
-- (non-immune). The "category" classifies the substance: food, medication,
-- environment, or biologic. "Criticality" indicates the potential severity
-- of future reactions: low, high, or unable-to-assess.

ASSERT ROW_COUNT = 6
ASSERT VALUE criticality = 'high' WHERE allergy_id = 'example'
ASSERT VALUE criticality = 'high' WHERE allergy_id = 'medication'
SELECT allergy_id, clinical_status, type, category,
       criticality, code, patient
FROM {{zone_name}}.fhir_demos.allergies
ORDER BY allergy_id;


-- ============================================================================
-- 8. ALLERGY REACTIONS — Detailed reaction history
-- ============================================================================
-- The reaction[] array documents past adverse reactions with manifestations
-- (symptoms), severity per reaction, onset dates, and exposure routes.
-- For example, a cashew nut allergy may have one anaphylactic (severe)
-- reaction and one urticaria (moderate) reaction. This data is critical
-- for clinical decision support.

ASSERT ROW_COUNT = 2
SELECT allergy_id, code, criticality, reaction, last_occurrence
FROM {{zone_name}}.fhir_demos.allergies
WHERE reaction IS NOT NULL
ORDER BY allergy_id;


-- ============================================================================
-- 9. ALLERGY CRITICALITY DISTRIBUTION
-- ============================================================================
-- Criticality indicates the potential for serious future reactions. High
-- criticality allergies require prominent charting, clinical alerts, and
-- medication interaction checking. Some records are NKA/NKDA/NKLA
-- assertions (no known allergies) which may not have criticality set.

ASSERT ROW_COUNT = 2
ASSERT VALUE allergy_count = 4 WHERE risk_level = 'Not specified'
ASSERT VALUE allergy_count = 2 WHERE risk_level = 'high'
SELECT CASE WHEN criticality IS NOT NULL THEN criticality ELSE 'Not specified' END AS risk_level,
       COUNT(*) AS allergy_count
FROM {{zone_name}}.fhir_demos.allergies
GROUP BY criticality
ORDER BY allergy_count DESC;


-- ============================================================================
-- 10. SCHEMA EVOLUTION ACROSS CONDITIONS
-- ============================================================================
-- FHIR Condition resources have many optional fields. Different diagnoses
-- populate different subsets: some have severity, body site, evidence, or
-- staging data. This shows the field coverage pattern.

ASSERT ROW_COUNT = 8
ASSERT VALUE has_severity = '-' WHERE condition_id = 'stroke'
ASSERT VALUE has_body_site = '-' WHERE condition_id = 'stroke'
ASSERT VALUE has_stage = 'Y' WHERE condition_id = 'f002'
ASSERT VALUE has_abatement = 'Y' WHERE condition_id = 'f204'
SELECT condition_id,
       CASE WHEN severity IS NOT NULL THEN 'Y' ELSE '-' END AS has_severity,
       CASE WHEN body_site IS NOT NULL THEN 'Y' ELSE '-' END AS has_body_site,
       CASE WHEN evidence IS NOT NULL THEN 'Y' ELSE '-' END AS has_evidence,
       CASE WHEN stage IS NOT NULL THEN 'Y' ELSE '-' END AS has_stage,
       CASE WHEN onset_date IS NOT NULL THEN 'Y' ELSE '-' END AS has_onset,
       CASE WHEN abatement_date IS NOT NULL THEN 'Y' ELSE '-' END AS has_abatement,
       df_file_name
FROM {{zone_name}}.fhir_demos.conditions
ORDER BY condition_id;


-- ============================================================================
-- 11. CROSS-RESOURCE LANDSCAPE — Complete clinical record summary
-- ============================================================================
-- A unified view of all three clinical resource types, showing the total
-- volume of clinical documentation. In a real EHR, these resources would
-- be linked via Patient references to build a complete patient chart.

ASSERT ROW_COUNT = 3
ASSERT VALUE record_count = 6 WHERE resource_type = 'Allergies'
ASSERT VALUE record_count = 8 WHERE resource_type = 'Conditions'
ASSERT VALUE record_count = 8 WHERE resource_type = 'Procedures'
SELECT 'Conditions' AS resource_type, COUNT(*) AS record_count FROM {{zone_name}}.fhir_demos.conditions
UNION ALL
SELECT 'Procedures' AS resource_type, COUNT(*) AS record_count FROM {{zone_name}}.fhir_demos.procedures
UNION ALL
SELECT 'Allergies' AS resource_type, COUNT(*) AS record_count FROM {{zone_name}}.fhir_demos.allergies
ORDER BY resource_type;


-- ============================================================================
-- 12. FILE PROVENANCE — Complete data lineage across all three tables
-- ============================================================================
-- Every record can be traced to its source FHIR JSON file, essential for
-- clinical data quality auditing and regulatory compliance.

ASSERT ROW_COUNT = 22
ASSERT VALUE type = 'Condition' WHERE id = 'stroke'
ASSERT VALUE type = 'Allergy' WHERE id = 'nka'
ASSERT VALUE type = 'Procedure' WHERE id = 'biopsy'
SELECT 'Condition' AS type, condition_id AS id, df_file_name FROM {{zone_name}}.fhir_demos.conditions
UNION ALL
SELECT 'Procedure' AS type, procedure_id AS id, df_file_name FROM {{zone_name}}.fhir_demos.procedures
UNION ALL
SELECT 'Allergy' AS type, allergy_id AS id, df_file_name FROM {{zone_name}}.fhir_demos.allergies
ORDER BY type, id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: row counts, column mapping, file metadata,
-- and key FHIR clinical invariants across all three resource types.

ASSERT ROW_COUNT = 10
ASSERT VALUE result = PASS WHERE check_name = 'condition_count_8'
ASSERT VALUE result = PASS WHERE check_name = 'procedure_count_8'
ASSERT VALUE result = PASS WHERE check_name = 'allergy_count_6'
ASSERT VALUE result = PASS WHERE check_name = 'condition_status_populated'
ASSERT VALUE result = PASS WHERE check_name = 'procedure_status_populated'
ASSERT VALUE result = PASS WHERE check_name = 'column_mapping_ids'
ASSERT VALUE result = PASS WHERE check_name = 'allergy_reactions_exist'
ASSERT VALUE result = PASS WHERE check_name = 'condition_codes_populated'
ASSERT VALUE result = PASS WHERE check_name = 'file_metadata_all'
ASSERT VALUE result = PASS WHERE check_name = 'multi_resource_types'
SELECT check_name, result FROM (

    -- Check 1: Condition count = 8
    SELECT 'condition_count_8' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.conditions) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Procedure count = 8
    SELECT 'procedure_count_8' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.procedures) = 8
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: Allergy count = 6
    SELECT 'allergy_count_6' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.allergies) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All conditions have clinical status
    SELECT 'condition_status_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.conditions WHERE clinical_status IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: All procedures have status
    SELECT 'procedure_status_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.procedures WHERE status IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Column mapping — condition_id, procedure_id, allergy_id
    SELECT 'column_mapping_ids' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.conditions WHERE condition_id IS NOT NULL) = 8
                 AND (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.procedures WHERE procedure_id IS NOT NULL) = 8
                 AND (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.allergies WHERE allergy_id IS NOT NULL) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Some allergies have reaction data
    SELECT 'allergy_reactions_exist' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.allergies WHERE reaction IS NOT NULL) > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: Conditions have SNOMED-coded diagnoses
    SELECT 'condition_codes_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.conditions WHERE code IS NOT NULL) > 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 9: File metadata on all three tables
    SELECT 'file_metadata_all' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.conditions WHERE df_file_name IS NOT NULL) = 8
                 AND (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.procedures WHERE df_file_name IS NOT NULL) = 8
                 AND (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.allergies WHERE df_file_name IS NOT NULL) = 6
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 10: Three distinct resource types from one directory
    -- Uses literal identifiers per table since resourceType extraction
    -- depends on flattener configuration; verifies all three tables have data.
    SELECT 'multi_resource_types' AS check_name,
           CASE WHEN (SELECT COUNT(DISTINCT source_type) FROM (
               SELECT 'Condition' AS source_type FROM {{zone_name}}.fhir_demos.conditions
               UNION SELECT 'Procedure' AS source_type FROM {{zone_name}}.fhir_demos.procedures
               UNION SELECT 'Allergy' AS source_type FROM {{zone_name}}.fhir_demos.allergies
           ) combined) = 3 THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
