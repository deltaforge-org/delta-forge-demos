-- ============================================================================
-- FHIR Medications & Prescriptions — Queries
-- ============================================================================
-- Explore FHIR R5 MedicationRequest and Coverage resources. These queries
-- demonstrate how DeltaForge handles deeply nested FHIR structures including
-- contained resources, dosage instructions, and insurance coverage classes.
-- ============================================================================


-- ============================================================================
-- 1. PRESCRIPTION OVERVIEW — All medication orders at a glance
-- ============================================================================
-- Each MedicationRequest contains a prescription with status, intent,
-- medication reference, and patient subject. The "medication" column holds
-- the FHIR medication reference (either a CodeableConcept or a Reference
-- to a contained or external Medication resource). The "status" field
-- indicates whether the prescription is active, completed, cancelled, etc.

ASSERT ROW_COUNT = 12
ASSERT VALUE status = completed WHERE prescription_id = 'medrx0301'
ASSERT VALUE intent = order WHERE prescription_id = 'medrx0301'
ASSERT VALUE status = active WHERE prescription_id = 'medrx0302'
ASSERT VALUE status = active WHERE prescription_id = 'medrx002'
SELECT prescription_id, status, intent, medication, subject, authored_date
FROM {{zone_name}}.fhir_demos.prescriptions
ORDER BY prescription_id;


-- ============================================================================
-- 2. PRESCRIPTION STATUS DISTRIBUTION
-- ============================================================================
-- FHIR MedicationRequest.status uses a required ValueSet: active | on-hold |
-- ended | stopped | completed | cancelled | entered-in-error | draft |
-- unknown. This distribution shows the lifecycle state of prescriptions —
-- useful for pharmacy workflow dashboards.

ASSERT ROW_COUNT = 2
ASSERT VALUE rx_count = 7 WHERE status = 'active'
ASSERT VALUE rx_count = 5 WHERE status = 'completed'
SELECT status, COUNT(*) AS rx_count
FROM {{zone_name}}.fhir_demos.prescriptions
GROUP BY status
ORDER BY rx_count DESC;


-- ============================================================================
-- 3. PRESCRIPTION INTENT TYPES — Orders vs plans vs proposals
-- ============================================================================
-- FHIR MedicationRequest.intent indicates the level of authority behind the
-- prescription: proposal (suggestion), plan (intended), order (authorized),
-- original-order, reflex-order, filler-order, instance-order, option.
-- Most clinical prescriptions are "order" — authorized by a prescriber.

ASSERT ROW_COUNT = 1
ASSERT VALUE intent = order
ASSERT VALUE rx_count = 12
SELECT intent, COUNT(*) AS rx_count
FROM {{zone_name}}.fhir_demos.prescriptions
GROUP BY intent
ORDER BY rx_count DESC;


-- ============================================================================
-- 4. DOSAGE INSTRUCTIONS — Preserved as JSON for full clinical detail
-- ============================================================================
-- FHIR dosageInstruction[] is a complex array containing timing, route,
-- dose ranges, patient instructions, and additional warnings. DeltaForge
-- preserves this as a JSON blob (via json_paths) so downstream systems can
-- parse the full clinical detail. Each prescription may have multiple
-- dosage steps (e.g., escalating doses over time).

ASSERT ROW_COUNT = 12
ASSERT VALUE status = completed WHERE prescription_id = 'medrx0301'
ASSERT VALUE status = active WHERE prescription_id = 'medrx0302'
SELECT prescription_id, status, dosage_instructions
FROM {{zone_name}}.fhir_demos.prescriptions
WHERE dosage_instructions IS NOT NULL
ORDER BY prescription_id;


-- ============================================================================
-- 5. DISPENSE REQUESTS — Quantity, validity, and supply duration
-- ============================================================================
-- The dispenseRequest contains pharmacy-specific details: how many tablets
-- to dispense, how long the supply should last, validity period for refills,
-- and number of repeats allowed. This data drives pharmacy inventory and
-- refill scheduling.

ASSERT ROW_COUNT = 9
ASSERT VALUE status = completed WHERE prescription_id = 'medrx0301'
ASSERT VALUE status = active WHERE prescription_id = 'medrx0311'
SELECT prescription_id, status, dispense_request
FROM {{zone_name}}.fhir_demos.prescriptions
WHERE dispense_request IS NOT NULL
ORDER BY prescription_id;


-- ============================================================================
-- 6. CONTAINED RESOURCES — Embedded Medication definitions
-- ============================================================================
-- FHIR allows resources to be "contained" inside other resources. Many
-- MedicationRequest files contain an embedded Medication resource with the
-- drug's SNOMED code, display name, and form. This avoids needing separate
-- Medication resource files. DeltaForge preserves these as JSON blobs.

-- 9 of 12 prescriptions have embedded Medication definitions
ASSERT ROW_COUNT = 9
SELECT prescription_id, contained
FROM {{zone_name}}.fhir_demos.prescriptions
WHERE contained IS NOT NULL
ORDER BY prescription_id;


-- ============================================================================
-- 7. SCHEMA EVOLUTION — Optional fields across prescription types
-- ============================================================================
-- Different prescription types populate different optional fields. Some have
-- encounter references, reasons, substitution rules, insurance links, or
-- notes. This query shows the field coverage pattern across all prescriptions.

ASSERT ROW_COUNT = 12
ASSERT VALUE has_insurance = Y WHERE prescription_id = 'medrx0301'
ASSERT VALUE has_category = Y WHERE prescription_id = 'medrx0301'
ASSERT VALUE has_encounter = - WHERE prescription_id = 'medrx0304'
ASSERT VALUE has_reason = - WHERE prescription_id = 'medrx0304'
ASSERT VALUE df_file_name LIKE '%medicationrequest0301%' WHERE prescription_id = 'medrx0301'
SELECT prescription_id,
       CASE WHEN encounter IS NOT NULL THEN 'Y' ELSE '-' END AS has_encounter,
       CASE WHEN reason IS NOT NULL THEN 'Y' ELSE '-' END AS has_reason,
       CASE WHEN substitution IS NOT NULL THEN 'Y' ELSE '-' END AS has_substitution,
       CASE WHEN insurance IS NOT NULL THEN 'Y' ELSE '-' END AS has_insurance,
       CASE WHEN note IS NOT NULL THEN 'Y' ELSE '-' END AS has_note,
       CASE WHEN category IS NOT NULL THEN 'Y' ELSE '-' END AS has_category,
       df_file_name
FROM {{zone_name}}.fhir_demos.prescriptions
ORDER BY prescription_id;


-- ============================================================================
-- 8. INSURANCE COVERAGE — All coverage plans
-- ============================================================================
-- FHIR Coverage resources model a patient's insurance arrangement. Each
-- coverage has a status, type (insurance, self-pay, EHIC), subscriber
-- and beneficiary references, and a coverage period. The "class" array
-- contains insurance classification details (group, plan, pharmacy IDs).

ASSERT ROW_COUNT = 4
ASSERT VALUE status = active WHERE coverage_id = '9876B1'
ASSERT VALUE kind = insurance WHERE coverage_id = '9876B1'
ASSERT VALUE kind = 'self-pay' WHERE coverage_id = 'SP1234'
ASSERT VALUE status = active WHERE coverage_id = 'SP1234'
SELECT coverage_id, status, kind, type, subscriber, beneficiary, period
FROM {{zone_name}}.fhir_demos.coverage
ORDER BY coverage_id;


-- ============================================================================
-- 9. COVERAGE CLASSES — Insurance plan classification details
-- ============================================================================
-- FHIR Coverage.class[] contains a rich array of insurance classifications:
-- group number, subgroup, plan code, pharmacy benefit IDs (rxid, rxbin,
-- rxgroup, rxpcn), and sequence numbers. These are preserved as JSON for
-- pharmacy benefit manager (PBM) integration.

ASSERT ROW_COUNT = 2
ASSERT VALUE status = active WHERE coverage_id = '7546D'
ASSERT VALUE status = active WHERE coverage_id = '9876B1'
SELECT coverage_id, status, class
FROM {{zone_name}}.fhir_demos.coverage
WHERE class IS NOT NULL
ORDER BY coverage_id;


-- ============================================================================
-- 10. COVERAGE TYPE DISTRIBUTION
-- ============================================================================
-- Different coverage types in the FHIR examples: extended healthcare
-- (EHCPOL), self-pay, European Health Insurance Card. This shows the
-- variety of payment arrangements patients may have.

ASSERT ROW_COUNT = 2
ASSERT VALUE coverage_count = 3 WHERE kind = 'insurance'
ASSERT VALUE coverage_count = 1 WHERE kind = 'self-pay'
SELECT kind, COUNT(*) AS coverage_count
FROM {{zone_name}}.fhir_demos.coverage
GROUP BY kind
ORDER BY coverage_count DESC;


-- ============================================================================
-- 11. FILE PROVENANCE — Track every record to its source file
-- ============================================================================
-- Data lineage across both tables: prescriptions and coverage.

ASSERT ROW_COUNT = 16
ASSERT VALUE resource_type = Coverage WHERE resource_id = '9876B1'
ASSERT VALUE resource_type = Prescription WHERE resource_id = 'medrx0301'
SELECT 'Prescription' AS resource_type, prescription_id AS resource_id, df_file_name
FROM {{zone_name}}.fhir_demos.prescriptions
UNION ALL
SELECT 'Coverage' AS resource_type, coverage_id AS resource_id, df_file_name
FROM {{zone_name}}.fhir_demos.coverage
ORDER BY resource_type, resource_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: prescription + coverage counts, field population,
-- and JSON preservation across both FHIR resource types.

ASSERT ROW_COUNT = 8
ASSERT VALUE result = PASS WHERE check_name = 'prescription_count_12'
ASSERT VALUE result = PASS WHERE check_name = 'coverage_count_4'
ASSERT VALUE result = PASS WHERE check_name = 'rx_status_populated'
ASSERT VALUE result = PASS WHERE check_name = 'rx_intent_populated'
ASSERT VALUE result = PASS WHERE check_name = 'column_mapping_rx_id'
ASSERT VALUE result = PASS WHERE check_name = 'coverage_status_populated'
ASSERT VALUE result = PASS WHERE check_name = 'contained_resources_exist'
ASSERT VALUE result = PASS WHERE check_name = 'file_metadata_rx'
SELECT check_name, result FROM (

    -- Check 1: Prescription count = 12
    SELECT 'prescription_count_12' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.prescriptions) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 2: Coverage count = 4
    SELECT 'coverage_count_4' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.coverage) = 4
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 3: All prescriptions have status
    SELECT 'rx_status_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.prescriptions WHERE status IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 4: All prescriptions have intent
    SELECT 'rx_intent_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.prescriptions WHERE intent IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 5: Column mapping — prescription_id exists
    SELECT 'column_mapping_rx_id' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.prescriptions WHERE prescription_id IS NOT NULL) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 6: Coverage has status
    SELECT 'coverage_status_populated' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.coverage WHERE status IS NULL) = 0
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 7: Exactly 9 prescriptions have contained resources (embedded Medication definitions)
    SELECT 'contained_resources_exist' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.prescriptions WHERE contained IS NOT NULL) = 9
                THEN 'PASS' ELSE 'FAIL' END AS result

    UNION ALL

    -- Check 8: File metadata on prescriptions
    SELECT 'file_metadata_rx' AS check_name,
           CASE WHEN (SELECT COUNT(*) FROM {{zone_name}}.fhir_demos.prescriptions WHERE df_file_name IS NOT NULL) = 12
                THEN 'PASS' ELSE 'FAIL' END AS result

) checks
ORDER BY check_name;
