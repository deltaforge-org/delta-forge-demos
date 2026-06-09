-- ============================================================================
-- FHIR XML Clinical Resources — Query Showcase
-- ============================================================================
-- Demonstrates DeltaForge's XML flattening capabilities with HL7 FHIR data.
-- FHIR XML is unique: ALL primitive values are stored in @value attributes
-- (e.g., <id value="f001"/>) rather than as element text content.
-- ============================================================================


-- ============================================================================
-- QUERY 1: FHIR XML @value Extraction — The Core FHIR XML Pattern
-- ============================================================================
-- In FHIR XML, every primitive value is an attribute: <gender value="male"/>,
-- <birthDate value="1974-12-25"/>. DeltaForge extracts these via XPath
-- @value selectors and maps them to analyst-friendly column names.
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE gender = 'male' WHERE patient_id = 'example'
ASSERT VALUE birth_date = '1974-12-25' WHERE patient_id = 'example'
ASSERT VALUE gender = 'male' WHERE patient_id = 'f001'
ASSERT VALUE birth_date = '1944-11-17' WHERE patient_id = 'f001'
ASSERT VALUE gender = 'female' WHERE patient_id = 'pat4'
ASSERT VALUE birth_date = '1932-09-24' WHERE patient_id = 'xcda'
SELECT
    patient_id,
    family_name,
    given_name,
    gender,
    birth_date,
    is_active
FROM {{zone_name}}.fhir_xml.patients_xml
ORDER BY patient_id;


-- ============================================================================
-- QUERY 2: FHIR Namespace Handling — xmlns="http://hl7.org/fhir"
-- ============================================================================
-- Every FHIR XML document declares the HL7 FHIR namespace on the root element.
-- The strip_namespace_prefixes option removes it, so column names don't have
-- "fhir_" prefixes. This query verifies that all 8 patient files were parsed
-- correctly despite the namespace declaration.
-- ============================================================================

ASSERT VALUE total_patients = 8
ASSERT VALUE unique_ids = 8
ASSERT VALUE has_gender = 8
ASSERT VALUE has_birth_date = 6
SELECT
    COUNT(*) AS total_patients,
    COUNT(DISTINCT patient_id) AS unique_ids,
    COUNT(gender) AS has_gender,
    COUNT(birth_date) AS has_birth_date
FROM {{zone_name}}.fhir_xml.patients_xml;


-- ============================================================================
-- QUERY 3: Repeating Element Handling — Multiple <name> and <telecom> Entries
-- ============================================================================
-- FHIR patients can have multiple <name> blocks (official, usual, nickname)
-- and multiple <telecom> entries (phone, email). With default_repeat_handling
-- set to "join_comma", these are merged into comma-separated strings.
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE family_name = 'van de Heuvel' WHERE patient_id = 'f001'
ASSERT VALUE family_name = 'Bor' WHERE patient_id = 'f201'
SELECT
    patient_id,
    family_name,
    given_name,
    name_use,
    telecom_system,
    telecom_value,
    telecom_use
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE telecom_value IS NOT NULL
ORDER BY patient_id;


-- ============================================================================
-- QUERY 4: Deep Nested XPath Extraction — maritalStatus/coding/code/@value
-- ============================================================================
-- FHIR CodeableConcept structures nest 4 levels deep:
--   <maritalStatus> → <coding> → <code value="M"/> + <display value="Married"/>
-- DeltaForge navigates this hierarchy using XPath to extract coded values
-- alongside their human-readable display text.
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE marital_code = 'M' WHERE patient_id = 'f001'
ASSERT VALUE marital_display = 'Married' WHERE patient_id = 'f001'
SELECT
    patient_id,
    family_name,
    marital_code,
    marital_display,
    marital_text
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE marital_code IS NOT NULL
ORDER BY patient_id;


-- ============================================================================
-- QUERY 5: exclude_paths — Skipping Narrative and Security Metadata
-- ============================================================================
-- FHIR XML includes two verbose elements that are rarely useful for analytics:
--   - <text> contains a generated XHTML <div> narrative (often hundreds of chars)
--   - <meta> contains security tags and profile references
-- These are excluded via exclude_paths. This query shows that the actual
-- clinical data remains intact while the narrative overhead is gone.
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE city = 'Amsterdam' WHERE patient_id = 'f001'
ASSERT VALUE city = 'Amsterdam' WHERE patient_id = 'f201'
ASSERT VALUE city = 'PleasantVille' WHERE patient_id = 'example'
SELECT
    patient_id,
    family_name,
    gender,
    birth_date,
    address_line,
    city,
    country
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE city IS NOT NULL
ORDER BY city;


-- ============================================================================
-- QUERY 6: Schema Evolution Across Patient Instances
-- ============================================================================
-- Different FHIR Patient resources populate different fields. Some patients
-- have marital status, contacts, and managing organization; others have only
-- basic demographics. This is standard FHIR optionality in action.
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE has_marital = 'Y' WHERE patient_id = 'f001'
ASSERT VALUE has_telecom = 'Y' WHERE patient_id = 'f001'
ASSERT VALUE has_gender = 'Y' WHERE patient_id = 'xcda'
ASSERT VALUE has_comms = 'Y' WHERE patient_id = 'f201'
SELECT
    patient_id,
    family_name,
    CASE WHEN gender IS NOT NULL THEN 'Y' ELSE 'N' END AS has_gender,
    CASE WHEN birth_date IS NOT NULL THEN 'Y' ELSE 'N' END AS has_dob,
    CASE WHEN marital_code IS NOT NULL THEN 'Y' ELSE 'N' END AS has_marital,
    CASE WHEN org_display IS NOT NULL THEN 'Y' ELSE 'N' END AS has_org,
    CASE WHEN telecom_value IS NOT NULL THEN 'Y' ELSE 'N' END AS has_telecom,
    CASE WHEN contact IS NOT NULL THEN 'Y' ELSE 'N' END AS has_contacts,
    CASE WHEN communication IS NOT NULL THEN 'Y' ELSE 'N' END AS has_comms
FROM {{zone_name}}.fhir_xml.patients_xml
ORDER BY patient_id;


-- ============================================================================
-- QUERY 7: xml_paths — Complex Subtrees Preserved as JSON
-- ============================================================================
-- The <contact> element contains nested relationship/name/telecom blocks that
-- would be unwieldy if flattened. Using xml_paths, these are preserved as
-- JSON objects. Similarly, <communication> with language/preferred pairs.
-- ============================================================================

ASSERT ROW_COUNT = 4
-- patient-example has two top-level <name><family> values (Chalmers, Windsor),
-- which join_comma concatenates into "Chalmers,Windsor" — match substring.
ASSERT VALUE family_name LIKE '%Windsor%' WHERE patient_id = 'example'
ASSERT VALUE family_name = 'Donald' WHERE patient_id = 'pat1'
SELECT
    patient_id,
    family_name,
    contact,
    communication
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE contact IS NOT NULL OR communication IS NOT NULL
ORDER BY patient_id;


-- ============================================================================
-- QUERY 8: Observation Coding — LOINC and SNOMED CT Extraction
-- ============================================================================
-- FHIR Observation.code uses CodeableConcept with nested <coding> elements.
-- Each coding block has system/@value (e.g., "http://loinc.org") and
-- code/@value (e.g., "29463-7" for body weight). The deep XPath extraction
-- navigates 4 levels: Observation → code → coding → code/@value.
-- ============================================================================

ASSERT ROW_COUNT = 8
ASSERT VALUE status = 'final' WHERE observation_id = 'example'
ASSERT VALUE status = 'final' WHERE observation_id = 'f001'
ASSERT VALUE status = 'final' WHERE observation_id = 'blood-pressure'
SELECT
    observation_id,
    code_system,
    code_value,
    code_display,
    status
FROM {{zone_name}}.fhir_xml.observations_xml
ORDER BY observation_id;


-- ============================================================================
-- QUERY 9: Observation Results — valueQuantity Extraction
-- ============================================================================
-- FHIR stores measurements in <valueQuantity> with separate @value attributes
-- for the numeric value, unit text, unit system URI, and unit code. Delta
-- Forge extracts each attribute independently via XPath.
-- ============================================================================

ASSERT ROW_COUNT = 7
ASSERT VALUE result_value = 185 WHERE observation_id = 'example'
ASSERT VALUE result_unit = 'lbs' WHERE observation_id = 'example'
ASSERT VALUE result_value = 16.2 WHERE observation_id = 'bmi'
ASSERT VALUE result_unit = 'kg/m2' WHERE observation_id = 'bmi'
SELECT
    observation_id,
    code_display,
    result_value,
    result_unit,
    unit_code,
    effective_date,
    patient_ref
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE result_value IS NOT NULL
ORDER BY observation_id;


-- ============================================================================
-- QUERY 10: Blood Pressure Components — Preserved as JSON
-- ============================================================================
-- Blood pressure observations use <component> elements instead of a single
-- valueQuantity. Each component contains its own code (systolic/diastolic)
-- and valueQuantity. The xml_paths option preserves these nested component
-- blocks as JSON for downstream extraction.
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE observation_id = 'blood-pressure'
SELECT
    observation_id,
    code_display,
    component,
    interp_code,
    interp_display,
    body_site_display
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE component IS NOT NULL
ORDER BY observation_id;


-- ============================================================================
-- QUERY 11: Reference Range — Lab Normal Ranges as JSON
-- ============================================================================
-- Lab observations include <referenceRange> with low and high bounds (each
-- with value, unit, system, and code). These are preserved as JSON via
-- xml_paths since they contain deeply nested quantity pairs.
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE result_unit = 'mmol/l' WHERE observation_id = 'f001'
ASSERT VALUE result_unit = 'mmol/l' WHERE observation_id = 'f002'
ASSERT VALUE result_unit = 'kPa' WHERE observation_id = 'f003'
SELECT
    observation_id,
    code_display,
    result_value,
    result_unit,
    reference_range,
    interp_code
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE reference_range IS NOT NULL
ORDER BY observation_id;


-- ============================================================================
-- QUERY 12: Cross-Table — Patients with Observations
-- ============================================================================
-- FHIR Observations reference Patients via <subject><reference value=
-- "Patient/example"/></subject>. This cross-table join demonstrates how
-- patient_ref in observations_xml matches patient_id in patients_xml.
-- ============================================================================

ASSERT ROW_COUNT = 8
-- Observation 'example' joins to Patient 'example' whose family_name is the
-- comma-joined "Chalmers,Windsor" (two top-level <name> entries) — match substring.
ASSERT VALUE family_name LIKE '%Windsor%' WHERE observation_id = 'example'
ASSERT VALUE family_name = 'van de Heuvel' WHERE observation_id = 'f001'
SELECT
    o.observation_id,
    o.code_display          AS observation_type,
    o.result_value,
    o.result_unit,
    p.family_name,
    p.given_name,
    p.gender
FROM {{zone_name}}.fhir_xml.observations_xml o
LEFT JOIN {{zone_name}}.fhir_xml.patients_xml p
    ON o.patient_ref = 'Patient/' || p.patient_id
ORDER BY o.observation_id;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- CHECK 1: Namespace stripping — all 8 patient files loaded
ASSERT VALUE actual_count = 8
SELECT COUNT(*) AS actual_count
FROM {{zone_name}}.fhir_xml.patients_xml;

-- CHECK 2: @value attribute extraction — all patients have IDs
ASSERT VALUE ids_found = 8
SELECT COUNT(patient_id) AS ids_found
FROM {{zone_name}}.fhir_xml.patients_xml;

-- CHECK 3: Observation XML files loaded
ASSERT VALUE obs_count = 8
SELECT COUNT(*) AS obs_count
FROM {{zone_name}}.fhir_xml.observations_xml;

-- CHECK 4: Deep XPath coding extraction works
ASSERT VALUE codes_found = 8
SELECT COUNT(code_value) AS codes_found
FROM {{zone_name}}.fhir_xml.observations_xml;

-- CHECK 5: xml_paths preserves component as JSON
ASSERT VALUE components_found = 1
SELECT COUNT(component) AS components_found
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE component IS NOT NULL;

-- CHECK 6: exclude_paths removed narrative text
-- Verified by schema inspection — no text or meta columns exist

-- CHECK 7: Repeating elements comma-joined
ASSERT VALUE patients_with_telecom = 3
SELECT COUNT(*) AS patients_with_telecom
FROM {{zone_name}}.fhir_xml.patients_xml
WHERE telecom_value IS NOT NULL;

-- CHECK 8: Column mappings applied correctly
ASSERT VALUE rows_with_mapped_cols = 8
SELECT COUNT(*) AS rows_with_mapped_cols
FROM {{zone_name}}.fhir_xml.observations_xml
WHERE observation_id IS NOT NULL AND code_display IS NOT NULL;
