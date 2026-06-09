-- ============================================================================
-- FHIR Multi-Vendor Patients (XML) — Verification Queries
-- ============================================================================
-- Proves URI-based XML namespace resolution: ONE external table, ONE
-- xml_flatten_config with paths like `/f:Bundle/.../f:Patient/...`, matches
-- patient records from THREE vendor documents that each use a different
-- prefix convention (fhir:, fh:, default xmlns). All three resolve to the
-- same URI (http://hl7.org/fhir), so the matcher treats them uniformly.
-- ============================================================================


-- ============================================================================
-- Q1: Total patient count across all three vendor files
-- ============================================================================
-- 3 patients per file x 3 files = 9 patients. If URI resolution is broken,
-- one or more vendors will silently produce 0 rows and this count drops.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_patients = 9
SELECT COUNT(*) AS total_patients
FROM {{zone_name}}.hie.patients;


-- ============================================================================
-- Q2: Per-file row counts via df_file_name
-- ============================================================================
-- Each vendor file MUST contribute exactly 3 rows. This is the strongest
-- proof that URI resolution works for ALL three prefix conventions:
--   - vendor_a uses fhir:Patient
--   - vendor_b uses fh:Patient
--   - vendor_c uses bare <Patient> with default xmlns

ASSERT ROW_COUNT = 3
ASSERT VALUE patient_count = 3 WHERE df_file_name LIKE '%vendor_a_fhir_prefix.xml'
ASSERT VALUE patient_count = 3 WHERE df_file_name LIKE '%vendor_b_fh_prefix.xml'
ASSERT VALUE patient_count = 3 WHERE df_file_name LIKE '%vendor_c_default_namespace.xml'
SELECT df_file_name, COUNT(*) AS patient_count
FROM {{zone_name}}.hie.patients
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- Q3: Vendor A lookup - prove the `fhir:` prefix resolves to URI
-- ============================================================================
-- Andersen lives only in vendor_a_fhir_prefix.xml. Finding her by family name
-- proves that <fhir:Patient><fhir:name><fhir:family value="Andersen"/></...>
-- matched our pattern `/f:Bundle/f:entry/f:resource/f:Patient/f:name/f:family/@value`.

ASSERT ROW_COUNT = 1
ASSERT VALUE patient_id = 'vA-pat-002'
ASSERT VALUE given_name = 'Sofia'
ASSERT VALUE birth_date = '1985-09-30'
ASSERT VALUE gender = 'female'
SELECT patient_id, family_name, given_name, birth_date, gender
FROM {{zone_name}}.hie.patients
WHERE family_name = 'Andersen';


-- ============================================================================
-- Q4: Vendor B lookup - prove the `fh:` prefix resolves to the SAME URI
-- ============================================================================
-- Bukhari lives only in vendor_b_fh_prefix.xml. A different prefix (`fh:`)
-- must resolve to the same FHIR URI and match the same paths.

ASSERT ROW_COUNT = 1
ASSERT VALUE patient_id = 'vB-pat-102'
ASSERT VALUE given_name = 'Aisha'
ASSERT VALUE birth_date = '1990-06-21'
ASSERT VALUE gender = 'female'
SELECT patient_id, family_name, given_name, birth_date, gender
FROM {{zone_name}}.hie.patients
WHERE family_name = 'Bukhari';


-- ============================================================================
-- Q5: Vendor C lookup - prove the DEFAULT namespace resolves to FHIR URI
-- ============================================================================
-- Corradi lives only in vendor_c_default_namespace.xml. The elements there
-- carry no prefix at all; they inherit the URI via xmlns="http://hl7.org/fhir"
-- on the root. The matcher must look up the in-scope default namespace.

ASSERT ROW_COUNT = 1
ASSERT VALUE patient_id = 'vC-pat-202'
ASSERT VALUE given_name = 'Lucia'
ASSERT VALUE birth_date = '1955-08-14'
ASSERT VALUE gender = 'female'
SELECT patient_id, family_name, given_name, birth_date, gender
FROM {{zone_name}}.hie.patients
WHERE family_name = 'Corradi';


-- ============================================================================
-- Q6: Gender breakdown across all three vendors
-- ============================================================================
-- Uniform extraction across all three prefix conventions produces a single
-- gender distribution. Synthetic data: 5 male, 4 female.

ASSERT ROW_COUNT = 2
ASSERT VALUE patient_count = 4 WHERE gender = 'female'
ASSERT VALUE patient_count = 5 WHERE gender = 'male'
SELECT gender, COUNT(*) AS patient_count
FROM {{zone_name}}.hie.patients
GROUP BY gender
ORDER BY gender;


-- ============================================================================
-- Q7: Identifier-system distribution - one system per vendor, 3 patients each
-- ============================================================================
-- Each vendor uses a unique identifier_system URI. If any vendor's records
-- failed to extract, the corresponding system would be missing from the
-- result set. Three distinct systems, three rows each, proves end-to-end.

ASSERT ROW_COUNT = 3
ASSERT VALUE patient_count = 3 WHERE identifier_system = 'urn:oid:2.16.840.1.113883.4.1'
ASSERT VALUE patient_count = 3 WHERE identifier_system = 'https://blueridge.example/patient'
ASSERT VALUE patient_count = 3 WHERE identifier_system = 'https://coastal.example/mrn'
SELECT identifier_system, COUNT(*) AS patient_count
FROM {{zone_name}}.hie.patients
GROUP BY identifier_system
ORDER BY identifier_system;


-- ============================================================================
-- Q8: Silver layer mirrors bronze - typed Delta promotion succeeded
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE silver_count = 9
SELECT COUNT(*) AS silver_count
FROM {{zone_name}}.hie.patients_silver;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- Cross-cutting sanity check: covers row counts, silver promotion, gender
-- split, and identifier-system uniqueness in one query. Every metric here
-- depends on URI resolution succeeding for ALL three vendors.

ASSERT ROW_COUNT = 5
SELECT 'total_patients'             AS check_name, CAST(COUNT(*) AS BIGINT) AS value FROM {{zone_name}}.hie.patients
UNION ALL
SELECT 'silver_total',                              CAST(COUNT(*) AS BIGINT)         FROM {{zone_name}}.hie.patients_silver
UNION ALL
SELECT 'female_count',                              CAST(COUNT(*) AS BIGINT)         FROM {{zone_name}}.hie.patients WHERE gender = 'female'
UNION ALL
SELECT 'male_count',                                CAST(COUNT(*) AS BIGINT)         FROM {{zone_name}}.hie.patients WHERE gender = 'male'
UNION ALL
SELECT 'distinct_identifier_systems',               CAST(COUNT(DISTINCT identifier_system) AS BIGINT) FROM {{zone_name}}.hie.patients
ORDER BY check_name;
