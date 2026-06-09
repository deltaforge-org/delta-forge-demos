-- ============================================================================
-- FHIR Multi-Vendor Patients (XML) — Setup Script
-- ============================================================================
-- Real-world story: a regional health information exchange (HIE) ingests
-- FHIR Patient resources from three different EHR vendors. Each vendor's
-- XML serialiser uses a different namespace prefix for the FHIR namespace
-- (http://hl7.org/fhir):
--
--   Vendor A  - Northstar Medical Group  uses  fhir:Patient
--   Vendor B  - BlueRidge Health         uses  fh:Patient
--   Vendor C  - Coastal Family Clinic    uses  default xmlns="..."  (bare <Patient>)
--
-- Without URI-based namespace resolution we would need three separate
-- external tables (or three flatten configs). With the new feature we
-- declare ONE alias `f -> http://hl7.org/fhir`, write paths as
-- `/f:Bundle/.../f:Patient/...`, and the matcher resolves all three prefix
-- conventions to the same URI. ONE table, three vendors, uniform schema.
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables - HIE landing zone';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.hie
    COMMENT 'Health Information Exchange - multi-vendor FHIR ingest';

-- ============================================================================
-- TABLE 1: patients (bronze) - one external table over all three vendor files
-- ============================================================================
-- The `namespaces` map declares `f -> http://hl7.org/fhir`. The path matcher
-- rewrites any `f:local` segment in our XPaths into `{URI}local` and then
-- compares against the runtime element identity, which is also derived from
-- the URI. Because all three vendor documents bind their elements to the
-- same FHIR URI (whether via `fhir:`, `fh:`, or the default xmlns), all
-- three match the SAME paths.
--
-- column_mappings keys MUST use the unresolved `f:`-prefixed form - that is
-- what the engine looks up. The auto-naming pipeline strips `{URI}` markers
-- and namespace prefixes in any case, but explicit mappings give us short,
-- analyst-friendly column names.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.hie.patients
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    xml_flatten_config = '{
        "row_xpath": "/f:Bundle/f:entry/f:resource/f:Patient",
        "include_paths": [
            "/f:Bundle/f:entry/f:resource/f:Patient/f:id/@value",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:identifier/f:system/@value",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:identifier/f:value/@value",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:name/f:family/@value",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:name/f:given/@value",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:birthDate/@value",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:gender/@value"
        ],
        "namespaces": {
            "f": "http://hl7.org/fhir"
        },
        "column_mappings": {
            "/f:Bundle/f:entry/f:resource/f:Patient/f:id/@value": "patient_id",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:identifier/f:system/@value": "identifier_system",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:identifier/f:value/@value": "identifier_value",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:name/f:family/@value": "family_name",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:name/f:given/@value": "given_name",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:birthDate/@value": "birth_date",
            "/f:Bundle/f:entry/f:resource/f:Patient/f:gender/@value": "gender"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);

DETECT SCHEMA FOR TABLE {{zone_name}}.hie.patients;

-- ============================================================================
-- TABLE 2: patients_silver (Delta) - typed silver layer promoted from bronze
-- ============================================================================
-- Bronze (patients) is the raw HIE landing - every vendor file shows up here
-- on the next ingest run. The silver layer is what BI tools and clinical
-- analysts query: typed columns, ACID writes, time travel, OPTIMIZE/VACUUM.
-- ============================================================================
CREATE DELTA TABLE IF NOT EXISTS {{zone_name}}.hie.patients_silver (
    patient_id        STRING,
    identifier_system STRING,
    identifier_value  STRING,
    family_name       STRING,
    given_name        STRING,
    birth_date        STRING,
    gender            STRING
)
LOCATION 'fhir-multi-vendor-patients/silver/patients';

INSERT INTO {{zone_name}}.hie.patients_silver
SELECT
    patient_id,
    identifier_system,
    identifier_value,
    family_name,
    given_name,
    birth_date,
    gender
FROM {{zone_name}}.hie.patients;

