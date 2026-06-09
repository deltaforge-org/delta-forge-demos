-- ============================================================================
-- FHIR XML Clinical Resources — Setup Script
-- ============================================================================
-- Creates two external tables from FHIR R4 XML resources:
--   1. patients_xml      — 8 Patient resources in HL7 FHIR XML format
--   2. observations_xml  — 8 Observation resources (vital signs + labs) in XML
--
-- Demonstrates:
--   - HL7 FHIR namespace (xmlns="http://hl7.org/fhir") with strip_namespace_prefixes
--   - FHIR XML @value attribute pattern — all primitives stored as attributes
--   - exclude_paths to skip narrative <text> and <meta> elements
--   - Repeating elements: <name>, <telecom>, <identifier> with join_comma
--   - Deep nested XPath extraction: coding/system/@value, valueQuantity/value/@value
--   - xml_paths to preserve complex subtrees (contact, component) as JSON
--   - column_mappings for XPath → analyst-friendly names
--   - Schema evolution across different resource instances
--   - Multi-file reading from a single directory (XML format)
-- ============================================================================

-- STEP 1: Zone & Schema
CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External tables — demo datasets and file-backed data';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.fhir_xml
    COMMENT 'FHIR XML external tables — HL7 FHIR R4 resources in native XML format';

-- ============================================================================
-- TABLE 1: patients_xml — 8 FHIR Patient resources in XML format
-- ============================================================================
-- FHIR stores ALL primitive values in @value attributes (e.g., <id value="f001"/>
-- not <id>f001</id>). This is the defining characteristic of FHIR XML. The
-- default HL7 FHIR namespace is stripped via strip_namespace_prefixes. The
-- narrative <text> element (which contains XHTML) and <meta> security tags
-- are excluded. Repeating elements like <name>, <telecom>, and <identifier>
-- are comma-joined. The complex <contact> and <communication> subtrees are
-- preserved as JSON via xml_paths.
--
-- Patients include Pieter van de Heuvel, Roel, Peter Chalmers, Donald Duck,
-- and others — each with varying levels of demographic detail.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_xml.patients_xml
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'patient-example*.xml',
    xml_flatten_config = '{
        "row_xpath": "//Patient",
        "include_paths": [
            "/Patient/id/@value",
            "/Patient/active/@value",
            "/Patient/gender/@value",
            "/Patient/birthDate/@value",
            "/Patient/deceasedBoolean/@value",
            "/Patient/multipleBirthBoolean/@value",
            "/Patient/name/use/@value",
            "/Patient/name/family/@value",
            "/Patient/name/given/@value",
            "/Patient/name/prefix/@value",
            "/Patient/name/suffix/@value",
            "/Patient/telecom/system/@value",
            "/Patient/telecom/value/@value",
            "/Patient/telecom/use/@value",
            "/Patient/address/use/@value",
            "/Patient/address/line/@value",
            "/Patient/address/city/@value",
            "/Patient/address/postalCode/@value",
            "/Patient/address/country/@value",
            "/Patient/maritalStatus/coding/code/@value",
            "/Patient/maritalStatus/coding/display/@value",
            "/Patient/maritalStatus/text/@value",
            "/Patient/managingOrganization/reference/@value",
            "/Patient/managingOrganization/display/@value",
            "/Patient/identifier/use/@value",
            "/Patient/identifier/system/@value",
            "/Patient/identifier/value/@value",
            "/Patient/contact",
            "/Patient/communication"
        ],
        "exclude_paths": ["/Patient/text", "/Patient/meta"],
        "xml_paths": ["/Patient/contact", "/Patient/communication"],
        "default_repeat_handling": "join_comma",
        "column_mappings": {
            "/Patient/id/@value": "patient_id",
            "/Patient/active/@value": "is_active",
            "/Patient/gender/@value": "gender",
            "/Patient/birthDate/@value": "birth_date",
            "/Patient/deceasedBoolean/@value": "is_deceased",
            "/Patient/multipleBirthBoolean/@value": "multiple_birth",
            "/Patient/name/use/@value": "name_use",
            "/Patient/name/family/@value": "family_name",
            "/Patient/name/given/@value": "given_name",
            "/Patient/name/prefix/@value": "name_prefix",
            "/Patient/name/suffix/@value": "name_suffix",
            "/Patient/telecom/system/@value": "telecom_system",
            "/Patient/telecom/value/@value": "telecom_value",
            "/Patient/telecom/use/@value": "telecom_use",
            "/Patient/address/use/@value": "address_use",
            "/Patient/address/line/@value": "address_line",
            "/Patient/address/city/@value": "city",
            "/Patient/address/postalCode/@value": "postal_code",
            "/Patient/address/country/@value": "country",
            "/Patient/maritalStatus/coding/code/@value": "marital_code",
            "/Patient/maritalStatus/coding/display/@value": "marital_display",
            "/Patient/maritalStatus/text/@value": "marital_text",
            "/Patient/managingOrganization/reference/@value": "org_reference",
            "/Patient/managingOrganization/display/@value": "org_display",
            "/Patient/identifier/use/@value": "identifier_use",
            "/Patient/identifier/system/@value": "identifier_system",
            "/Patient/identifier/value/@value": "identifier_value",
            "/Patient/contact": "contact",
            "/Patient/communication": "communication"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true,
        "nested_output_format": "json",
        "namespaces": {
            "fhir": "http://hl7.org/fhir",
            "xhtml": "http://www.w3.org/1999/xhtml"
        }
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);


-- ============================================================================
-- TABLE 2: observations_xml — 8 FHIR Observation resources in XML format
-- ============================================================================
-- FHIR Observations in XML demonstrate deep nesting: the code element has
-- coding/system/@value and coding/code/@value paths (4 levels deep), while
-- valueQuantity stores numeric results across value/@value, unit/@value, and
-- code/@value attributes. Blood pressure uses <component> with systolic and
-- diastolic sub-observations. The component subtree is preserved as JSON via
-- xml_paths since it contains nested coding + quantity pairs.
--
-- Observations include: body weight, blood pressure (with components),
-- BMI, body height, body temperature, glucose, CO2, and base excess.
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS {{zone_name}}.fhir_xml.observations_xml
USING XML
LOCATION '{{data_path}}'
OPTIONS (
    file_filter = 'observation-example*.xml',
    xml_flatten_config = '{
        "row_xpath": "//Observation",
        "include_paths": [
            "/Observation/id/@value",
            "/Observation/status/@value",
            "/Observation/code/coding/system/@value",
            "/Observation/code/coding/code/@value",
            "/Observation/code/coding/display/@value",
            "/Observation/code/text/@value",
            "/Observation/category/coding/code/@value",
            "/Observation/category/coding/display/@value",
            "/Observation/subject/reference/@value",
            "/Observation/subject/display/@value",
            "/Observation/effectiveDateTime/@value",
            "/Observation/effectivePeriod/start/@value",
            "/Observation/effectivePeriod/end/@value",
            "/Observation/issued/@value",
            "/Observation/valueQuantity/value/@value",
            "/Observation/valueQuantity/unit/@value",
            "/Observation/valueQuantity/system/@value",
            "/Observation/valueQuantity/code/@value",
            "/Observation/interpretation/coding/code/@value",
            "/Observation/interpretation/coding/display/@value",
            "/Observation/interpretation/text/@value",
            "/Observation/bodySite/coding/code/@value",
            "/Observation/bodySite/coding/display/@value",
            "/Observation/referenceRange",
            "/Observation/component",
            "/Observation/performer/reference/@value",
            "/Observation/performer/display/@value"
        ],
        "exclude_paths": ["/Observation/text", "/Observation/meta", "/Observation/identifier"],
        "xml_paths": ["/Observation/referenceRange", "/Observation/component"],
        "default_repeat_handling": "join_comma",
        "column_mappings": {
            "/Observation/id/@value": "observation_id",
            "/Observation/status/@value": "status",
            "/Observation/code/coding/system/@value": "code_system",
            "/Observation/code/coding/code/@value": "code_value",
            "/Observation/code/coding/display/@value": "code_display",
            "/Observation/code/text/@value": "code_text",
            "/Observation/category/coding/code/@value": "category_code",
            "/Observation/category/coding/display/@value": "category_display",
            "/Observation/subject/reference/@value": "patient_ref",
            "/Observation/subject/display/@value": "patient_display",
            "/Observation/effectiveDateTime/@value": "effective_date",
            "/Observation/effectivePeriod/start/@value": "effective_start",
            "/Observation/effectivePeriod/end/@value": "effective_end",
            "/Observation/issued/@value": "issued_date",
            "/Observation/valueQuantity/value/@value": "result_value",
            "/Observation/valueQuantity/unit/@value": "result_unit",
            "/Observation/valueQuantity/system/@value": "unit_system",
            "/Observation/valueQuantity/code/@value": "unit_code",
            "/Observation/interpretation/coding/code/@value": "interp_code",
            "/Observation/interpretation/coding/display/@value": "interp_display",
            "/Observation/interpretation/text/@value": "interp_text",
            "/Observation/bodySite/coding/code/@value": "body_site_code",
            "/Observation/bodySite/coding/display/@value": "body_site_display",
            "/Observation/referenceRange": "reference_range",
            "/Observation/component": "component",
            "/Observation/performer/reference/@value": "performer_ref",
            "/Observation/performer/display/@value": "performer_display"
        },
        "include_attributes": true,
        "separator": "_",
        "max_depth": 10,
        "strip_namespace_prefixes": true,
        "nested_output_format": "json",
        "namespaces": {
            "fhir": "http://hl7.org/fhir",
            "xhtml": "http://www.w3.org/1999/xhtml"
        }
    }',
    file_metadata = '{"columns":["df_file_name","df_row_number"]}'
);
