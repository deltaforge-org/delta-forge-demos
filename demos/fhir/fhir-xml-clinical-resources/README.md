# FHIR XML Clinical Resources

Demonstrates DeltaForge's XML flattening capabilities with HL7 FHIR R4 clinical
data in native XML format. FHIR XML has a unique structure: all primitive values
are stored as `@value` attributes rather than element text content.

## Data Story

A healthcare system stores patient demographics and clinical observations as
standard HL7 FHIR R4 XML resources. Each XML file is a single FHIR resource
with the HL7 namespace declaration (`xmlns="http://hl7.org/fhir"`). Patient
resources vary in completeness — some include marital status, managing
organization, contact persons, and communication preferences; others have only
basic demographics. Observation resources span vital signs (body weight, height,
BMI, temperature, blood pressure) and lab results (glucose, CO2, base excess).

DeltaForge reads these native FHIR XML files without any pre-processing,
handling the namespace, extracting `@value` attributes, flattening nested coding
blocks, joining repeating elements, and preserving complex subtrees as JSON.

## What Is Tested

| Feature | Where |
|---------|-------|
| FHIR namespace (`xmlns="http://hl7.org/fhir"`) | Both tables — `strip_namespace_prefixes` |
| `@value` attribute extraction | Every column — FHIR XML's core pattern |
| `exclude_paths` | Both tables — skip `<text>` narrative and `<meta>` |
| Repeating elements (`join_comma`) | `patients_xml` — `<name>`, `<telecom>`, `<identifier>` |
| Deep nested XPath (4+ levels) | `observations_xml` — `code/coding/code/@value` |
| `xml_paths` (subtree → JSON) | Both tables — `contact`, `communication`, `component`, `referenceRange` |
| `column_mappings` (XPath → names) | Both tables — 30+ mapped columns |
| Schema evolution | Both tables — different resources populate different fields |
| Cross-table FHIR references | Join query — `Patient/example` reference resolution |
| `nested_output_format: "json"` | Both tables — XML subtrees output as JSON |
| Multi-file reading | Both tables — 8 XML files each |

## Tables

### patients_xml
| Column | Source XPath | Description |
|--------|-------------|-------------|
| patient_id | `/Patient/id/@value` | FHIR resource ID |
| family_name | `/Patient/name/family/@value` | Family/last name |
| given_name | `/Patient/name/given/@value` | Given/first name(s) |
| gender | `/Patient/gender/@value` | Administrative gender |
| birth_date | `/Patient/birthDate/@value` | Date of birth |
| marital_code | `/Patient/maritalStatus/coding/code/@value` | Marital status code |
| org_display | `/Patient/managingOrganization/display/@value` | Managing organization |
| contact | `/Patient/contact` (xml_paths) | Contact persons as JSON |
| communication | `/Patient/communication` (xml_paths) | Language preferences as JSON |

### observations_xml
| Column | Source XPath | Description |
|--------|-------------|-------------|
| observation_id | `/Observation/id/@value` | FHIR resource ID |
| code_display | `/Observation/code/coding/display/@value` | Observation type name |
| code_value | `/Observation/code/coding/code/@value` | LOINC/SNOMED code |
| result_value | `/Observation/valueQuantity/value/@value` | Numeric result |
| result_unit | `/Observation/valueQuantity/unit/@value` | Unit of measurement |
| patient_ref | `/Observation/subject/reference/@value` | Patient reference |
| component | `/Observation/component` (xml_paths) | BP systolic/diastolic as JSON |
| reference_range | `/Observation/referenceRange` (xml_paths) | Lab normal range as JSON |

## Verification Queries

The queries showcase 12 scenarios plus 8 PASS/FAIL verification checks covering
namespace stripping, attribute extraction, deep XPath navigation, xml_paths
preservation, exclude_paths filtering, repeating element handling, column
mappings, and cross-table FHIR reference joins.
