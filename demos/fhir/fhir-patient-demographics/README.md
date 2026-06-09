# FHIR Patient Demographics — Basics

Demonstrates FHIR R5 Patient resource ingestion by parsing both bulk NDJSON
exports and individual JSON resource files into queryable tables.

## Data Story

A hospital's EHR system exports its patient registry in two formats: a bulk
NDJSON file (50 patients, one per line) for batch analytics, and individual
Patient JSON files for detailed clinical review. The analyst needs to query
both sources using standard SQL, with clean column names and full data lineage.

## FHIR Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **NDJSON format** | Bulk Data export with one Patient resource per line |
| **Individual JSON** | Rich Patient resources with nested names, telecom, address |
| **column_mappings** | `$.id` → `patient_id`, `$.birthDate` → `birth_date` |
| **Schema evolution** | Files have varying completeness (deceasedBoolean, maritalStatus) |
| **file_filter** | Separate tables from `.ndjson` vs `.json` files in same directory |
| **file_metadata** | `df_file_name` and `df_row_number` for data lineage |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `patients_bulk` | External Table | 50 | Flat demographics from NDJSON bulk export |
| `patients_detailed` | External Table | 7 | Rich Patient resources with nested FHIR data |

## Known Verification Values

| Check | Expected |
|-------|----------|
| Bulk row count | 50 |
| Detailed row count | 7 |
| Bulk gender populated | 50 non-NULL |
| Bulk birth_date populated | 50 non-NULL |
| Column mapping (patient_id) | 50 non-NULL |
| File metadata | All rows have df_file_name |

## How to Verify

Run **Query #9 (Summary)** — all 7 checks should return `PASS`.
