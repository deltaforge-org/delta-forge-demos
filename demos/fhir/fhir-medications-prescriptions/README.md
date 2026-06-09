# FHIR Medications — Prescriptions & Coverage

Demonstrates FHIR R5 MedicationRequest and Coverage resource ingestion,
showcasing deeply nested FHIR structures including contained resources,
dosage instructions, and insurance coverage classifications.

## Data Story

A hospital pharmacy system exports prescription orders and insurance coverage
records as FHIR resources. The analyst needs to query prescription statuses,
dosage instructions, dispense quantities, and link prescriptions to insurance
coverage — all from raw FHIR JSON files without any ETL transformation.

## FHIR Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Contained resources** | Medication embedded inside MedicationRequest |
| **json_paths** | dosageInstruction, contained, dispenseRequest preserved as JSON blobs |
| **Deep nesting** | dosageInstruction[].timing.repeat, dispenseRequest.quantity |
| **CodeableConcept** | medication.reference, category, reason with SNOMED/RxNorm coding |
| **Coverage classes** | Insurance group, plan, rxid, rxbin, rxgroup, rxpcn arrays |
| **Schema evolution** | Different prescription types populate different optional fields |
| **column_mappings** | `$.authoredOn` → `authored_date`, `$.dosageInstruction` → `dosage_instructions` |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `prescriptions` | External Table | 12 | MedicationRequest resources (prescriptions/orders) |
| `coverage` | External Table | 4 | Coverage resources (insurance/self-pay plans) |

## Key FHIR Patterns

- **MedicationRequest.contained[]** — Embedded Medication resource with SNOMED drug codes
- **MedicationRequest.dosageInstruction[]** — Timing, route, dose, patient instructions
- **MedicationRequest.dispenseRequest** — Quantity, validity period, supply duration
- **Coverage.class[]** — Insurance classification hierarchy (group → plan → rx details)
- **Coverage.period** — Coverage validity date range

## How to Verify

Run **Query #12 (Summary)** — all 8 checks should return `PASS`.
