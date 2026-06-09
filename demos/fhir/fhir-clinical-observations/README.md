# FHIR Clinical Observations — Vital Signs & Lab Results

Demonstrates FHIR R5 Observation resource ingestion across vital signs, lab
results, and clinical assessments, highlighting the schema evolution that
occurs when different observation types populate different FHIR fields.

## Data Story

A hospital generates thousands of clinical observations daily. Heart rate
monitors produce NDJSON bulk exports for batch analytics, while detailed
clinical observations (vital signs panels, lab results, assessments) are
stored as individual FHIR JSON files. The analyst needs to query both data
streams and understand the structural differences across observation types.

## FHIR Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **NDJSON bulk export** | 100 heart rate readings from telemetry monitoring |
| **Deep nested JSON** | CodeableConcept (code.coding[].code), Quantity (valueQuantity.value) |
| **Schema evolution** | Vital signs use valueQuantity; blood pressure uses component[] |
| **FHIR References** | subject.reference links each observation to a Patient |
| **column_mappings** | `$.effectiveDateTime` → `effective_date`, `$.valueQuantity` → `value_quantity` |
| **Multi-file reading** | 14 observation files with varying schemas combined into one table |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `observations_bulk` | External Table | 100 | Heart rate readings from NDJSON bulk export |
| `observations_clinical` | External Table | 14 | Vital signs, lab results, and clinical assessments |

## Observation Types Covered

| Category | Observations | LOINC Codes |
|----------|-------------|-------------|
| **Vital Signs** | Body weight, height, BMI, blood pressure, temperature, respiratory rate, heart rate, SpO2 | 29463-7, 8302-2, 39156-5, 85354-9, 8310-5, 9279-1, 8867-4, 2708-6 |
| **Lab Results** | Glucose, base excess, CO2, erythrocyte count, hemoglobin | 15074-8, 11555-0, 11557-6, 789-8, 718-7 |
| **Assessments** | Glasgow Coma Scale | 9269-2 |

## How to Verify

Run **Query #10 (Summary)** — all 8 checks should return `PASS`.
