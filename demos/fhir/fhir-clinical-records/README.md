# FHIR Clinical Records — Conditions, Procedures & Allergies

Demonstrates advanced FHIR R5 resource ingestion across three related clinical
resource types from a single directory, showcasing multi-resource-type scanning,
deep nested arrays, and cross-resource clinical data model relationships.

## Data Story

A hospital's clinical documentation system exports patient records as FHIR
resources. Three resource types coexist in the export: diagnoses (Condition),
surgical interventions (Procedure), and allergy records (AllergyIntolerance).
The analyst needs to query all three resource types independently while
understanding their structural differences and clinical relationships.

## FHIR Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Multi-resource-type directory** | 3 resource types in one directory, separated by file_filter |
| **Deep nested arrays** | reaction[].manifestation[].concept.coding[], performer[].actor |
| **json_paths** | reaction, performer, reason, followUp preserved as JSON blobs |
| **CodeableConcept hierarchies** | SNOMED CT, ICD-10 coding with multiple codings per concept |
| **Schema evolution** | Optional fields vary across resource instances (severity, bodySite, stage) |
| **column_mappings** | `$.clinicalStatus` → `clinical_status`, `$.onsetDateTime` → `onset_date` |
| **Cross-resource references** | All resources reference Patient via subject/patient field |

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `conditions` | External Table | 8 | Diagnoses and clinical findings |
| `procedures` | External Table | 8 | Surgical and clinical interventions |
| `allergies` | External Table | 6 | Allergy and intolerance records |

## Clinical Resources Covered

### Conditions (Diagnoses)
Burn of ear, Heart valve disorder, NSCLC, Fever, Malignant neoplasm, Sepsis,
Renal insufficiency, Stroke

### Procedures (Interventions)
Appendectomy, Biopsy, Colonoscopy, Heart valve replacement, Lobectomy,
Abscess I&D, Tracheotomy, Device implant

### Allergies (AllergyIntolerance)
Cashew nut allergy (high criticality), Fish allergy, Medication allergy,
No Known Allergies (NKA), No Known Drug Allergies (NKDA),
No Known Latex Allergies (NKLA)

## How to Verify

Run **Query #13 (Summary)** — all 10 checks should return `PASS`.
