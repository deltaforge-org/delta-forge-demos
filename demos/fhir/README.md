# FHIR Demos

SQL demos for querying FHIR R4/R5 clinical resources. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Patient Demographics** -- Patient resource parsing, identifier systems, contact details
- **Clinical Observations** -- Vital signs, lab results, observation coding, reference ranges
- **Medications & Prescriptions** -- MedicationRequest resources, dosage instructions, dispense records
- **Clinical Records** -- Conditions, procedures, encounters, care plans
- **XML Resources** -- FHIR XML bundles with namespace handling and XPath extraction

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
