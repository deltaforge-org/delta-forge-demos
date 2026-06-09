# HL7 v2 Demos

SQL demos for parsing HL7 v2 pipe-delimited messages. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Patient Administration (ADT)** -- Admit, discharge, transfer messages, patient demographics
- **Lab Orders & Results (ORM/ORU)** -- Order entry, result reporting, observation segments
- **Clinical Workflows** -- Message routing, segment parsing, repeating fields, component extraction

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
