# Pseudonymisation Demos

SQL demos for in-memory data protection and PII handling. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Quickstart** -- Basic pseudonymisation setup with keyed hash and encryption
- **Apply** -- Column-level transforms (hash, encrypt, mask, redact) applied during read
- **Lifecycle** -- Adding, modifying, and removing pseudonymisation rules over time
- **Healthcare** -- GDPR-compliant patient data handling with clinical context

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
