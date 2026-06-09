# JSON Demos

SQL demos for querying JSON files with nested and hierarchical structures. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Country Factbook** -- CIA World Factbook profiles with deep nesting, arrays, and schema evolution
- **Customer Records** -- Basic JSON parsing, path extraction, NULL handling
- **Music Catalog** -- Multi-level nested objects, array flattening, cross-file queries
- **Subtree Capture** -- Preserving raw JSON fragments for audit trails and downstream processing

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
