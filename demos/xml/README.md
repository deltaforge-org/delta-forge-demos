# XML Demos

SQL demos for querying XML documents with namespaces, attributes, and repeating elements. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Books Schema Evolution** -- XML attribute extraction, schema changes across files
- **E-Commerce Order Lines** -- Repeating elements with join vs. explode strategies
- **NYT News RSS** -- RSS feed parsing, date handling, category extraction
- **Subtree Capture** -- Preserving raw XML fragments for audit and reprocessing

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
