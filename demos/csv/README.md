# CSV Demos

SQL demos for querying CSV files with various formatting options. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Northwind Database** -- Classic relational dataset with multi-table joins, aggregation, and reporting
- **Sales Quickstart** -- Basic CSV ingestion, filtering, grouping, and summary analytics
- **Veterinary Clinic** -- Multi-file joins, date handling, patient/visit/billing relationships
- **Options Testbench** -- Delimiter variations, quoting modes, header detection, encoding options
- **Concurrent CDR Ingestion** -- Ten regional telecom feeds written concurrently into one Delta table via CONCURRENT BEGIN...END and PARALLEL INSERT, proving optimistic concurrency lands every row exactly once

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
