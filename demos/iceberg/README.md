# Apache Iceberg Demos

SQL demos for the Apache Iceberg table format across V1, V2, and V3 specs. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **V1** -- Core table operations, schema evolution, partition pruning, snapshot isolation
- **V2** -- Row-level deletes, position delete files, equality delete files, merge-on-read
- **V3** -- Extended type support and latest spec features
- **UniForm** -- Interoperability between Iceberg and Delta Lake formats

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
