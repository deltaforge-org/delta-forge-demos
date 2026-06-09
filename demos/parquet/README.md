# Parquet Demos

SQL demos for columnar Parquet file analytics. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Flight Delays** -- Recursive directory scanning, predicate pushdown, file-level filtering, carrier/route analytics
- **Supply Chain** -- Multi-file columnar joins, aggregation, row group statistics

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
