# Delta Lake Demos

SQL demos covering the full Delta Lake protocol -- ACID transactions, time travel, schema evolution, and table maintenance. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **CRUD & DML** -- INSERT, UPDATE, DELETE, and multi-pass update patterns
- **MERGE** -- SCD Type 2, deduplication, soft delete, idempotent upsert, composite keys, computed columns, multi-source
- **Time Travel** -- VERSION AS OF, TIMESTAMP AS OF, point-in-time joins, row-level diffs, vacuum boundaries
- **Change Data Feed** -- Row-level before/after tracking for incremental processing
- **Partitioning** -- Single/multi-level, cross-partition updates, selective OPTIMIZE, time series, Unicode partitions
- **Schema Evolution** -- Column addition, type widening, column mapping, flexible ingestion
- **Table Maintenance** -- VACUUM, OPTIMIZE, Z-ORDER, RESTORE, bloom filters, deletion vectors, storage diagnostics
- **Governance** -- GDPR erasure, audit trails, views with data masking, constraint enforcement, append-only tables
- **Advanced SQL** -- Window functions, grouping sets, funnel analysis, CTEs, subqueries, PIVOT, overflow detection

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
