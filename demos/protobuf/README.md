# Protocol Buffers Demos

SQL demos for querying Protocol Buffers (proto3) binary files. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **Address Book Contacts** -- Nested messages, repeated fields, enum decoding, well-known types
- **Freight Shipping Manifest** -- Complex message hierarchies, timestamp handling, logistics analytics
- **Sensor Network** -- IoT device telemetry, repeated measurements, device metadata flattening

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
