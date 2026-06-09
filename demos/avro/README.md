# Avro Demos

SQL demos for Apache Avro binary file analytics. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **E-Commerce Orders** -- Logical types (date, timestamp-millis), nullable unions, multi-file compression (null + deflate)
- **Insurance Claims** -- Schema evolution, complex union types, nested records
- **IoT Sensors** -- Time-series sensor data, device metadata, aggregation across readings

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
