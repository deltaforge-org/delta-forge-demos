# Delta Z-ORDER — Multi-Column Data Layout Optimization

Demonstrates OPTIMIZE with Z-ORDER BY for multi-column data layout
optimization on IoT sensor telemetry across multiple regions and sensor types.

## Data Story

An IoT platform collects sensor telemetry from devices across 4 regions
(us-east, eu-west, us-west, ap-south). Data arrives in 3 batches:
temperature/humidity sensors first, then pressure/wind sensors, then a mixed
wave from all regions. After loading, OPTIMIZE with Z-ORDER BY reorganizes the
data files so queries filtering by region, sensor_type, and/or recorded_date
read fewer files. Low-quality readings are then flagged for recalibration.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `sensor_telemetry` | Delta Table | 80 (final) | IoT sensor readings with Z-ORDER optimization |

## Schema

**sensor_telemetry:** `id INT, device_id VARCHAR, sensor_type VARCHAR, reading DOUBLE, unit VARCHAR, latitude DOUBLE, longitude DOUBLE, region VARCHAR, quality_score INT, recorded_date VARCHAR`

## Patterns Demonstrated

1. **Multi-batch inserts** — 3 separate INSERT operations creating multiple files
2. **OPTIMIZE ZORDER BY** — co-locate data by (region, sensor_type, recorded_date)
3. **Post-optimize integrity** — row count preserved after optimization
4. **Multi-dimensional queries** — filter by region AND sensor_type AND date range
5. **Quality-based updates** — flag low-quality readings after optimization

## Verification

8 automated PASS/FAIL checks verify total row count, region distribution,
sensor type distribution, region-specific counts, type-specific counts,
quality flagging, average reading accuracy, and date range filtering.
