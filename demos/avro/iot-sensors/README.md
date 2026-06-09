# Avro IoT Sensors — Smart Building Telemetry

## Overview

This demo uses 5 Avro files containing IoT sensor readings from a smart
building (5 floors, 500 readings per floor, 2,500 total). The files showcase
Avro's self-describing format with schema evolution across file versions and
mixed compression codecs.

## Data

| File | Floor | Rows | Schema | Codec |
|------|-------|------|--------|-------|
| `floor1_sensors.avro` | 1 | 500 | v1 (8 fields) | null |
| `floor2_sensors.avro` | 2 | 500 | v1 (8 fields) | deflate |
| `floor3_sensors.avro` | 3 | 500 | v1 (8 fields) | null |
| `floor4_sensors.avro` | 4 | 500 | v2 (10 fields) | deflate |
| `floor5_sensors.avro` | 5 | 500 | v2 (10 fields) | null |

### Schema Versions

**v1** (floors 1–3): `sensor_id`, `floor`, `zone`, `timestamp`,
`temperature_c`, `humidity_pct`, `co2_ppm`, `occupancy`

**v2** (floors 4–5): all v1 fields + `battery_pct`, `firmware_version`

When reading all 5 files together, the union schema merges both versions.
Rows from v1 files get `NULL` for `battery_pct` and `firmware_version`.

## Tables

| Table | Rows | Features |
|-------|------|----------|
| `all_readings` | 2,500 | Multi-file, schema evolution, file_metadata |
| `floor4_only` | 500 | file_filter, v2 schema with all columns |
| `readings_sample` | 250 | max_rows (50 per file), data profiling |

## Avro Features Demonstrated

| Feature | How |
|---------|-----|
| **Self-describing schema** | Avro file headers provide field names and types |
| **Schema evolution** | v1→v2 adds `battery_pct` + `firmware_version`; NULL filling |
| **Mixed codecs** | null (floors 1,3,5) and deflate (floors 2,4) |
| **Multi-file reading** | 5 files merged into one table |
| **file_filter** | `floor4*` selects single floor |
| **max_rows** | 50 rows per file for sampling |
| **file_metadata** | `df_file_name`, `df_row_number` system columns |

## Queries

11 queries with 10 automated PASS/FAIL checks covering:
- Total row count verification
- Schema evolution NULL filling (v1 floors lack battery/firmware)
- File filter extraction (floor 4 only)
- Max rows sampling
- File metadata population
- Column count with union schema
- Temperature/humidity/CO2 analytics by floor and zone
- Occupancy rate analysis
