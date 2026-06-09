# Delta Auto-Optimize — Automatic Compaction & Write Optimization

Demonstrates how Delta auto-optimize table properties solve the "small files
problem" that occurs when many small INSERTs create numerous tiny data files.

## Data Story

An IoT platform ingests sensor data from 10 devices in 7 small batches, each
representing a different metric type. Without auto-optimize, each INSERT creates
a tiny file. By enabling `optimizeWrite` and `autoCompact` via TBLPROPERTIES,
Delta automatically coalesces small writes into optimally-sized files. After
ingestion, 8 readings with extreme values are flagged as poor quality.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `iot_readings` | Delta Table | 70 | IoT sensor data with auto-optimize |

## Table Properties

| Property | Value | Effect |
|----------|-------|--------|
| `delta.autoOptimize.optimizeWrite` | `true` | Coalesces small writes into larger files |
| `delta.autoOptimize.autoCompact` | `true` | Automatically compacts small files after writes |

## Schema

**iot_readings:** `id INT, device_id VARCHAR, metric VARCHAR, value DOUBLE, unit VARCHAR, quality VARCHAR, batch_id INT, recorded_at VARCHAR`

## Data Breakdown

| Batch | Metric | Rows | Extreme (poor) |
|-------|--------|------|-----------------|
| 1 | temperature | 10 | 2 |
| 2 | humidity | 10 | 1 |
| 3 | pressure | 10 | 1 |
| 4 | wind_speed | 10 | 1 |
| 5 | light | 10 | 1 |
| 6 | noise | 10 | 1 |
| 7 | vibration | 10 | 1 |
| **Total** | **7 metrics** | **70** | **8** |

## Verification

8 automated PASS/FAIL checks verify total rows (70), metric count (7),
batch count (7), poor/good quality split (8/62), device count (10),
temperature count (10), and average temperature value (28.1).
