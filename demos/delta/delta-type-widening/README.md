# Delta Type Widening — Numeric Precision & Ranges

Demonstrates numeric type handling in Delta tables: INT vs BIGINT ranges,
DOUBLE precision, and scaling operations with precision preservation.

## Data Story

An industrial IoT platform collects sensor measurements across temperature,
pressure, humidity, flow, voltage, nanosecond timings, byte counts, CPU
frequencies, astronomical distances, and system ticks. Pressure readings
are converted from hectopascals to pascals (×1000) to verify scaling.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `measurements` | Delta Table | 40 (final) | Sensor readings with varied numeric ranges |

## Schema

**measurements:** `id INT, sensor_id VARCHAR, category VARCHAR, small_reading INT, large_reading BIGINT, precise_value DOUBLE, amount DOUBLE, unit VARCHAR, recorded_date VARCHAR`

## Patterns Demonstrated

1. **INT range** — small values fitting standard 32-bit integer
2. **BIGINT range** — large values (TB counts, nanoseconds, astronomical distances)
3. **DOUBLE precision** — fractional sensor readings with controlled rounding
4. **Scaling operations** — multiply readings ×1000 with ROUND() for precision
5. **Range verification** — MIN/MAX queries confirming value boundaries

## Verification

8 automated PASS/FAIL checks verify row counts, category distribution,
unit conversion, scaled values, BIGINT boundaries, and unchanged readings.
