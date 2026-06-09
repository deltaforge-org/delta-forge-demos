#!/usr/bin/env python3
"""
Iceberg UniForm Type Widening — Data Verification
====================================================
Reads the sensor_readings table purely through the Iceberg metadata chain
and verifies the final state after type widening operations:
  - 24 sensors seeded with INT/FLOAT types (V1)
  - reading_count INT -> BIGINT (V2)
  - INSERT 4 rows with BIGINT values > 2 billion (V3)
  - temperature FLOAT -> DOUBLE (V4)
  - humidity FLOAT -> DOUBLE (V5)
  - INSERT 4 rows with high-precision DOUBLE values (V6)

Final state: 32 sensor readings, 4 with BIGINT values, 4 with
high-precision DOUBLE values.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing sensor_readings/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (read_iceberg_table, ok, fail, info,
    assert_row_count, assert_avg, assert_distinct_count,
    assert_count_where, assert_value_where, assert_format_version,
    print_header, print_section, print_summary, exit_with_status)


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_sensor_readings(data_root, verbose=False):
    print_section("sensor_readings — Type Widening")

    table_path = os.path.join(data_root, "sensor_readings")
    table, metadata = read_iceberg_table(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    assert_format_version(metadata, 2)

    import pyarrow.compute as pc

    # Final state: 32 rows (24 original + 4 BIGINT + 4 high-precision)
    assert_row_count(table, 32)

    # 4 distinct locations
    assert_distinct_count(table, "location", 4)

    # 8 per location
    assert_count_where(table, "location", "rooftop", 8)
    assert_count_where(table, "location", "basement", 8)
    assert_count_where(table, "location", "warehouse", 8)
    assert_count_where(table, "location", "cleanroom", 8)

    # Max reading_count = 3500000000 (BIGINT)
    max_reading = pc.max(table.column("reading_count")).as_py()
    if max_reading == 3500000000:
        ok(f"MAX(reading_count) = 3500000000 (BIGINT)")
    else:
        fail(f"MAX(reading_count) = {max_reading}, expected 3500000000")

    # 4 rows with reading_count > 2 billion (INT overflow threshold)
    bigint_mask = pc.greater(table.column("reading_count"), 2147483647)
    bigint_count = pc.sum(bigint_mask).as_py()
    if bigint_count == 4:
        ok(f"Rows with reading_count > 2^31-1 = 4")
    else:
        fail(f"Rows with reading_count > 2^31-1 = {bigint_count}, expected 4")

    # Specific BIGINT values
    assert_value_where(table, "reading_count", 2500000000, "sensor_id", "S025")
    assert_value_where(table, "reading_count", 3100000000, "sensor_id", "S026")
    assert_value_where(table, "reading_count", 2800000000, "sensor_id", "S027")
    assert_value_where(table, "reading_count", 3500000000, "sensor_id", "S028")

    # High-precision DOUBLE values (round to 6 dp, not the default 2)
    for sensor_id, expected_temp in [
        ("S029", 32.456789),
        ("S030", 18.789012),
        ("S031", 22.567890),
        ("S032", 21.234567),
    ]:
        mask = pc.equal(table.column("sensor_id"), sensor_id)
        filtered = table.filter(mask)
        if filtered.num_rows == 0:
            fail(f"No rows where sensor_id = {sensor_id!r}")
            continue
        actual = round(float(filtered.column("temperature")[0].as_py()), 6)
        expected_r = round(expected_temp, 6)
        if actual == expected_r:
            ok(f"temperature = {expected_temp!r} WHERE sensor_id = {sensor_id!r}")
        else:
            fail(f"temperature = {actual!r}, expected {expected_temp!r} WHERE sensor_id = {sensor_id!r}")

    # Per-location averages
    for loc, expected_avg_temp in [
        ("basement", 18.57),
        ("cleanroom", 21.14),
        ("rooftop", 32.97),
        ("warehouse", 22.87),
    ]:
        mask = pc.equal(table.column("location"), loc)
        filtered = table.filter(mask)
        actual = round(pc.mean(filtered.column("temperature")).as_py(), 2)
        if actual == expected_avg_temp:
            ok(f"AVG(temperature) for {loc} = {expected_avg_temp}")
        else:
            fail(f"AVG(temperature) for {loc} = {actual}, expected {expected_avg_temp}")

    # Grand averages
    assert_avg(table, "temperature", 23.89)
    assert_avg(table, "humidity", 55.34)


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-schema-widen demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing sensor_readings/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Type Widening — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "sensor_readings")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_sensor_readings(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
