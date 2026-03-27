#!/usr/bin/env python3
"""
Generate an Iceberg V2 table with position delete files using PySpark.

Scenario: Pharmaceutical cold-chain monitoring — 600 temperature sensor
readings across 4 shipment routes. 30 readings from a faulty sensor
(SENSOR-F01) are retracted via Iceberg V2 position deletes, leaving
570 valid readings.

Output: cold_chain_readings/ directory with Iceberg V2 metadata, data
files, and position delete files — ready for Delta Forge to read.
"""
import os
import sys
import shutil
import json

# PySpark + Iceberg setup
ICEBERG_JAR = os.path.expanduser(
    "~/.ivy2.5.2/jars/org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
)
JAVA_HOME = os.path.expanduser("~/local/jdk")
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = f"{JAVA_HOME}/bin:{os.environ['PATH']}"

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_NAME = "cold_chain_readings"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_v2_dv_warehouse"

# Clean previous runs
for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergV2PositionDeleteGenerator")
    .config("spark.jars", ICEBERG_JAR)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    .config("spark.sql.defaultCatalog", "local")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Step 1: Generate the dataset ─────────────────────────────────────
import random
from datetime import datetime, timedelta

random.seed(42)

ROUTES = ["ROUTE-A", "ROUTE-B", "ROUTE-C", "ROUTE-D"]
SENSORS_NORMAL = [f"SENSOR-{i:03d}" for i in range(1, 21)]  # 20 normal sensors
SENSOR_FAULTY = "SENSOR-F01"  # 1 faulty sensor
ALL_SENSORS = SENSORS_NORMAL + [SENSOR_FAULTY]

VACCINE_TYPES = ["mRNA-COVID", "Influenza-Quad", "HPV-9v", "Tdap"]
FACILITY_ORIGINS = ["Pfizer-Kalamazoo", "GSK-Rixensart", "Merck-Durham", "Sanofi-Lyon"]
FACILITY_DESTS = ["Mayo-Rochester", "Johns-Hopkins", "Cleveland-Clinic", "Mass-General"]

rows = []
base_time = datetime(2025, 6, 1, 6, 0, 0)
reading_id = 1

for route_idx, route in enumerate(ROUTES):
    vaccine = VACCINE_TYPES[route_idx]
    origin = FACILITY_ORIGINS[route_idx]
    dest = FACILITY_DESTS[route_idx]

    # 150 readings per route = 600 total
    for i in range(150):
        # Pick sensor — faulty sensor appears in ROUTE-A only (30 readings)
        if route == "ROUTE-A" and i < 30:
            sensor = SENSOR_FAULTY
            # Faulty sensor reads too high (will be retracted)
            temp_c = round(random.uniform(8.5, 15.0), 2)
            is_faulty = True
        else:
            sensor = random.choice(SENSORS_NORMAL)
            # Normal readings: -8°C to +8°C (vaccine cold chain range)
            temp_c = round(random.uniform(-8.0, 8.0), 2)
            is_faulty = False

        humidity_pct = round(random.uniform(30.0, 70.0), 1)
        battery_pct = random.randint(15, 100)
        # Excursion = temperature outside -2°C to +8°C safe range
        temp_excursion = temp_c < -2.0 or temp_c > 8.0
        elapsed_min = i * 15  # 15-min intervals
        timestamp = base_time + timedelta(minutes=elapsed_min)

        rows.append((
            f"RD-{reading_id:04d}",
            route,
            sensor,
            vaccine,
            origin,
            dest,
            temp_c,
            humidity_pct,
            battery_pct,
            temp_excursion,
            timestamp.isoformat(),
        ))
        reading_id += 1

schema = StructType([
    StructField("reading_id", StringType(), False),
    StructField("route", StringType(), False),
    StructField("sensor_id", StringType(), False),
    StructField("vaccine_type", StringType(), False),
    StructField("origin_facility", StringType(), False),
    StructField("dest_facility", StringType(), False),
    StructField("temperature_c", DoubleType(), False),
    StructField("humidity_pct", DoubleType(), False),
    StructField("battery_pct", IntegerType(), False),
    StructField("temp_excursion", BooleanType(), False),
    StructField("reading_time", StringType(), False),
])

df = spark.createDataFrame(rows, schema)

print(f"Generated {df.count()} rows")
print(f"Faulty sensor readings: {df.filter(F.col('sensor_id') == 'SENSOR-F01').count()}")

# ── Step 2: Create Iceberg V2 table and load data ────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.pharma")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.pharma.{TABLE_NAME} (
        reading_id STRING NOT NULL,
        route STRING NOT NULL,
        sensor_id STRING NOT NULL,
        vaccine_type STRING NOT NULL,
        origin_facility STRING NOT NULL,
        dest_facility STRING NOT NULL,
        temperature_c DOUBLE NOT NULL,
        humidity_pct DOUBLE NOT NULL,
        battery_pct INT NOT NULL,
        temp_excursion BOOLEAN NOT NULL,
        reading_time STRING NOT NULL
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read'
    )
""")

# Insert data — coalesce to 1 partition for a single data file
df.coalesce(1).writeTo(f"local.pharma.{TABLE_NAME}").append()
print(f"Loaded data into Iceberg table")

# Verify pre-delete
pre_count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.pharma.{TABLE_NAME}").collect()[0].cnt
print(f"Pre-delete count: {pre_count}")

# ── Step 3: Delete faulty sensor readings (creates position deletes) ──
spark.sql(f"""
    DELETE FROM local.pharma.{TABLE_NAME}
    WHERE sensor_id = 'SENSOR-F01'
""")
print("Deleted faulty sensor readings (SENSOR-F01)")

# Verify post-delete
post_count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.pharma.{TABLE_NAME}").collect()[0].cnt
print(f"Post-delete count: {post_count}")
print(f"Rows deleted: {pre_count - post_count}")

# ── Step 4: Verify delete files exist in metadata ─────────────────────
table_loc = f"{WAREHOUSE}/pharma/{TABLE_NAME}"
metadata_dir = os.path.join(table_loc, "metadata")

# Find latest metadata.json
meta_files = sorted([f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")])
latest_meta = os.path.join(metadata_dir, meta_files[-1])
with open(latest_meta) as f:
    meta = json.load(f)

current_snap_id = meta["current-snapshot-id"]
for snap in meta["snapshots"]:
    if snap["snapshot-id"] == current_snap_id:
        summary = snap["summary"]
        print(f"\nCurrent snapshot summary:")
        for k, v in sorted(summary.items()):
            print(f"  {k}: {v}")
        break

# ── Step 5: Copy table to demo directory (without CRC files) ──────────
print(f"\nCopying table from {table_loc} to {TABLE_OUTPUT}")
shutil.copytree(
    table_loc,
    TABLE_OUTPUT,
    ignore=shutil.ignore_patterns("*.crc", "version-hint.text", ".version-hint.text.crc"),
)

# List all files
print("\nGenerated files:")
for root, dirs, files in os.walk(TABLE_OUTPUT):
    for f in sorted(files):
        full = os.path.join(root, f)
        rel = os.path.relpath(full, TABLE_OUTPUT)
        size = os.path.getsize(full)
        print(f"  {rel} ({size:,} bytes)")

spark.stop()
print("\nDone!")
