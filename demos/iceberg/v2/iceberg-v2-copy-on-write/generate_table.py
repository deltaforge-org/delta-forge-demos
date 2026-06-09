#!/usr/bin/env python3
"""
Generate an Iceberg V2 table using copy-on-write delete/update mode.

Scenario: Logistics Shipment Tracking — 120 shipment records tracked across
carriers (FedEx, UPS, DHL, USPS). Copy-on-write mode rewrites entire data
files on UPDATE/DELETE instead of creating delete files.

Snapshot 1: Initial load — 120 shipments (append)
Snapshot 2: UPDATE — 20 "In Transit" shipments marked as "Delivered" (overwrite)
Snapshot 3: DELETE — 10 cancelled/returned shipments removed (overwrite)

Final state: 110 shipments (120 - 10 deleted), with 20 having updated statuses.
No delete files exist — copy-on-write rewrites data files entirely.
"""
import os
import sys
import shutil
import random
import json
from datetime import date, timedelta

# PySpark + Iceberg setup
ICEBERG_JAR = os.path.expanduser(
    "~/.ivy2.5.2/jars/org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
)
JAVA_HOME = os.path.expanduser("~/local/jdk")
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = f"{JAVA_HOME}/bin:{os.environ['PATH']}"

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_NAME = "shipments"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_copy_on_write_warehouse"

# Clean previous runs
for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergCopyOnWrite")
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
random.seed(42)

CITIES = [
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "Miami",
    "Atlanta", "Boston", "Seattle", "Denver", "Detroit",
]
CARRIERS = ["FedEx", "UPS", "DHL", "USPS"]
STATUSES = ["Delivered", "In Transit", "Processing", "Returned"]
PRIORITIES = ["Standard", "Express", "Overnight"]

rows = []
base_date = date(2025, 1, 5)

for i in range(1, 121):
    shipment_id = f"SHP-{i:04d}"
    origin = random.choice(CITIES)
    dest = random.choice([c for c in CITIES if c != origin])
    ship_date = base_date + timedelta(days=random.randint(0, 80))
    transit_days = random.randint(2, 14)
    estimated_delivery = ship_date + timedelta(days=transit_days)
    weight_kg = round(random.uniform(0.5, 150.0), 2)
    shipping_cost = round(random.uniform(5.0, 500.0), 2)
    carrier = random.choice(CARRIERS)
    priority = random.choice(PRIORITIES)

    # Assign status with distribution
    r = random.random()
    if r < 0.45:
        status = "Delivered"
        actual_delivery = estimated_delivery + timedelta(days=random.randint(-2, 3))
    elif r < 0.75:
        status = "In Transit"
        actual_delivery = None
    elif r < 0.90:
        status = "Processing"
        actual_delivery = None
    else:
        status = "Returned"
        actual_delivery = None

    rows.append((
        shipment_id, origin, dest,
        ship_date.isoformat(), estimated_delivery.isoformat(),
        actual_delivery.isoformat() if actual_delivery else None,
        weight_kg, shipping_cost, carrier, status, priority,
    ))

schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("origin_city", StringType(), False),
    StructField("destination_city", StringType(), False),
    StructField("ship_date", StringType(), False),
    StructField("estimated_delivery", StringType(), False),
    StructField("actual_delivery", StringType(), True),
    StructField("weight_kg", DoubleType(), False),
    StructField("shipping_cost", DoubleType(), False),
    StructField("carrier", StringType(), False),
    StructField("status", StringType(), False),
    StructField("priority", StringType(), False),
])

df = spark.createDataFrame(rows, schema)

# Cast date strings to DATE type
df = (
    df
    .withColumn("ship_date", F.col("ship_date").cast("date"))
    .withColumn("estimated_delivery", F.col("estimated_delivery").cast("date"))
    .withColumn("actual_delivery", F.col("actual_delivery").cast("date"))
)

print(f"Generated {df.count()} rows")
print(f"Status distribution:")
df.groupBy("status").count().orderBy("status").show()

# ── Step 2: Create Iceberg V2 table with copy-on-write ───────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.default")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.default.{TABLE_NAME} (
        shipment_id STRING,
        origin_city STRING,
        destination_city STRING,
        ship_date DATE,
        estimated_delivery DATE,
        actual_delivery DATE,
        weight_kg DOUBLE,
        shipping_cost DOUBLE,
        carrier STRING,
        status STRING,
        priority STRING
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'copy-on-write',
        'write.update.mode' = 'copy-on-write',
        'write.merge.mode' = 'copy-on-write'
    )
""")

# Insert data — coalesce to 1 for a single data file
df.coalesce(1).writeTo(f"local.default.{TABLE_NAME}").append()
print("Snapshot 1: Loaded 120 shipments")

count1 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.default.{TABLE_NAME}").collect()[0].cnt
print(f"Row count after snapshot 1: {count1}")

# ── Step 3: UPDATE — Mark 20 "In Transit" shipments as "Delivered" ────
# Find first 20 "In Transit" shipments
in_transit = spark.sql(f"""
    SELECT shipment_id FROM local.default.{TABLE_NAME}
    WHERE status = 'In Transit'
    ORDER BY shipment_id
""").collect()

update_ids = [row.shipment_id for row in in_transit[:20]]
update_ids_str = ", ".join(f"'{sid}'" for sid in update_ids)

print(f"\nUpdating {len(update_ids)} shipments to 'Delivered':")
print(f"  IDs: {update_ids}")

spark.sql(f"""
    UPDATE local.default.{TABLE_NAME}
    SET status = 'Delivered',
        actual_delivery = DATE_ADD(estimated_delivery, 1)
    WHERE shipment_id IN ({update_ids_str})
""")
print("Snapshot 2: Updated 20 In Transit → Delivered (copy-on-write rewrite)")

count2 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.default.{TABLE_NAME}").collect()[0].cnt
print(f"Row count after snapshot 2: {count2}")

# ── Step 4: DELETE — Remove 10 cancelled/returned shipments ──────────
# Find 10 "Returned" + "Processing" shipments to delete
deletable = spark.sql(f"""
    SELECT shipment_id FROM local.default.{TABLE_NAME}
    WHERE status IN ('Returned', 'Processing')
    ORDER BY shipment_id
""").collect()

delete_ids = [row.shipment_id for row in deletable[:10]]
delete_ids_str = ", ".join(f"'{sid}'" for sid in delete_ids)

print(f"\nDeleting {len(delete_ids)} shipments:")
print(f"  IDs: {delete_ids}")

spark.sql(f"""
    DELETE FROM local.default.{TABLE_NAME}
    WHERE shipment_id IN ({delete_ids_str})
""")
print("Snapshot 3: Deleted 10 shipments (copy-on-write rewrite)")

count3 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.default.{TABLE_NAME}").collect()[0].cnt
print(f"Row count after snapshot 3: {count3}")

# ── Step 5: Verify no delete files exist ─────────────────────────────
table_loc = f"{WAREHOUSE}/default/{TABLE_NAME}"
data_dir = os.path.join(table_loc, "data")

delete_files = []
for root, dirs, files in os.walk(data_dir):
    for f in files:
        if "deletes" in f.lower():
            delete_files.append(f)

if delete_files:
    print(f"\nWARNING: Found delete files (unexpected for copy-on-write): {delete_files}")
else:
    print("\nConfirmed: NO delete files found (copy-on-write rewrites data files)")

# ── Step 6: Show metadata summary ────────────────────────────────────
metadata_dir = os.path.join(table_loc, "metadata")
meta_files = sorted([f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")])
latest_meta = os.path.join(metadata_dir, meta_files[-1])
with open(latest_meta) as f:
    meta = json.load(f)

print(f"\nMetadata file: {meta_files[-1]}")
print(f"Format version: {meta['format-version']}")
print(f"Number of snapshots: {len(meta['snapshots'])}")

for snap in meta["snapshots"]:
    summary = snap["summary"]
    op = summary.get("operation", "unknown")
    total = summary.get("total-records", "?")
    added = summary.get("added-data-files", "?")
    deleted = summary.get("deleted-data-files", "?")
    print(f"  Snapshot {snap['snapshot-id']}: op={op}, total-records={total}, "
          f"added-data-files={added}, deleted-data-files={deleted}")

# ── Step 7: Copy table to demo directory ──────────────────────────────
print(f"\nCopying table from {table_loc} to {TABLE_OUTPUT}")
shutil.copytree(
    table_loc,
    TABLE_OUTPUT,
    ignore=shutil.ignore_patterns("*.crc", "version-hint.text", ".version-hint.text.crc"),
)

# List all files
print("\nGenerated files:")
total_size = 0
for root, dirs, files in os.walk(TABLE_OUTPUT):
    for f in sorted(files):
        full = os.path.join(root, f)
        rel = os.path.relpath(full, TABLE_OUTPUT)
        size = os.path.getsize(full)
        total_size += size
        print(f"  {rel} ({size:,} bytes)")

print(f"\nTotal size: {total_size:,} bytes")

# Verify no delete files in output
output_delete_files = []
for root, dirs, files in os.walk(os.path.join(TABLE_OUTPUT, "data")):
    for f in files:
        if "deletes" in f.lower():
            output_delete_files.append(f)

if output_delete_files:
    print(f"WARNING: Delete files in output: {output_delete_files}")
else:
    print("Output verified: NO delete files present")

spark.stop()
print("\nDone!")
