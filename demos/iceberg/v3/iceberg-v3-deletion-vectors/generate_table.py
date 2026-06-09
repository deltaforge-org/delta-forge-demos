#!/usr/bin/env python3
"""
Generate an Iceberg V3 table with Puffin deletion vectors using PySpark.

Scenario: Global supply-chain logistics — 540 shipment records across
3 regions (Americas, EMEA, APAC). A faulty barcode scanner (SCAN-ERR)
in the Americas warehouse processed 36 shipments with incorrect weight
readings. Those records are retracted via Iceberg V3 deletion vectors
(Puffin-encoded bitmaps), leaving 504 valid shipments.

Deletion vectors are the V3-native delete mechanism: instead of separate
Parquet position-delete files (V2 style), the deleted row positions are
encoded as a RoaringBitmap stored in a Puffin file. This is the most
efficient row-level delete representation in Iceberg.

Output: shipment_manifests/ directory with Iceberg V3 metadata, data
files, and Puffin deletion vector files — ready for DeltaForge to read.
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
TABLE_NAME = "shipment_manifests"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_v3_dv_warehouse"

# Clean previous runs
for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergV3DeletionVectorGenerator")
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
from datetime import date, timedelta

random.seed(2025)

REGIONS = ["Americas", "EMEA", "APAC"]
CARRIERS = [
    "FedEx", "UPS", "DHL", "Maersk", "CMA-CGM", "Hapag-Lloyd",
    "Kuehne-Nagel", "DB-Schenker", "XPO-Logistics", "Nippon-Express",
    "SF-Express", "Aramex"
]
PRODUCT_CATEGORIES = [
    "Electronics", "Pharmaceuticals", "Automotive-Parts",
    "Perishable-Foods", "Textiles", "Heavy-Machinery"
]
DESTINATION_COUNTRIES = {
    "Americas": ["US", "CA", "MX", "BR", "AR", "CL"],
    "EMEA": ["GB", "DE", "FR", "AE", "ZA", "NG"],
    "APAC": ["JP", "AU", "SG", "KR", "IN", "NZ"],
}
SCANNER_FAULTY = "SCAN-ERR"
SCANNERS_NORMAL = [f"SCAN-{i:02d}" for i in range(1, 16)]

rows = []
base_date = date(2025, 1, 15)
shipment_id = 1

for region_idx, region in enumerate(REGIONS):
    countries = DESTINATION_COUNTRIES[region]
    # 180 shipments per region = 540 total
    for i in range(180):
        # Faulty scanner: first 36 shipments in Americas only
        if region == "Americas" and i < 36:
            scanner = SCANNER_FAULTY
            # Faulty scanner records wildly wrong weights (will be retracted)
            weight_kg = round(random.uniform(5000.0, 99999.0), 2)
            is_weight_error = True
        else:
            scanner = random.choice(SCANNERS_NORMAL)
            # Normal weights: 0.5 kg to 2500 kg
            weight_kg = round(random.uniform(0.5, 2500.0), 2)
            is_weight_error = False

        carrier = random.choice(CARRIERS)
        category = random.choice(PRODUCT_CATEGORIES)
        country = random.choice(countries)
        declared_value = round(random.uniform(50.0, 50000.0), 2)
        is_hazardous = random.random() < 0.12  # ~12% hazardous
        ship_date = (base_date + timedelta(days=random.randint(0, 89))).isoformat()

        rows.append((
            f"SH-{shipment_id:05d}",
            region,
            carrier,
            category,
            scanner,
            weight_kg,
            declared_value,
            is_hazardous,
            country,
            ship_date,
        ))
        shipment_id += 1

schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("region", StringType(), False),
    StructField("carrier", StringType(), False),
    StructField("product_category", StringType(), False),
    StructField("scanner_id", StringType(), False),
    StructField("weight_kg", DoubleType(), False),
    StructField("declared_value", DoubleType(), False),
    StructField("is_hazardous", BooleanType(), False),
    StructField("destination_country", StringType(), False),
    StructField("ship_date", StringType(), False),
])

df = spark.createDataFrame(rows, schema)

print(f"Generated {df.count()} rows")
print(f"Faulty scanner rows: {df.filter(F.col('scanner_id') == SCANNER_FAULTY).count()}")

# ── Step 2: Create Iceberg V3 table and load data ────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.logistics")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.logistics.{TABLE_NAME} (
        shipment_id STRING NOT NULL,
        region STRING NOT NULL,
        carrier STRING NOT NULL,
        product_category STRING NOT NULL,
        scanner_id STRING NOT NULL,
        weight_kg DOUBLE NOT NULL,
        declared_value DOUBLE NOT NULL,
        is_hazardous BOOLEAN NOT NULL,
        destination_country STRING NOT NULL,
        ship_date STRING NOT NULL
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '3',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read'
    )
""")

# Insert data — coalesce to 1 partition for a single data file
df.coalesce(1).writeTo(f"local.logistics.{TABLE_NAME}").append()
print(f"Loaded data into Iceberg V3 table")

# Verify pre-delete
pre_count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.logistics.{TABLE_NAME}").collect()[0].cnt
print(f"Pre-delete count: {pre_count}")

# ── Step 3: Delete faulty scanner records (creates Puffin DVs) ──
spark.sql(f"""
    DELETE FROM local.logistics.{TABLE_NAME}
    WHERE scanner_id = '{SCANNER_FAULTY}'
""")
print(f"Deleted faulty scanner records ({SCANNER_FAULTY})")

# Verify post-delete
post_count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.logistics.{TABLE_NAME}").collect()[0].cnt
print(f"Post-delete count: {post_count}")
print(f"Rows deleted: {pre_count - post_count}")

# ── Step 4: Compute proof values ─────────────────────────────────────
print("\n=== Proof Values ===")
proofs = spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT region) AS region_count,
        COUNT(DISTINCT carrier) AS carrier_count,
        COUNT(DISTINCT scanner_id) AS scanner_count,
        SUM(CASE WHEN scanner_id = '{SCANNER_FAULTY}' THEN 1 ELSE 0 END) AS faulty_rows,
        SUM(CASE WHEN is_hazardous THEN 1 ELSE 0 END) AS hazardous_count,
        COUNT(DISTINCT product_category) AS category_count,
        COUNT(DISTINCT destination_country) AS country_count,
        ROUND(SUM(weight_kg), 2) AS total_weight,
        ROUND(SUM(declared_value), 2) AS total_value,
        ROUND(AVG(weight_kg), 2) AS avg_weight,
        ROUND(AVG(declared_value), 2) AS avg_value
    FROM local.logistics.{TABLE_NAME}
""").collect()[0]
for field in proofs.__fields__:
    print(f"  {field}: {getattr(proofs, field)}")

# Per-region counts
print("\n  Per-region counts:")
region_counts = spark.sql(f"""
    SELECT region, COUNT(*) as cnt,
           ROUND(AVG(weight_kg), 2) as avg_weight,
           SUM(CASE WHEN is_hazardous THEN 1 ELSE 0 END) as hazardous
    FROM local.logistics.{TABLE_NAME}
    GROUP BY region ORDER BY region
""").collect()
for r in region_counts:
    print(f"    {r.region}: count={r.cnt}, avg_weight={r.avg_weight}, hazardous={r.hazardous}")

# Per-category counts
print("\n  Per-category counts:")
cat_counts = spark.sql(f"""
    SELECT product_category, COUNT(*) as cnt
    FROM local.logistics.{TABLE_NAME}
    GROUP BY product_category ORDER BY product_category
""").collect()
for c in cat_counts:
    print(f"    {c.product_category}: {c.cnt}")

# Per-carrier counts
print("\n  Per-carrier counts:")
carrier_counts = spark.sql(f"""
    SELECT carrier, COUNT(*) as cnt
    FROM local.logistics.{TABLE_NAME}
    GROUP BY carrier ORDER BY carrier
""").collect()
for c in carrier_counts:
    print(f"    {c.carrier}: {c.cnt}")

# Weight range
print("\n  Weight range:")
weight_range = spark.sql(f"""
    SELECT ROUND(MIN(weight_kg), 2) as min_w, ROUND(MAX(weight_kg), 2) as max_w
    FROM local.logistics.{TABLE_NAME}
""").collect()[0]
print(f"    min={weight_range.min_w}, max={weight_range.max_w}")

# Low-value shipments
low_val = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM local.logistics.{TABLE_NAME}
    WHERE declared_value < 500
""").collect()[0].cnt
print(f"  Low-value shipments (<$500): {low_val}")

# ── Step 5: Verify Puffin DV files exist ─────────────────────────────
table_loc = f"{WAREHOUSE}/logistics/{TABLE_NAME}"
metadata_dir = os.path.join(table_loc, "metadata")

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

# List data directory for Puffin files
data_dir = os.path.join(table_loc, "data")
puffin_files = [f for f in os.listdir(data_dir) if f.endswith(".puffin")]
parquet_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]
print(f"\nData files: {len(parquet_files)} Parquet, {len(puffin_files)} Puffin DVs")
for f in puffin_files:
    print(f"  DV: {f} ({os.path.getsize(os.path.join(data_dir, f))} bytes)")

# ── Step 6: Copy table to demo directory (without CRC files) ──────────
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
