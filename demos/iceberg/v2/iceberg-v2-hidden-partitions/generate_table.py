#!/usr/bin/env python3
"""
Generate an Iceberg V2 table with hidden month() partitioning using PySpark.

Scenario: Ride-Share Trip Analytics — 300 trip records across 6 months
(Jan-Jun 2025), ~50 per month. The table uses Iceberg hidden partitioning
via months(pickup_date), meaning the partition column doesn't appear in the
data schema. Queries filter on pickup_date and Iceberg transparently prunes
partitions.

Columns: trip_id, driver_id, rider_id, pickup_date, pickup_time, dropoff_time,
         distance_miles, fare_amount, tip_amount, payment_type, city

Output: trips/ directory with Iceberg V2 metadata and partitioned Parquet
data files — ready for DeltaForge to read.
"""
import os
import sys
import shutil
import random
from datetime import date, timedelta

# PySpark + Iceberg setup
ICEBERG_JAR = os.path.expanduser(
    "~/.ivy2.5.2/jars/org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
)
JAVA_HOME = os.path.expanduser("~/local/jdk")
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = f"{JAVA_HOME}/bin:{os.environ['PATH']}"

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_NAME = "trips"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_hidden_partitions_warehouse"

# Clean previous runs
for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergHiddenPartitions")
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

CITIES = ["New York", "Chicago", "San Francisco", "Austin", "Seattle"]
PAYMENT_TYPES = ["Credit Card", "Cash", "Digital Wallet"]

# Generate 300 rows, 50 per month Jan-Jun 2025
rows = []
for i in range(300):
    trip_id = i + 1
    driver_id = f"DRV-{random.randint(1001, 1050):04d}"
    rider_id = f"RDR-{random.randint(2001, 2200):04d}"

    # Distribute evenly across 6 months (~50 per month)
    month_idx = i % 6  # 0=Jan, 1=Feb, ..., 5=Jun
    month = month_idx + 1
    # Random day within that month
    if month == 2:
        max_day = 28
    elif month in (4, 6):
        max_day = 30
    else:
        max_day = 31
    day = random.randint(1, max_day)
    pickup_date = date(2025, month, day)

    # Pickup time HH:MM format
    hour = random.randint(5, 23)
    minute = random.randint(0, 59)
    pickup_time = f"{hour:02d}:{minute:02d}"

    # Trip duration 5–60 minutes
    duration_min = random.randint(5, 60)
    dropoff_hour = hour + (minute + duration_min) // 60
    dropoff_minute = (minute + duration_min) % 60
    if dropoff_hour >= 24:
        dropoff_hour = 23
        dropoff_minute = 59
    dropoff_time = f"{dropoff_hour:02d}:{dropoff_minute:02d}"

    distance_miles = round(random.uniform(0.5, 25.0), 1)
    fare_amount = round(2.50 + distance_miles * random.uniform(1.8, 3.5), 2)
    payment_type = random.choice(PAYMENT_TYPES)

    # ~20% of cash rides have NULL tips
    if payment_type == "Cash" and random.random() < 0.20:
        tip_amount = None
    else:
        tip_amount = round(random.uniform(0.0, fare_amount * 0.30), 2)

    city = CITIES[i % 5]  # Round-robin for even distribution: 60 per city

    rows.append((
        trip_id,
        driver_id,
        rider_id,
        pickup_date,
        pickup_time,
        dropoff_time,
        distance_miles,
        fare_amount,
        tip_amount,
        payment_type,
        city,
    ))

schema = StructType([
    StructField("trip_id", IntegerType(), False),
    StructField("driver_id", StringType(), False),
    StructField("rider_id", StringType(), False),
    StructField("pickup_date", DateType(), False),
    StructField("pickup_time", StringType(), False),
    StructField("dropoff_time", StringType(), False),
    StructField("distance_miles", DoubleType(), False),
    StructField("fare_amount", DoubleType(), False),
    StructField("tip_amount", DoubleType(), True),
    StructField("payment_type", StringType(), False),
    StructField("city", StringType(), False),
])

df = spark.createDataFrame(rows, schema)
print(f"Generated {df.count()} rows")

# ── Step 2: Create Iceberg V2 table with hidden month partitioning ───
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.default")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.default.{TABLE_NAME} (
        trip_id INT,
        driver_id STRING,
        rider_id STRING,
        pickup_date DATE,
        pickup_time STRING,
        dropoff_time STRING,
        distance_miles DOUBLE,
        fare_amount DOUBLE,
        tip_amount DOUBLE,
        payment_type STRING,
        city STRING
    )
    USING iceberg
    PARTITIONED BY (months(pickup_date))
    TBLPROPERTIES (
        'format-version' = '2'
    )
""")

# Insert data
df.writeTo(f"local.default.{TABLE_NAME}").append()
print(f"Loaded data into Iceberg V2 table with hidden month(pickup_date) partitioning")

# ── Step 3: Compute proof values ─────────────────────────────────────
print("\n=== Proof Values ===")

proofs = spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT city) AS city_count,
        COUNT(DISTINCT payment_type) AS payment_type_count,
        COUNT(DISTINCT driver_id) AS driver_count,
        ROUND(SUM(fare_amount), 2) AS total_fare,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(SUM(distance_miles), 1) AS total_distance,
        ROUND(AVG(distance_miles), 2) AS avg_distance,
        SUM(CASE WHEN tip_amount IS NULL THEN 1 ELSE 0 END) AS null_tip_count
    FROM local.default.{TABLE_NAME}
""").collect()[0]
for field in proofs.__fields__:
    print(f"  {field}: {getattr(proofs, field)}")

print("\n  Per-month counts:")
month_counts = spark.sql(f"""
    SELECT MONTH(pickup_date) as m, COUNT(*) as cnt
    FROM local.default.{TABLE_NAME}
    GROUP BY MONTH(pickup_date)
    ORDER BY m
""").collect()
for r in month_counts:
    print(f"    month {r.m}: {r.cnt}")

print("\n  Per-city counts:")
city_counts = spark.sql(f"""
    SELECT city, COUNT(*) as cnt, ROUND(AVG(fare_amount), 2) as avg_fare
    FROM local.default.{TABLE_NAME}
    GROUP BY city ORDER BY city
""").collect()
for c in city_counts:
    print(f"    {c.city}: count={c.cnt}, avg_fare={c.avg_fare}")

print("\n  Per-payment-type:")
pay_counts = spark.sql(f"""
    SELECT
        payment_type,
        COUNT(*) as cnt,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        SUM(CASE WHEN tip_amount IS NULL THEN 1 ELSE 0 END) as null_tips
    FROM local.default.{TABLE_NAME}
    GROUP BY payment_type ORDER BY payment_type
""").collect()
for p in pay_counts:
    print(f"    {p.payment_type}: count={p.cnt}, avg_fare={p.avg_fare}, null_tips={p.null_tips}")

print("\n  March 2025 filter:")
march = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM local.default.{TABLE_NAME}
    WHERE pickup_date >= DATE '2025-03-01' AND pickup_date <= DATE '2025-03-31'
""").collect()[0]
print(f"    rows: {march.cnt}")

print("\n  Top city by avg distance:")
top_city_dist = spark.sql(f"""
    SELECT city, ROUND(AVG(distance_miles), 2) as avg_dist, COUNT(*) as cnt
    FROM local.default.{TABLE_NAME}
    GROUP BY city ORDER BY avg_dist DESC
""").collect()
for c in top_city_dist:
    print(f"    {c.city}: avg_dist={c.avg_dist}, cnt={c.cnt}")

print("\n  Driver trip counts (top 10):")
driver_counts = spark.sql(f"""
    SELECT driver_id, COUNT(*) as trip_count
    FROM local.default.{TABLE_NAME}
    GROUP BY driver_id ORDER BY trip_count DESC LIMIT 10
""").collect()
for d in driver_counts:
    print(f"    {d.driver_id}: {d.trip_count}")

print("\n  Distinct driver count:")
drv_cnt = spark.sql(f"""
    SELECT COUNT(DISTINCT driver_id) as cnt
    FROM local.default.{TABLE_NAME}
""").collect()[0]
print(f"    {drv_cnt.cnt}")

print("\n  Fare stats:")
fare_stats = spark.sql(f"""
    SELECT
        ROUND(MIN(fare_amount), 2) as min_fare,
        ROUND(MAX(fare_amount), 2) as max_fare,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(SUM(fare_amount), 2) as total_fare
    FROM local.default.{TABLE_NAME}
""").collect()[0]
print(f"    min={fare_stats.min_fare}, max={fare_stats.max_fare}, avg={fare_stats.avg_fare}, total={fare_stats.total_fare}")

print("\n  Distance stats:")
dist_stats = spark.sql(f"""
    SELECT
        ROUND(MIN(distance_miles), 1) as min_dist,
        ROUND(MAX(distance_miles), 1) as max_dist,
        ROUND(AVG(distance_miles), 2) as avg_dist,
        ROUND(SUM(distance_miles), 1) as total_dist
    FROM local.default.{TABLE_NAME}
""").collect()[0]
print(f"    min={dist_stats.min_dist}, max={dist_stats.max_dist}, avg={dist_stats.avg_dist}, total={dist_stats.total_dist}")

# ── Step 4: Copy table to demo directory (without CRC files) ──────────
table_loc = f"{WAREHOUSE}/default/{TABLE_NAME}"
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
