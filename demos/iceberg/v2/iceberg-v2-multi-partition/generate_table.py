#!/usr/bin/env python3
"""Generate Iceberg V2 table with multi-column partitioning (region + year).

Scenario: Global Weather Station Readings — 450 weather observations
partitioned by region (identity) AND years(observation_date). Tests
multi-column partitioning in Iceberg V2 with partition pruning.

Output: weather_readings/ directory with Iceberg V2 metadata, partitioned
Parquet data files — ready for DeltaForge to read.
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
TABLE_NAME = "weather_readings"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_multi_partition_warehouse"

# Clean previous runs
for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergMultiPartition")
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

REGIONS = ['North America', 'Europe', 'Asia', 'South America', 'Africa']
YEARS = [2023, 2024, 2025]
CONDITIONS = ['Clear', 'Cloudy', 'Rain', 'Snow', 'Storm']

# Station IDs: 3 stations per region
STATION_PREFIXES = {
    'North America': 'NA',
    'Europe': 'EU',
    'Asia': 'AS',
    'South America': 'SA',
    'Africa': 'AF',
}

# Temperature ranges per region (min_c, max_c)
TEMP_RANGES = {
    'North America': (-10.0, 35.0),
    'Europe': (-5.0, 32.0),
    'Asia': (-8.0, 40.0),
    'South America': (5.0, 38.0),
    'Africa': (10.0, 40.0),
}

rows = []
reading_id = 1

for region in REGIONS:
    prefix = STATION_PREFIXES[region]
    stations = [f"WX-{prefix}{i:03d}" for i in range(1, 4)]
    temp_min, temp_max = TEMP_RANGES[region]

    for year in YEARS:
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)
        day_range = (year_end - year_start).days

        for _ in range(30):
            station = random.choice(stations)
            obs_date = year_start + timedelta(days=random.randint(0, day_range))
            temp = round(random.uniform(temp_min, temp_max), 1)
            humidity = round(random.uniform(20.0, 100.0), 1)
            wind = round(random.uniform(0.0, 120.0), 1)

            # ~15% NULL precipitation
            if random.random() < 0.15:
                precip = None
            else:
                precip = round(random.uniform(0.0, 50.0), 1)

            condition = random.choice(CONDITIONS)

            rows.append((
                reading_id,
                station,
                region,
                obs_date,
                temp,
                humidity,
                wind,
                precip,
                condition,
            ))
            reading_id += 1

print(f"Generated {len(rows)} rows")

schema = StructType([
    StructField("reading_id", IntegerType(), False),
    StructField("station_id", StringType(), False),
    StructField("region", StringType(), False),
    StructField("observation_date", DateType(), False),
    StructField("temperature_c", DoubleType(), False),
    StructField("humidity_pct", DoubleType(), False),
    StructField("wind_speed_kmh", DoubleType(), False),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("condition", StringType(), False),
])

df = spark.createDataFrame(rows, schema)

# ── Step 2: Create Iceberg V2 table with multi-column partitioning ───
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.default")

spark.sql(f"""
CREATE TABLE local.default.{TABLE_NAME} (
    reading_id INT,
    station_id STRING,
    region STRING,
    observation_date DATE,
    temperature_c DOUBLE,
    humidity_pct DOUBLE,
    wind_speed_kmh DOUBLE,
    precipitation_mm DOUBLE,
    condition STRING
) USING iceberg
PARTITIONED BY (region, years(observation_date))
TBLPROPERTIES ('format-version' = '2')
""")

# Insert data
df.writeTo(f"local.default.{TABLE_NAME}").append()
print(f"Loaded data into Iceberg V2 table")

# Verify count
total = spark.sql(f"SELECT COUNT(*) as cnt FROM local.default.{TABLE_NAME}").collect()[0].cnt
print(f"Total rows: {total}")

# ── Step 3: Compute proof values ─────────────────────────────────────
print("\n=== Proof Values ===")

# Overall stats
proofs = spark.sql(f"""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT station_id) AS station_count
FROM local.default.{TABLE_NAME}
""").collect()[0]
print(f"  total_rows: {proofs.total_rows}")
print(f"  region_count: {proofs.region_count}")
print(f"  station_count: {proofs.station_count}")

# Per-region counts
print("\n  Per-region counts:")
region_counts = spark.sql(f"""
SELECT region, COUNT(*) as cnt
FROM local.default.{TABLE_NAME}
GROUP BY region ORDER BY region
""").collect()
for r in region_counts:
    print(f"    {r.region}: {r.cnt}")

# Region + year partition filter test (Europe, 2024)
europe_2024 = spark.sql(f"""
SELECT COUNT(*) as cnt
FROM local.default.{TABLE_NAME}
WHERE region = 'Europe'
  AND observation_date >= '2024-01-01'
  AND observation_date < '2025-01-01'
""").collect()[0].cnt
print(f"\n  Europe 2024 count: {europe_2024}")

# Per-station aggregation
print("\n  Per-station aggregation:")
station_stats = spark.sql(f"""
SELECT station_id,
       ROUND(AVG(temperature_c), 2) AS avg_temp,
       ROUND(AVG(humidity_pct), 2) AS avg_humidity,
       COUNT(*) as cnt
FROM local.default.{TABLE_NAME}
GROUP BY station_id ORDER BY station_id
""").collect()
for s in station_stats:
    print(f"    {s.station_id}: cnt={s.cnt}, avg_temp={s.avg_temp}, avg_humidity={s.avg_humidity}")

# Weather condition distribution
print("\n  Weather condition distribution:")
cond_dist = spark.sql(f"""
SELECT condition, COUNT(*) as cnt
FROM local.default.{TABLE_NAME}
GROUP BY condition ORDER BY condition
""").collect()
for c in cond_dist:
    print(f"    {c.condition}: {c.cnt}")

# Year-over-year
print("\n  Year-over-year avg temperature:")
yoy = spark.sql(f"""
SELECT YEAR(observation_date) as obs_year,
       ROUND(AVG(temperature_c), 2) AS avg_temp,
       COUNT(*) as cnt
FROM local.default.{TABLE_NAME}
GROUP BY YEAR(observation_date) ORDER BY obs_year
""").collect()
for y in yoy:
    print(f"    {y.obs_year}: avg_temp={y.avg_temp}, cnt={y.cnt}")

# Extreme readings
extreme = spark.sql(f"""
SELECT COUNT(*) as cnt
FROM local.default.{TABLE_NAME}
WHERE temperature_c > 35 OR temperature_c < -5
""").collect()[0].cnt
print(f"\n  Extreme readings (>35 or <-5): {extreme}")

# NULL precipitation count
null_precip = spark.sql(f"""
SELECT COUNT(*) as cnt
FROM local.default.{TABLE_NAME}
WHERE precipitation_mm IS NULL
""").collect()[0].cnt
print(f"  NULL precipitation count: {null_precip}")

# Non-null precipitation count
nonnull_precip = spark.sql(f"""
SELECT COUNT(*) as cnt
FROM local.default.{TABLE_NAME}
WHERE precipitation_mm IS NOT NULL
""").collect()[0].cnt
print(f"  Non-null precipitation count: {nonnull_precip}")

# Grand totals for VERIFY
verify = spark.sql(f"""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT region) AS region_count,
    COUNT(DISTINCT station_id) AS station_count,
    COUNT(DISTINCT condition) AS condition_count,
    SUM(CASE WHEN temperature_c > 35 OR temperature_c < -5 THEN 1 ELSE 0 END) AS extreme_count,
    SUM(CASE WHEN precipitation_mm IS NULL THEN 1 ELSE 0 END) AS null_precip_count
FROM local.default.{TABLE_NAME}
""").collect()[0]
print(f"\n  VERIFY grand totals:")
print(f"    total_rows: {verify.total_rows}")
print(f"    region_count: {verify.region_count}")
print(f"    station_count: {verify.station_count}")
print(f"    condition_count: {verify.condition_count}")
print(f"    extreme_count: {verify.extreme_count}")
print(f"    null_precip_count: {verify.null_precip_count}")

# ── Step 4: Copy table to demo directory ─────────────────────────────
table_loc = f"{WAREHOUSE}/default/{TABLE_NAME}"
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

print(f"\nTotal data size: {total_size:,} bytes")

spark.stop()
print("\nDone!")
