#!/usr/bin/env python3
"""
Generate an Iceberg V2 table with native partition transforms using PySpark.

Scenario: Network traffic analysis — 480 packet records across 3 regions
(north-america, europe, asia-pacific), 4 protocols (TCP, UDP, ICMP, DNS),
and various threat levels (low, medium, high, critical).

The table uses Iceberg-native partition transforms:
  - bucket(8, source_ip) — hash-based bucketing on source IP
  - days(capture_time) — daily partitioning on capture timestamp

These are Iceberg-native transforms that Delta Lake cannot produce,
making this a pure Iceberg read test.

Output: network_traffic/ directory with Iceberg V2 metadata and partitioned
Parquet data files — ready for Delta Forge to read.
"""
import os
import sys
import shutil
import json
import random
from datetime import datetime, timedelta

# PySpark + Iceberg setup
ICEBERG_JAR = os.path.expanduser(
    "~/.ivy2.5.2/jars/org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
)
JAVA_HOME = os.path.expanduser("~/local/jdk")
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = f"{JAVA_HOME}/bin:{os.environ['PATH']}"

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_NAME = "network_traffic"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_partition_transforms_warehouse"

# Clean previous runs
for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergPartitionTransformsGenerator")
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

REGIONS = ["north-america", "europe", "asia-pacific"]
PROTOCOLS = ["TCP", "UDP", "ICMP", "DNS"]
THREAT_LEVELS = ["low", "medium", "high", "critical"]
PORTS = [22, 53, 80, 443, 993, 1433, 3306, 3389, 5432, 8080, 8443, 9200]

# Generate source/dest IPs deterministically
SOURCE_IPS = [f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}" for _ in range(60)]
DEST_IPS = [f"192.168.{random.randint(0,255)}.{random.randint(1,254)}" for _ in range(40)]

# Base timestamp: 2025-03-01 to 2025-03-10 (10 days)
base_ts = datetime(2025, 3, 1, 0, 0, 0)

rows = []
for i in range(480):
    region = REGIONS[i % 3]  # 160 per region exactly
    protocol = PROTOCOLS[i % 4]  # 120 per protocol exactly
    source_ip = SOURCE_IPS[i % len(SOURCE_IPS)]
    dest_ip = DEST_IPS[i % len(DEST_IPS)]
    port = PORTS[random.randint(0, len(PORTS) - 1)]
    bytes_transferred = random.randint(64, 1048576)  # 64 bytes to 1MB
    # Threat levels: weighted distribution
    threat_roll = random.random()
    if threat_roll < 0.50:
        threat_level = "low"
    elif threat_roll < 0.80:
        threat_level = "medium"
    elif threat_roll < 0.95:
        threat_level = "high"
    else:
        threat_level = "critical"
    # Spread across 10 days
    capture_time = base_ts + timedelta(
        days=random.randint(0, 9),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )

    rows.append((
        f"PKT-{i+1:05d}",
        source_ip,
        dest_ip,
        protocol,
        port,
        bytes_transferred,
        threat_level,
        capture_time,
        region,
    ))

schema = StructType([
    StructField("packet_id", StringType(), False),
    StructField("source_ip", StringType(), False),
    StructField("dest_ip", StringType(), False),
    StructField("protocol", StringType(), False),
    StructField("port", IntegerType(), False),
    StructField("bytes_transferred", LongType(), False),
    StructField("threat_level", StringType(), False),
    StructField("capture_time", TimestampType(), False),
    StructField("region", StringType(), False),
])

df = spark.createDataFrame(rows, schema)
print(f"Generated {df.count()} rows")

# ── Step 2: Create Iceberg V2 table with partition transforms ────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.network")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.network.{TABLE_NAME} (
        packet_id STRING NOT NULL,
        source_ip STRING NOT NULL,
        dest_ip STRING NOT NULL,
        protocol STRING NOT NULL,
        port INT NOT NULL,
        bytes_transferred LONG NOT NULL,
        threat_level STRING NOT NULL,
        capture_time TIMESTAMP NOT NULL,
        region STRING NOT NULL
    )
    USING iceberg
    PARTITIONED BY (bucket(8, source_ip), days(capture_time))
    TBLPROPERTIES (
        'format-version' = '2'
    )
""")

# Insert data
df.writeTo(f"local.network.{TABLE_NAME}").append()
print(f"Loaded data into Iceberg V2 partitioned table")

# ── Step 3: Compute proof values ─────────────────────────────────────
print("\n=== Proof Values ===")

proofs = spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT region) AS region_count,
        COUNT(DISTINCT protocol) AS protocol_count,
        COUNT(DISTINCT threat_level) AS threat_level_count,
        ROUND(SUM(bytes_transferred), 0) AS total_bytes,
        ROUND(AVG(bytes_transferred), 2) AS avg_bytes
    FROM local.network.{TABLE_NAME}
""").collect()[0]
for field in proofs.__fields__:
    print(f"  {field}: {getattr(proofs, field)}")

print("\n  Per-region counts:")
region_counts = spark.sql(f"""
    SELECT region, COUNT(*) as cnt, ROUND(SUM(bytes_transferred), 0) as total_bytes
    FROM local.network.{TABLE_NAME}
    GROUP BY region ORDER BY region
""").collect()
for r in region_counts:
    print(f"    {r.region}: count={r.cnt}, total_bytes={r.total_bytes}")

print("\n  Per-protocol counts:")
proto_counts = spark.sql(f"""
    SELECT protocol, COUNT(*) as cnt
    FROM local.network.{TABLE_NAME}
    GROUP BY protocol ORDER BY protocol
""").collect()
for p in proto_counts:
    print(f"    {p.protocol}: {p.cnt}")

print("\n  Per-threat-level counts:")
threat_counts = spark.sql(f"""
    SELECT threat_level, COUNT(*) as cnt
    FROM local.network.{TABLE_NAME}
    GROUP BY threat_level ORDER BY threat_level
""").collect()
for t in threat_counts:
    print(f"    {t.threat_level}: {t.cnt}")

print("\n  Bytes stats:")
bytes_stats = spark.sql(f"""
    SELECT
        ROUND(MIN(bytes_transferred), 0) as min_bytes,
        ROUND(MAX(bytes_transferred), 0) as max_bytes,
        ROUND(AVG(bytes_transferred), 2) as avg_bytes,
        ROUND(SUM(bytes_transferred), 0) as total_bytes
    FROM local.network.{TABLE_NAME}
""").collect()[0]
print(f"    min={bytes_stats.min_bytes}, max={bytes_stats.max_bytes}, avg={bytes_stats.avg_bytes}, total={bytes_stats.total_bytes}")

# Source IP with 10.X prefix count (for partition-aware query)
print("\n  Source IP '10.200' prefix count:")
ip_prefix = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM local.network.{TABLE_NAME}
    WHERE source_ip LIKE '10.200.%'
""").collect()[0]
print(f"    10.200.x.x: {ip_prefix.cnt}")

# Date range query
print("\n  Date range 2025-03-01 to 2025-03-03:")
date_range = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM local.network.{TABLE_NAME}
    WHERE capture_time >= TIMESTAMP '2025-03-01 00:00:00'
      AND capture_time < TIMESTAMP '2025-03-04 00:00:00'
""").collect()[0]
print(f"    rows in first 3 days: {date_range.cnt}")

# Critical threats count
print("\n  Critical threats:")
critical = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM local.network.{TABLE_NAME}
    WHERE threat_level = 'critical'
""").collect()[0]
print(f"    count: {critical.cnt}")

# High port (>8000) count
print("\n  High port (>8000):")
high_port = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM local.network.{TABLE_NAME}
    WHERE port > 8000
""").collect()[0]
print(f"    count: {high_port.cnt}")

# ── Step 4: Copy table to demo directory (without CRC files) ──────────
table_loc = f"{WAREHOUSE}/network/{TABLE_NAME}"
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
