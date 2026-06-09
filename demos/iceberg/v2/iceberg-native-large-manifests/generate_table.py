#!/usr/bin/env python3
"""
Generate an Iceberg V2 table with many manifest files via repeated appends.

Scenario: Web analytics data ingested in 10 micro-batches of 60 rows each
(600 total). Each append creates a new snapshot and manifest entry, testing
manifest list -> manifest chain traversal at scale.

Output: web_analytics/ directory with Iceberg V2 metadata, 10 data files,
10+ manifest entries across the metadata chain.
"""
import os
import sys
import shutil
import json

ICEBERG_JAR = os.path.expanduser(
    "~/.ivy2.5.2/jars/org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
)
JAVA_HOME = os.path.expanduser("~/local/jdk")
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = f"{JAVA_HOME}/bin:{os.environ['PATH']}"

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_NAME = "web_analytics"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_large_manifests_warehouse"

for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergNativeLargeManifestsGenerator")
    .config("spark.jars", ICEBERG_JAR)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    .config("spark.sql.defaultCatalog", "local")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

import random
import uuid
random.seed(7777)

# ── Reference data ────────────────────────────────────────────────────
COUNTRIES = ["US", "UK", "DE", "FR", "JP", "BR", "IN", "AU", "CA", "MX"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
PAGE_URLS = [
    "/home", "/products", "/pricing", "/about", "/contact",
    "/blog", "/docs", "/signup", "/login", "/dashboard",
    "/settings", "/profile", "/cart", "/checkout", "/support",
]
REFERRERS = [
    "google.com", "facebook.com", "twitter.com", "linkedin.com",
    "direct", "bing.com", "reddit.com", "youtube.com",
    "github.com", "email-campaign",
]
USER_AGENTS = [
    "Mozilla/5.0 Chrome/120", "Mozilla/5.0 Firefox/121",
    "Mozilla/5.0 Safari/17.2", "Mozilla/5.0 Edge/120",
    "Mozilla/5.0 Chrome/119 Mobile", "Mozilla/5.0 Safari/17 Mobile",
]

schema = StructType([
    StructField("session_id", StringType(), False),
    StructField("user_agent", StringType(), False),
    StructField("page_url", StringType(), False),
    StructField("referrer", StringType(), False),
    StructField("time_on_page", IntegerType(), False),
    StructField("is_bounce", BooleanType(), False),
    StructField("event_count", IntegerType(), False),
    StructField("country", StringType(), False),
    StructField("device_type", StringType(), False),
])

# ── Create table ──────────────────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.analytics")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.analytics.{TABLE_NAME} (
        session_id STRING NOT NULL,
        user_agent STRING NOT NULL,
        page_url STRING NOT NULL,
        referrer STRING NOT NULL,
        time_on_page INT NOT NULL,
        is_bounce BOOLEAN NOT NULL,
        event_count INT NOT NULL,
        country STRING NOT NULL,
        device_type STRING NOT NULL
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '2'
    )
""")

# ── 10 appends of 60 rows each ───────────────────────────────────────
BATCHES = 10
ROWS_PER_BATCH = 60

for batch in range(BATCHES):
    rows = []
    for i in range(ROWS_PER_BATCH):
        session_id = f"sess-{batch:02d}-{i:03d}-{uuid.uuid4().hex[:8]}"
        user_agent = random.choice(USER_AGENTS)
        page_url = random.choice(PAGE_URLS)
        referrer = random.choice(REFERRERS)
        time_on_page = random.randint(3, 600)
        is_bounce = random.random() < 0.30  # 30% bounce rate target
        event_count = 1 if is_bounce else random.randint(2, 25)
        country = random.choice(COUNTRIES)
        device_type = random.choice(DEVICE_TYPES)
        rows.append((session_id, user_agent, page_url, referrer,
                      time_on_page, is_bounce, event_count, country, device_type))

    df = spark.createDataFrame(rows, schema)
    df.coalesce(1).writeTo(f"local.analytics.{TABLE_NAME}").append()
    current = spark.sql(f"SELECT COUNT(*) as cnt FROM local.analytics.{TABLE_NAME}").collect()[0].cnt
    print(f"Batch {batch+1}/{BATCHES}: appended {ROWS_PER_BATCH} rows (total: {current})")

# ── Print proof values ────────────────────────────────────────────────
total = spark.sql(f"SELECT COUNT(*) as cnt FROM local.analytics.{TABLE_NAME}").collect()[0].cnt
print(f"\n=== Final State ===")
print(f"Total rows: {total}")

# Per-country
country_counts = spark.sql(f"""
    SELECT country, COUNT(*) as cnt
    FROM local.analytics.{TABLE_NAME}
    GROUP BY country ORDER BY country
""").collect()
print("\nPer-country:")
for row in country_counts:
    print(f"  {row.country}: {row.cnt}")

# Per-device
device_counts = spark.sql(f"""
    SELECT device_type, COUNT(*) as cnt
    FROM local.analytics.{TABLE_NAME}
    GROUP BY device_type ORDER BY device_type
""").collect()
print("\nPer-device:")
for row in device_counts:
    print(f"  {row.device_type}: {row.cnt}")

# Bounce rate
bounce_info = spark.sql(f"""
    SELECT
        SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) as bounce_count,
        ROUND(AVG(CASE WHEN is_bounce THEN 1.0 ELSE 0.0 END) * 100, 2) as bounce_pct
    FROM local.analytics.{TABLE_NAME}
""").collect()[0]
print(f"\nBounce count: {bounce_info.bounce_count}")
print(f"Bounce rate: {bounce_info.bounce_pct}%")

# Avg time on page by device
avg_time = spark.sql(f"""
    SELECT device_type, ROUND(AVG(time_on_page), 2) as avg_top
    FROM local.analytics.{TABLE_NAME}
    GROUP BY device_type ORDER BY device_type
""").collect()
print("\nAvg time_on_page by device:")
for row in avg_time:
    print(f"  {row.device_type}: {row.avg_top}")

# Avg event_count
avg_ec = spark.sql(f"""
    SELECT ROUND(AVG(event_count), 2) as avg_ec
    FROM local.analytics.{TABLE_NAME}
""").collect()[0].avg_ec
print(f"\nAvg event_count: {avg_ec}")

# Snapshot / manifest info
snapshots = spark.sql(f"SELECT * FROM local.analytics.{TABLE_NAME}.snapshots").collect()
print(f"\nSnapshots: {len(snapshots)}")
for s in snapshots:
    print(f"  ID={s.snapshot_id}, op={s.operation}")

# Metadata file count
table_loc = f"{WAREHOUSE}/analytics/{TABLE_NAME}"
metadata_dir = os.path.join(table_loc, "metadata")
meta_jsons = [f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")]
manifest_lists = [f for f in os.listdir(metadata_dir) if f.startswith("snap-") and f.endswith(".avro")]
manifests = [f for f in os.listdir(metadata_dir) if f.endswith("-m0.avro") or "-m" in f and f.endswith(".avro") and not f.startswith("snap-")]
print(f"\nMetadata files: {len(meta_jsons)}")
print(f"Manifest lists: {len(manifest_lists)}")
print(f"Manifest files (approx): {len([f for f in os.listdir(metadata_dir) if f.endswith('.avro') and not f.startswith('snap-')])}")

# ── Copy table to demo directory ──────────────────────────────────────
print(f"\nCopying table from {table_loc} to {TABLE_OUTPUT}")
shutil.copytree(
    table_loc,
    TABLE_OUTPUT,
    ignore=shutil.ignore_patterns("*.crc", "version-hint.text", ".version-hint.text.crc"),
)

print("\nGenerated files:")
for root, dirs, files in os.walk(TABLE_OUTPUT):
    for f in sorted(files):
        full = os.path.join(root, f)
        rel = os.path.relpath(full, TABLE_OUTPUT)
        size = os.path.getsize(full)
        print(f"  {rel} ({size:,} bytes)")

spark.stop()
print("\nDone!")
