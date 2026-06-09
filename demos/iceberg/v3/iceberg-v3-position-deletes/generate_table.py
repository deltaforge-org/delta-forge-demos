#!/usr/bin/env python3
"""
Generate an Iceberg V3 table with traditional Parquet position delete files.

Scenario: Equity trading desk — 480 stock trades across 4 exchanges
(NYSE, NASDAQ, LSE, TSE). A malfunctioning algorithmic trader (ALGO-X99)
submitted 24 erroneous trades on NYSE that were flagged and retracted
via position deletes, leaving 456 valid trades.

Strategy: Spark 4.0 + Iceberg 1.10.1 produces Puffin DVs by default for
V3 tables. To get traditional Parquet position-delete files in a V3 table,
we create the table as V2 (which uses Parquet position deletes), perform
the delete, then upgrade the format version to V3. This produces a valid
V3 table with V2-style position delete files — a real-world scenario when
upgrading existing V2 tables to V3.

Output: equity_trades/ directory with Iceberg V3 metadata, Parquet data
files, and Parquet position delete files — ready for DeltaForge to read.
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
TABLE_NAME = "equity_trades"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_v3_posdelete_warehouse"

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
    .appName("IcebergV3PositionDeleteGenerator")
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

random.seed(7777)

EXCHANGES = ["NYSE", "NASDAQ", "LSE", "TSE"]
TRADERS_NORMAL = [f"TRADER-{i:02d}" for i in range(1, 11)]  # 10 normal traders
TRADER_FAULTY = "ALGO-X99"  # Malfunctioning algo
ALL_TRADERS = TRADERS_NORMAL + [TRADER_FAULTY]

SYMBOLS = {
    "NYSE": ["AAPL", "MSFT", "JPM", "GS", "BA"],
    "NASDAQ": ["GOOG", "AMZN", "META", "NFLX", "TSLA"],
    "LSE": ["SHEL", "HSBA", "AZN", "ULVR", "BP"],
    "TSE": ["7203", "6758", "9984", "8306", "6861"],
}
SIDES = ["BUY", "SELL"]

rows = []
base_date = date(2025, 3, 1)
trade_id = 1

for exchange_idx, exchange in enumerate(EXCHANGES):
    symbols = SYMBOLS[exchange]
    # 120 trades per exchange = 480 total
    for i in range(120):
        # Faulty algo: first 24 trades on NYSE only
        if exchange == "NYSE" and i < 24:
            trader = TRADER_FAULTY
            # Erroneous trades: absurd quantities and prices (will be retracted)
            quantity = random.randint(100000, 999999)
            price = round(random.uniform(0.001, 0.01), 4)
            is_erroneous = True
        else:
            trader = random.choice(TRADERS_NORMAL)
            quantity = random.randint(10, 5000)
            price = round(random.uniform(10.0, 500.0), 2)
            is_erroneous = False

        symbol = random.choice(symbols)
        side = random.choice(SIDES)
        notional = round(quantity * price, 2)
        trade_date = (base_date + timedelta(days=random.randint(0, 59))).isoformat()

        rows.append((
            f"TRD-{trade_id:05d}",
            exchange,
            trader,
            symbol,
            side,
            quantity,
            price,
            notional,
            is_erroneous,
            trade_date,
        ))
        trade_id += 1

schema = StructType([
    StructField("trade_id", StringType(), False),
    StructField("exchange", StringType(), False),
    StructField("trader", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("side", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("notional", DoubleType(), False),
    StructField("is_erroneous", BooleanType(), False),
    StructField("trade_date", StringType(), False),
])

df = spark.createDataFrame(rows, schema)

print(f"Generated {df.count()} rows")
print(f"Erroneous trades (ALGO-X99): {df.filter(F.col('trader') == TRADER_FAULTY).count()}")

# ── Step 2: Create Iceberg V2 table (to get Parquet position deletes) ──
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.trading")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.trading.{TABLE_NAME} (
        trade_id STRING NOT NULL,
        exchange STRING NOT NULL,
        trader STRING NOT NULL,
        symbol STRING NOT NULL,
        side STRING NOT NULL,
        quantity INT NOT NULL,
        price DOUBLE NOT NULL,
        notional DOUBLE NOT NULL,
        is_erroneous BOOLEAN NOT NULL,
        trade_date STRING NOT NULL
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
df.coalesce(1).writeTo(f"local.trading.{TABLE_NAME}").append()
print(f"Loaded data into Iceberg V2 table")

# Verify pre-delete
pre_count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.trading.{TABLE_NAME}").collect()[0].cnt
print(f"Pre-delete count: {pre_count}")

# ── Step 3: Delete erroneous trades (creates Parquet position deletes) ──
spark.sql(f"""
    DELETE FROM local.trading.{TABLE_NAME}
    WHERE trader = '{TRADER_FAULTY}'
""")
print(f"Deleted erroneous trades ({TRADER_FAULTY})")

# Verify delete produced Parquet position delete files (not Puffin DVs)
table_loc = f"{WAREHOUSE}/trading/{TABLE_NAME}"
data_dir = os.path.join(table_loc, "data")
puffin_files = [f for f in os.listdir(data_dir) if f.endswith(".puffin")]
parquet_deletes = [f for f in os.listdir(data_dir) if "deletes" in f and f.endswith(".parquet")]
print(f"Delete files: {len(parquet_deletes)} Parquet, {len(puffin_files)} Puffin")
assert len(parquet_deletes) > 0, "Expected Parquet position delete files!"
assert len(puffin_files) == 0, "Got unexpected Puffin DVs — V2 should use Parquet!"

# ── Step 4: Upgrade to V3 format ──────────────────────────────────────
print("Upgrading table from V2 to V3...")
spark.sql(f"""
    ALTER TABLE local.trading.{TABLE_NAME}
    SET TBLPROPERTIES ('format-version' = '3')
""")
print("Format version upgraded to V3")

# Verify post-delete counts (should be unchanged by upgrade)
post_count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.trading.{TABLE_NAME}").collect()[0].cnt
print(f"Post-delete count (after V3 upgrade): {post_count}")
print(f"Rows deleted: {pre_count - post_count}")

# ── Step 5: Compute proof values ─────────────────────────────────────
print("\n=== Proof Values ===")
proofs = spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT exchange) AS exchange_count,
        COUNT(DISTINCT trader) AS trader_count,
        COUNT(DISTINCT symbol) AS symbol_count,
        SUM(CASE WHEN trader = '{TRADER_FAULTY}' THEN 1 ELSE 0 END) AS erroneous_rows,
        SUM(CASE WHEN is_erroneous THEN 1 ELSE 0 END) AS flagged_rows,
        ROUND(SUM(notional), 2) AS total_notional,
        ROUND(AVG(price), 2) AS avg_price,
        ROUND(AVG(quantity), 2) AS avg_quantity,
        SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) AS buy_count,
        SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) AS sell_count
    FROM local.trading.{TABLE_NAME}
""").collect()[0]
for field in proofs.__fields__:
    print(f"  {field}: {getattr(proofs, field)}")

# Per-exchange counts
print("\n  Per-exchange counts:")
exch_counts = spark.sql(f"""
    SELECT exchange, COUNT(*) as cnt,
           ROUND(SUM(notional), 2) as total_notional,
           ROUND(AVG(price), 2) as avg_price
    FROM local.trading.{TABLE_NAME}
    GROUP BY exchange ORDER BY exchange
""").collect()
for r in exch_counts:
    print(f"    {r.exchange}: count={r.cnt}, notional={r.total_notional}, avg_price={r.avg_price}")

# Per-side counts per exchange
print("\n  Per-exchange side breakdown:")
side_counts = spark.sql(f"""
    SELECT exchange, side, COUNT(*) as cnt
    FROM local.trading.{TABLE_NAME}
    GROUP BY exchange, side ORDER BY exchange, side
""").collect()
for r in side_counts:
    print(f"    {r.exchange}/{r.side}: {r.cnt}")

# Top symbols by trade count
print("\n  Top 5 symbols by count:")
sym_counts = spark.sql(f"""
    SELECT symbol, COUNT(*) as cnt
    FROM local.trading.{TABLE_NAME}
    GROUP BY symbol ORDER BY cnt DESC LIMIT 5
""").collect()
for r in sym_counts:
    print(f"    {r.symbol}: {r.cnt}")

# High-value trades
high_val = spark.sql(f"""
    SELECT COUNT(*) as cnt
    FROM local.trading.{TABLE_NAME}
    WHERE notional > 100000
""").collect()[0].cnt
print(f"\n  High-value trades (>$100K): {high_val}")

# Price range
price_range = spark.sql(f"""
    SELECT ROUND(MIN(price), 2) as min_p, ROUND(MAX(price), 2) as max_p
    FROM local.trading.{TABLE_NAME}
""").collect()[0]
print(f"  Price range: {price_range.min_p} - {price_range.max_p}")

# ── Step 6: Verify metadata ──────────────────────────────────────────
metadata_dir = os.path.join(table_loc, "metadata")
meta_files = sorted([f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")])
latest_meta = os.path.join(metadata_dir, meta_files[-1])
with open(latest_meta) as f:
    meta = json.load(f)

print(f"\nFormat version: {meta['format-version']}")
current_snap_id = meta["current-snapshot-id"]
for snap in meta["snapshots"]:
    if snap["snapshot-id"] == current_snap_id:
        summary = snap["summary"]
        print(f"Current snapshot ({snap['summary']['operation']}):")
        for k, v in sorted(summary.items()):
            if "delete" in k.lower() or "position" in k.lower() or "data-files" in k.lower():
                print(f"  {k}: {v}")
        break

# ── Step 7: Copy table to demo directory ──────────────────────────────
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
