#!/usr/bin/env python3
"""
Generate an Iceberg V2 table with multiple snapshots for time-travel demo.

Scenario: Stock price history with 4 snapshots representing end-of-day
price corrections, new IPO listings, and delisted ticker removals.

Snapshot 1: 120 records (20 tickers x 6 trading days)
Snapshot 2: UPDATE tech sector prices +5% (earnings beat correction)
Snapshot 3: INSERT 30 new IPO records (5 new tickers x 6 days)
Snapshot 4: DELETE 12 delisted records (2 tickers x 6 days)

Output: stock_prices/ directory with Iceberg V2 metadata chain.
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
TABLE_NAME = "stock_prices"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_time_travel_warehouse"

for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergNativeTimeTravelGenerator")
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
random.seed(2024)

# ── Seed data ────────────────────────────────────────────────────────
SECTORS = {
    "Technology": ["AAPL", "MSFT", "GOOG", "NVDA", "META", "CRM", "ADBE", "ORCL"],
    "Healthcare": ["JNJ", "PFE", "UNH", "MRK"],
    "Finance": ["JPM", "BAC", "GS", "MS"],
    "Energy": ["XOM", "CVX", "COP", "SLB"],
}

COMPANY_NAMES = {
    "AAPL": "Apple Inc", "MSFT": "Microsoft Corp", "GOOG": "Alphabet Inc",
    "NVDA": "NVIDIA Corp", "META": "Meta Platforms", "CRM": "Salesforce Inc",
    "ADBE": "Adobe Inc", "ORCL": "Oracle Corp",
    "JNJ": "Johnson & Johnson", "PFE": "Pfizer Inc",
    "UNH": "UnitedHealth Group", "MRK": "Merck & Co",
    "JPM": "JPMorgan Chase", "BAC": "Bank of America",
    "GS": "Goldman Sachs", "MS": "Morgan Stanley",
    "XOM": "Exxon Mobil", "CVX": "Chevron Corp",
    "COP": "ConocoPhillips", "SLB": "Schlumberger NV",
}

# Base prices per ticker (deterministic)
BASE_PRICES = {
    "AAPL": 185.50, "MSFT": 420.30, "GOOG": 175.20, "NVDA": 880.40,
    "META": 510.60, "CRM": 305.10, "ADBE": 590.80, "ORCL": 125.40,
    "JNJ": 155.70, "PFE": 28.90, "UNH": 530.20, "MRK": 125.80,
    "JPM": 195.30, "BAC": 37.40, "GS": 395.60, "MS": 88.70,
    "XOM": 105.20, "CVX": 155.30, "COP": 115.40, "SLB": 48.60,
}

TRADE_DATES = ["2025-01-06", "2025-01-07", "2025-01-08", "2025-01-09", "2025-01-10", "2025-01-13"]

# IPO tickers (for snapshot 3)
IPO_TICKERS = {
    "NWAI": ("NewAI Corp", "Technology", 45.00),
    "GRNH": ("GreenH Energy", "Energy", 32.50),
    "BIOT": ("BioTech Innovations", "Healthcare", 78.20),
    "FINX": ("FinX Digital", "Finance", 55.80),
    "QCMP": ("QuantumComp Inc", "Technology", 120.00),
}

# Delisted tickers (for snapshot 4) — pick 2 from Energy
DELISTED_TICKERS = ["COP", "SLB"]

# Build ticker->sector mapping
TICKER_SECTOR = {}
for sector, tickers in SECTORS.items():
    for t in tickers:
        TICKER_SECTOR[t] = sector

# ── Snapshot 1: Initial 120 rows ─────────────────────────────────────
rows = []
for ticker in sorted(BASE_PRICES.keys()):
    sector = TICKER_SECTOR[ticker]
    company = COMPANY_NAMES[ticker]
    base_price = BASE_PRICES[ticker]
    for day_idx, trade_date in enumerate(TRADE_DATES):
        # Deterministic daily variation based on seed
        price_var = random.uniform(-0.03, 0.03)
        price = round(base_price * (1 + price_var), 2)
        volume = random.randint(5000000, 80000000)
        market_cap = round(price * random.randint(1000000, 5000000000), 2)
        rows.append((ticker, company, price, volume, market_cap, sector, trade_date))

schema = StructType([
    StructField("ticker", StringType(), False),
    StructField("company_name", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("volume", LongType(), False),
    StructField("market_cap", DoubleType(), False),
    StructField("sector", StringType(), False),
    StructField("trade_date", StringType(), False),
])

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.stocks")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.stocks.{TABLE_NAME} (
        ticker STRING NOT NULL,
        company_name STRING NOT NULL,
        price DOUBLE NOT NULL,
        volume LONG NOT NULL,
        market_cap DOUBLE NOT NULL,
        sector STRING NOT NULL,
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

df1 = spark.createDataFrame(rows, schema)
df1.coalesce(1).writeTo(f"local.stocks.{TABLE_NAME}").append()

count1 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.stocks.{TABLE_NAME}").collect()[0].cnt
print(f"=== Snapshot 1: Initial load ===")
print(f"Row count: {count1}")

# ── Snapshot 2: UPDATE tech prices +5% ────────────────────────────────
spark.sql(f"""
    UPDATE local.stocks.{TABLE_NAME}
    SET price = ROUND(price * 1.05, 2),
        market_cap = ROUND(market_cap * 1.05, 2)
    WHERE sector = 'Technology'
""")

count2 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.stocks.{TABLE_NAME}").collect()[0].cnt
print(f"\n=== Snapshot 2: Tech price correction +5% ===")
print(f"Row count: {count2}")

# ── Snapshot 3: INSERT 30 IPO records ─────────────────────────────────
ipo_rows = []
for ticker, (company, sector, base_price) in sorted(IPO_TICKERS.items()):
    for day_idx, trade_date in enumerate(TRADE_DATES):
        price_var = random.uniform(-0.02, 0.04)
        price = round(base_price * (1 + price_var), 2)
        volume = random.randint(10000000, 50000000)
        market_cap = round(price * random.randint(100000, 500000000), 2)
        ipo_rows.append((ticker, company, price, volume, market_cap, sector, trade_date))

df3 = spark.createDataFrame(ipo_rows, schema)
df3.coalesce(1).writeTo(f"local.stocks.{TABLE_NAME}").append()

count3 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.stocks.{TABLE_NAME}").collect()[0].cnt
print(f"\n=== Snapshot 3: IPO insertions ===")
print(f"Row count: {count3}")

# ── Snapshot 4: DELETE delisted tickers ───────────────────────────────
delisted_list = ",".join(f"'{t}'" for t in DELISTED_TICKERS)
spark.sql(f"""
    DELETE FROM local.stocks.{TABLE_NAME}
    WHERE ticker IN ({delisted_list})
""")

count4 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.stocks.{TABLE_NAME}").collect()[0].cnt
print(f"\n=== Snapshot 4: Delisted tickers removed ===")
print(f"Row count: {count4}")

# ── Print proof values at final state ─────────────────────────────────
print(f"\n=== Final State Proof Values ===")
print(f"Total rows: {count4}")

# Per-sector breakdown
sector_counts = spark.sql(f"""
    SELECT sector, COUNT(*) as cnt, ROUND(AVG(price), 2) as avg_price,
           SUM(volume) as total_volume
    FROM local.stocks.{TABLE_NAME}
    GROUP BY sector ORDER BY sector
""").collect()
for row in sector_counts:
    print(f"  {row.sector}: count={row.cnt}, avg_price={row.avg_price}, total_volume={row.total_volume}")

# Verify deletions
deleted_check = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM local.stocks.{TABLE_NAME}
    WHERE ticker IN ({delisted_list})
""").collect()[0].cnt
print(f"Delisted ticker rows (should be 0): {deleted_check}")

# Verify IPO presence
ipo_list = ",".join(f"'{t}'" for t in sorted(IPO_TICKERS.keys()))
ipo_check = spark.sql(f"""
    SELECT COUNT(*) as cnt FROM local.stocks.{TABLE_NAME}
    WHERE ticker IN ({ipo_list})
""").collect()[0].cnt
print(f"IPO ticker rows (should be 30): {ipo_check}")

# Snapshot history
snapshots = spark.sql(f"SELECT * FROM local.stocks.{TABLE_NAME}.snapshots").collect()
print(f"\n=== Snapshot History ({len(snapshots)} snapshots) ===")
for snap in snapshots:
    print(f"  Snapshot ID: {snap.snapshot_id}, operation: {snap.operation}")

# ── Copy table to demo directory ──────────────────────────────────────
table_loc = f"{WAREHOUSE}/stocks/{TABLE_NAME}"
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
