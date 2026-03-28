#!/usr/bin/env python3
"""
Generate an Iceberg V2 table for window function analytics demo.

Scenario: Airline loyalty program — 60 frequent flyer members across
4 tiers (Bronze, Silver, Gold, Platinum) and 5 home airports (JFK, LAX,
ORD, ATL, DFW). Queries exercise ROW_NUMBER, RANK, DENSE_RANK, LAG,
LEAD, NTILE, running SUM, and CTE-based top-N-per-group patterns.

Output: loyalty_members/ directory with Iceberg V2 metadata chain.
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
TABLE_NAME = "loyalty_members"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_v2_window_warehouse"

for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergV2WindowAnalyticsGenerator")
    .config("spark.jars", ICEBERG_JAR)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    .config("spark.sql.defaultCatalog", "local")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Seed data ────────────────────────────────────────────────────────
import random
random.seed(42)

AIRPORTS = ["JFK", "LAX", "ORD", "ATL", "DFW"]
TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
# Distribution: 20 Bronze, 18 Silver, 14 Gold, 8 Platinum = 60
TIER_COUNTS = {"Bronze": 20, "Silver": 18, "Gold": 14, "Platinum": 8}

FIRST_NAMES = [
    "James", "Maria", "Kenji", "Priya", "Hans", "Fatima", "Carlos",
    "Yuki", "Oliver", "Aisha", "Liam", "Sophie", "Ravi", "Elena",
    "Omar", "Grace", "Wei", "Anna", "David", "Nina", "Tariq",
    "Clara", "Marcus", "Ingrid", "Raj", "Helena", "Ahmed", "Julia",
    "Thomas", "Mei", "Patrick", "Zara", "Erik", "Nadia", "Lucas",
    "Emma", "Kwame", "Isabella", "Sven", "Amara", "Diego", "Hana",
    "Victor", "Leila", "Finn", "Chiara", "Nikolai", "Rosa", "Hugo",
    "Anya", "Paulo", "Freya", "Idris", "Marta", "Leo", "Sakura",
    "Andre", "Lina", "Chen", "Petra",
]

LAST_NAMES = [
    "Chen", "Fischer", "Tanaka", "Patel", "Mueller", "Hassan",
    "Vega", "Sato", "Smith", "Mohammed", "Murphy", "Laurent",
    "Krishnan", "Petrova", "Diallo", "Okafor", "Zhang", "Johansson",
    "Kim", "Ivanova", "Al-Rashid", "Hoffmann", "Garcia", "Lindgren",
    "Gupta", "Novak", "Khalil", "Santos", "Andersen", "Wong",
    "O'Connor", "Sharma", "Erikson", "Kozlova", "Rivera", "Clarke",
    "Mensah", "Rossi", "Berg", "Toure", "Mendez", "Watanabe",
    "Popov", "Khoury", "Larsen", "Bianchi", "Volkov", "Herrera",
    "Berger", "Sokolova", "Costa", "Nilsson", "Osei", "Fernandez",
    "Park", "Yamamoto", "Dubois", "Al-Farsi", "Wei", "Novotny",
]

# Miles, flights, spend ranges per tier (deterministic per member)
TIER_RANGES = {
    "Bronze":   {"miles": (1000, 15000),   "flights": (2, 12),  "spend": (200, 3000)},
    "Silver":   {"miles": (15000, 40000),  "flights": (10, 30), "spend": (3000, 8000)},
    "Gold":     {"miles": (40000, 80000),  "flights": (25, 55), "spend": (8000, 18000)},
    "Platinum": {"miles": (80000, 200000), "flights": (50, 120),"spend": (18000, 50000)},
}

JOIN_YEARS = list(range(2018, 2025))

rows = []
member_id = 1
for tier, count in TIER_COUNTS.items():
    ranges = TIER_RANGES[tier]
    for i in range(count):
        airport = AIRPORTS[member_id % len(AIRPORTS)]
        miles = random.randint(*ranges["miles"])
        flights = random.randint(*ranges["flights"])
        spend = round(random.uniform(*ranges["spend"]), 2)
        join_year = random.choice(JOIN_YEARS)
        join_month = random.randint(1, 12)
        join_day = random.randint(1, 28)
        join_date = f"{join_year}-{join_month:02d}-{join_day:02d}"
        last_month = random.randint(1, 6)
        last_day = random.randint(1, 28)
        last_flight = f"2025-{last_month:02d}-{last_day:02d}"

        rows.append((
            member_id,
            f"{FIRST_NAMES[member_id - 1]} {LAST_NAMES[member_id - 1]}",
            tier,
            miles,
            flights,
            spend,
            join_date,
            airport,
            last_flight,
        ))
        member_id += 1

schema = StructType([
    StructField("member_id", IntegerType(), False),
    StructField("member_name", StringType(), False),
    StructField("tier", StringType(), False),
    StructField("miles_ytd", IntegerType(), False),
    StructField("flights_ytd", IntegerType(), False),
    StructField("spend_ytd", DoubleType(), False),
    StructField("join_date", StringType(), False),
    StructField("home_airport", StringType(), False),
    StructField("last_flight_date", StringType(), False),
])

df = spark.createDataFrame(rows, schema)

# ── Create Iceberg V2 table ──────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.airline")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.airline.{TABLE_NAME} (
        member_id INT NOT NULL,
        member_name STRING NOT NULL,
        tier STRING NOT NULL,
        miles_ytd INT NOT NULL,
        flights_ytd INT NOT NULL,
        spend_ytd DOUBLE NOT NULL,
        join_date STRING NOT NULL,
        home_airport STRING NOT NULL,
        last_flight_date STRING NOT NULL
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
    )
""")

df.coalesce(1).writeTo(f"local.airline.{TABLE_NAME}").append()

count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.airline.{TABLE_NAME}").collect()[0].cnt
print(f"Loaded {count} rows into Iceberg V2 table")

# ── Compute ALL proof values ─────────────────────────────────────────
print("\n=== Proof Values ===")

# Basic counts
print(f"\nTotal rows: {count}")

# Per-tier
tier_stats = spark.sql(f"""
    SELECT tier, COUNT(*) as cnt,
           SUM(miles_ytd) as total_miles,
           SUM(flights_ytd) as total_flights,
           ROUND(SUM(spend_ytd), 2) as total_spend,
           ROUND(AVG(spend_ytd), 2) as avg_spend
    FROM local.airline.{TABLE_NAME}
    GROUP BY tier ORDER BY tier
""").collect()
print("\nPer-tier stats:")
for r in tier_stats:
    print(f"  {r.tier}: count={r.cnt}, total_miles={r.total_miles}, "
          f"total_flights={r.total_flights}, total_spend={r.total_spend}, avg_spend={r.avg_spend}")

# Per-airport
airport_stats = spark.sql(f"""
    SELECT home_airport, COUNT(*) as cnt,
           ROUND(SUM(spend_ytd), 2) as total_spend
    FROM local.airline.{TABLE_NAME}
    GROUP BY home_airport ORDER BY home_airport
""").collect()
print("\nPer-airport stats:")
for r in airport_stats:
    print(f"  {r.home_airport}: count={r.cnt}, total_spend={r.total_spend}")

# Grand totals
totals = spark.sql(f"""
    SELECT
        COUNT(*) as total_rows,
        SUM(miles_ytd) as total_miles,
        SUM(flights_ytd) as total_flights,
        ROUND(SUM(spend_ytd), 2) as total_spend,
        ROUND(AVG(miles_ytd), 2) as avg_miles,
        ROUND(AVG(spend_ytd), 2) as avg_spend,
        COUNT(DISTINCT tier) as tier_count,
        COUNT(DISTINCT home_airport) as airport_count
    FROM local.airline.{TABLE_NAME}
""").collect()[0]
print(f"\nGrand totals:")
for field in totals.__fields__:
    print(f"  {field}: {getattr(totals, field)}")

# Window function proof values
print("\n--- Window function proofs ---")

# ROW_NUMBER overall by miles DESC
rn_top3 = spark.sql(f"""
    SELECT member_id, member_name, tier, miles_ytd,
           ROW_NUMBER() OVER (ORDER BY miles_ytd DESC) as overall_rank
    FROM local.airline.{TABLE_NAME}
    ORDER BY miles_ytd DESC
    LIMIT 5
""").collect()
print("\nTop 5 by miles (ROW_NUMBER):")
for r in rn_top3:
    print(f"  rank={r.overall_rank}, id={r.member_id}, name={r.member_name}, "
          f"tier={r.tier}, miles={r.miles_ytd}")

# RANK by spend within each tier — top per tier
rank_per_tier = spark.sql(f"""
    WITH ranked AS (
        SELECT member_id, member_name, tier, spend_ytd,
               RANK() OVER (PARTITION BY tier ORDER BY spend_ytd DESC) as spend_rank
        FROM local.airline.{TABLE_NAME}
    )
    SELECT * FROM ranked WHERE spend_rank = 1
    ORDER BY tier
""").collect()
print("\nTop spender per tier (RANK=1):")
for r in rank_per_tier:
    print(f"  tier={r.tier}, id={r.member_id}, name={r.member_name}, spend={r.spend_ytd}")

# NTILE(4) quartiles by spend
ntile_counts = spark.sql(f"""
    WITH quartiled AS (
        SELECT member_id, spend_ytd,
               NTILE(4) OVER (ORDER BY spend_ytd) as quartile
        FROM local.airline.{TABLE_NAME}
    )
    SELECT quartile, COUNT(*) as cnt,
           ROUND(MIN(spend_ytd), 2) as min_spend,
           ROUND(MAX(spend_ytd), 2) as max_spend
    FROM quartiled
    GROUP BY quartile ORDER BY quartile
""").collect()
print("\nNTILE(4) quartiles by spend:")
for r in ntile_counts:
    print(f"  Q{r.quartile}: count={r.cnt}, min={r.min_spend}, max={r.max_spend}")

# Running sum of miles within Platinum tier
running_platinum = spark.sql(f"""
    SELECT member_id, member_name, miles_ytd,
           SUM(miles_ytd) OVER (ORDER BY miles_ytd ROWS UNBOUNDED PRECEDING) as running_total
    FROM local.airline.{TABLE_NAME}
    WHERE tier = 'Platinum'
    ORDER BY miles_ytd
""").collect()
print("\nPlatinum running sum by miles:")
for r in running_platinum:
    print(f"  id={r.member_id}, miles={r.miles_ytd}, running_total={r.running_total}")

# LAG/LEAD on top 5 by miles
lag_lead = spark.sql(f"""
    WITH ordered AS (
        SELECT member_id, member_name, miles_ytd,
               LAG(miles_ytd, 1) OVER (ORDER BY miles_ytd DESC) as prev_miles,
               LEAD(miles_ytd, 1) OVER (ORDER BY miles_ytd DESC) as next_miles
        FROM local.airline.{TABLE_NAME}
    )
    SELECT * FROM ordered ORDER BY miles_ytd DESC LIMIT 5
""").collect()
print("\nLAG/LEAD on top 5 by miles:")
for r in lag_lead:
    print(f"  id={r.member_id}, miles={r.miles_ytd}, prev={r.prev_miles}, next={r.next_miles}")

# Top 3 per airport by spend (CTE + ROW_NUMBER)
top3_per_airport = spark.sql(f"""
    WITH ranked AS (
        SELECT member_id, member_name, home_airport, spend_ytd,
               ROW_NUMBER() OVER (PARTITION BY home_airport ORDER BY spend_ytd DESC) as rn
        FROM local.airline.{TABLE_NAME}
    )
    SELECT * FROM ranked WHERE rn <= 3
    ORDER BY home_airport, rn
""").collect()
print(f"\nTop 3 per airport by spend ({len(top3_per_airport)} rows):")
for r in top3_per_airport:
    print(f"  {r.home_airport} #{r.rn}: id={r.member_id}, name={r.member_name}, spend={r.spend_ytd}")

# DENSE_RANK by flights within each tier
dense_rank_flights = spark.sql(f"""
    SELECT tier, COUNT(DISTINCT flights_ytd) as distinct_flight_counts
    FROM local.airline.{TABLE_NAME}
    GROUP BY tier ORDER BY tier
""").collect()
print("\nDistinct flight counts per tier (for DENSE_RANK):")
for r in dense_rank_flights:
    print(f"  {r.tier}: {r.distinct_flight_counts} distinct values")

# ── Copy table to demo directory ──────────────────────────────────────
table_loc = f"{WAREHOUSE}/airline/{TABLE_NAME}"
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
