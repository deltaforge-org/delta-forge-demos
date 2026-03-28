#!/usr/bin/env python3
"""
Generate an Iceberg V2 table for multi-dimensional aggregate analytics.

Scenario: Retail chain sales — 120 transactions across 4 stores, 3 regions,
and 5 product categories. Queries exercise GROUPING SETS, ROLLUP, CUBE,
FILTER clause, DISTINCT aggregates, and HAVING with complex predicates.

Output: retail_sales/ directory with Iceberg V2 metadata chain.
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
TABLE_NAME = "retail_sales"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_v2_aggregate_warehouse"

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
    .appName("IcebergV2AggregateAnalyticsGenerator")
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
random.seed(2024)

STORES = {
    "Downtown Flagship": "East",
    "Midtown Express":   "East",
    "Westside Mall":     "West",
    "Lakefront Center":  "Central",
}

CATEGORIES = ["Electronics", "Clothing", "Home & Garden", "Sports", "Grocery"]

PRODUCTS = {
    "Electronics":   ["Wireless Earbuds", "Tablet 10-inch", "Smart Watch", "USB Charger", "Bluetooth Speaker"],
    "Clothing":      ["Winter Jacket", "Running Shoes", "Cotton T-Shirt", "Denim Jeans", "Wool Scarf"],
    "Home & Garden": ["LED Desk Lamp", "Garden Hose", "Throw Pillow", "Cutting Board", "Plant Pot"],
    "Sports":        ["Yoga Mat", "Resistance Bands", "Water Bottle", "Tennis Balls", "Jump Rope"],
    "Grocery":       ["Organic Coffee", "Olive Oil", "Granola Bars", "Sparkling Water", "Dark Chocolate"],
}

SALESPERSONS = [
    "Alice Morgan", "Bob Fischer", "Carol Reeves", "Daniel Ortiz",
    "Emily Watson", "Frank Dubois", "Grace Nakamura", "Henry Kowalski",
]

PRICE_RANGES = {
    "Wireless Earbuds": (29.99, 79.99),  "Tablet 10-inch": (199.99, 349.99),
    "Smart Watch": (149.99, 299.99),      "USB Charger": (9.99, 24.99),
    "Bluetooth Speaker": (39.99, 99.99),  "Winter Jacket": (89.99, 199.99),
    "Running Shoes": (59.99, 149.99),     "Cotton T-Shirt": (14.99, 39.99),
    "Denim Jeans": (39.99, 89.99),        "Wool Scarf": (19.99, 49.99),
    "LED Desk Lamp": (24.99, 69.99),      "Garden Hose": (19.99, 44.99),
    "Throw Pillow": (14.99, 34.99),       "Cutting Board": (12.99, 29.99),
    "Plant Pot": (7.99, 19.99),           "Yoga Mat": (19.99, 49.99),
    "Resistance Bands": (9.99, 29.99),    "Water Bottle": (12.99, 29.99),
    "Tennis Balls": (4.99, 14.99),        "Jump Rope": (7.99, 19.99),
    "Organic Coffee": (9.99, 18.99),      "Olive Oil": (7.99, 14.99),
    "Granola Bars": (3.99, 8.99),         "Sparkling Water": (1.99, 5.99),
    "Dark Chocolate": (4.99, 12.99),
}

rows = []
sale_id = 1
stores_list = list(STORES.keys())
sale_months = ["2025-01", "2025-02", "2025-03"]

for _ in range(120):
    store = random.choice(stores_list)
    region = STORES[store]
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    price_low, price_high = PRICE_RANGES[product]
    unit_price = round(random.uniform(price_low, price_high), 2)
    quantity = random.randint(1, 8)
    discount_pct = random.choice([0.0, 0.0, 0.0, 5.0, 10.0, 15.0, 20.0])
    salesperson = random.choice(SALESPERSONS)
    month = random.choice(sale_months)
    day = random.randint(1, 28)
    sale_date = f"{month}-{day:02d}"
    is_return = 1 if random.random() < 0.08 else 0  # ~8% returns

    rows.append((
        sale_id,
        store,
        region,
        category,
        product,
        quantity,
        unit_price,
        discount_pct,
        sale_date,
        salesperson,
        is_return,
    ))
    sale_id += 1

schema = StructType([
    StructField("sale_id", IntegerType(), False),
    StructField("store_name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("category", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("discount_pct", DoubleType(), False),
    StructField("sale_date", StringType(), False),
    StructField("salesperson", StringType(), False),
    StructField("is_return", IntegerType(), False),
])

df = spark.createDataFrame(rows, schema)

# ── Create Iceberg V2 table ──────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.retail")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.retail.{TABLE_NAME} (
        sale_id INT NOT NULL,
        store_name STRING NOT NULL,
        region STRING NOT NULL,
        category STRING NOT NULL,
        product_name STRING NOT NULL,
        quantity INT NOT NULL,
        unit_price DOUBLE NOT NULL,
        discount_pct DOUBLE NOT NULL,
        sale_date STRING NOT NULL,
        salesperson STRING NOT NULL,
        is_return INT NOT NULL
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '2',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
    )
""")

df.coalesce(1).writeTo(f"local.retail.{TABLE_NAME}").append()

count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.retail.{TABLE_NAME}").collect()[0].cnt
print(f"Loaded {count} rows into Iceberg V2 table")

# ── Compute ALL proof values ─────────────────────────────────────────
print("\n=== Proof Values ===")
print(f"Total rows: {count}")

# Grand totals
totals = spark.sql(f"""
    SELECT
        COUNT(*) as total_rows,
        ROUND(SUM(quantity * unit_price), 2) as gross_revenue,
        ROUND(SUM(quantity * unit_price * (1 - discount_pct/100)), 2) as net_revenue,
        SUM(quantity) as total_units,
        COUNT(DISTINCT store_name) as store_count,
        COUNT(DISTINCT region) as region_count,
        COUNT(DISTINCT category) as category_count,
        COUNT(DISTINCT product_name) as product_count,
        COUNT(DISTINCT salesperson) as salesperson_count,
        SUM(is_return) as return_count,
        ROUND(AVG(discount_pct), 2) as avg_discount
    FROM local.retail.{TABLE_NAME}
""").collect()[0]
print(f"\nGrand totals:")
for field in totals.__fields__:
    print(f"  {field}: {getattr(totals, field)}")

# Per-region
region_stats = spark.sql(f"""
    SELECT region, COUNT(*) as cnt,
           ROUND(SUM(quantity * unit_price), 2) as gross,
           ROUND(SUM(quantity * unit_price * (1 - discount_pct/100)), 2) as net
    FROM local.retail.{TABLE_NAME}
    GROUP BY region ORDER BY region
""").collect()
print(f"\nPer-region:")
for r in region_stats:
    print(f"  {r.region}: count={r.cnt}, gross={r.gross}, net={r.net}")

# Per-category
cat_stats = spark.sql(f"""
    SELECT category, COUNT(*) as cnt,
           ROUND(SUM(quantity * unit_price), 2) as gross,
           SUM(quantity) as units
    FROM local.retail.{TABLE_NAME}
    GROUP BY category ORDER BY category
""").collect()
print(f"\nPer-category:")
for r in cat_stats:
    print(f"  {r.category}: count={r.cnt}, gross={r.gross}, units={r.units}")

# Per-store
store_stats = spark.sql(f"""
    SELECT store_name, COUNT(*) as cnt,
           ROUND(SUM(quantity * unit_price), 2) as gross
    FROM local.retail.{TABLE_NAME}
    GROUP BY store_name ORDER BY store_name
""").collect()
print(f"\nPer-store:")
for r in store_stats:
    print(f"  {r.store_name}: count={r.cnt}, gross={r.gross}")

# GROUPING SETS (region, category)
gs_stats = spark.sql(f"""
    SELECT region, category,
           COUNT(*) as cnt,
           ROUND(SUM(quantity * unit_price), 2) as gross
    FROM local.retail.{TABLE_NAME}
    GROUP BY GROUPING SETS ((region, category), (region), (category), ())
    ORDER BY region NULLS LAST, category NULLS LAST
""").collect()
print(f"\nGROUPING SETS (region, category) — {len(gs_stats)} rows:")
for r in gs_stats:
    print(f"  region={r.region}, category={r.category}, cnt={r.cnt}, gross={r.gross}")

# ROLLUP (region, store_name)
rollup_stats = spark.sql(f"""
    SELECT region, store_name,
           COUNT(*) as cnt,
           ROUND(SUM(quantity * unit_price), 2) as gross
    FROM local.retail.{TABLE_NAME}
    GROUP BY ROLLUP (region, store_name)
    ORDER BY region NULLS LAST, store_name NULLS LAST
""").collect()
print(f"\nROLLUP (region, store_name) — {len(rollup_stats)} rows:")
for r in rollup_stats:
    print(f"  region={r.region}, store={r.store_name}, cnt={r.cnt}, gross={r.gross}")

# FILTER clause
filter_stats = spark.sql(f"""
    SELECT
        COUNT(*) as total_sales,
        COUNT(*) FILTER (WHERE is_return = 1) as returns,
        COUNT(*) FILTER (WHERE discount_pct > 0) as discounted_sales,
        ROUND(SUM(quantity * unit_price) FILTER (WHERE is_return = 0), 2) as non_return_gross,
        COUNT(DISTINCT salesperson) FILTER (WHERE region = 'East') as east_salespeople
    FROM local.retail.{TABLE_NAME}
""").collect()[0]
print(f"\nFILTER clause results:")
for field in filter_stats.__fields__:
    print(f"  {field}: {getattr(filter_stats, field)}")

# DISTINCT aggregates
distinct_agg = spark.sql(f"""
    SELECT
        region,
        COUNT(DISTINCT category) as distinct_categories,
        COUNT(DISTINCT product_name) as distinct_products,
        COUNT(DISTINCT salesperson) as distinct_salespeople
    FROM local.retail.{TABLE_NAME}
    GROUP BY region ORDER BY region
""").collect()
print(f"\nDISTINCT aggregates per region:")
for r in distinct_agg:
    print(f"  {r.region}: categories={r.distinct_categories}, "
          f"products={r.distinct_products}, salespeople={r.distinct_salespeople}")

# HAVING clause
having_stats = spark.sql(f"""
    SELECT category, COUNT(*) as cnt,
           ROUND(SUM(quantity * unit_price), 2) as gross
    FROM local.retail.{TABLE_NAME}
    GROUP BY category
    HAVING SUM(quantity * unit_price) > 3000
    ORDER BY gross DESC
""").collect()
print(f"\nCategories with gross > 3000:")
for r in having_stats:
    print(f"  {r.category}: cnt={r.cnt}, gross={r.gross}")

# Per-salesperson
sp_stats = spark.sql(f"""
    SELECT salesperson, COUNT(*) as cnt,
           ROUND(SUM(quantity * unit_price), 2) as gross
    FROM local.retail.{TABLE_NAME}
    GROUP BY salesperson ORDER BY salesperson
""").collect()
print(f"\nPer-salesperson:")
for r in sp_stats:
    print(f"  {r.salesperson}: count={r.cnt}, gross={r.gross}")

# ── Copy table to demo directory ──────────────────────────────────────
table_loc = f"{WAREHOUSE}/retail/{TABLE_NAME}"
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
