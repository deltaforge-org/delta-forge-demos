#!/usr/bin/env python3
"""
Generate Iceberg V2 table with multiple snapshots for consistency testing.

Scenario: Retail Store Inventory Ledger — 80 products across 4 categories
undergo 4 snapshots:
  1. Initial stock load (80 products, 20 per category)
  2. Restocking — INSERT 20 new products (5 per category) → 100 total
  3. Price corrections — UPDATE Electronics prices +8% (25 products affected)
  4. Discontinued — DELETE 10 specific SKUs → 90 total

Output: inventory/ directory with Iceberg V2 metadata, data files, and
position delete files — ready for Delta Forge to read.
"""
import os
import sys
import shutil
import json
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
TABLE_NAME = "inventory"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_snapshot_consistency_warehouse"

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
    .appName("IcebergSnapshotConsistency")
    .config("spark.jars", ICEBERG_JAR)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    .config("spark.sql.defaultCatalog", "local")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

random.seed(42)

# ── Data definitions ─────────────────────────────────────────────────

CATEGORIES = ['Clothing', 'Electronics', 'Home & Garden', 'Sports']

PRODUCTS = {
    'Electronics': [
        ('SKU-E001', 'Wireless Mouse', 29.99, 'TechCorp'),
        ('SKU-E002', 'USB-C Hub', 49.99, 'TechCorp'),
        ('SKU-E003', 'Bluetooth Speaker', 79.99, 'SoundWave'),
        ('SKU-E004', 'Webcam HD', 59.99, 'TechCorp'),
        ('SKU-E005', 'Mechanical Keyboard', 129.99, 'KeyMaster'),
        ('SKU-E006', 'Monitor Stand', 39.99, 'DeskPro'),
        ('SKU-E007', 'HDMI Cable 6ft', 12.99, 'CableCo'),
        ('SKU-E008', 'Power Strip', 24.99, 'PowerPlus'),
        ('SKU-E009', 'Wireless Charger', 34.99, 'ChargeTech'),
        ('SKU-E010', 'USB Flash Drive 64GB', 14.99, 'DataStore'),
        ('SKU-E011', 'Noise Cancelling Headphones', 199.99, 'SoundWave'),
        ('SKU-E012', 'Laptop Sleeve 15in', 22.99, 'TechCorp'),
        ('SKU-E013', 'Screen Protector', 9.99, 'ShieldMax'),
        ('SKU-E014', 'Ethernet Adapter', 19.99, 'CableCo'),
        ('SKU-E015', 'Smart Plug', 17.99, 'HomeSmart'),
        ('SKU-E016', 'Portable SSD 1TB', 89.99, 'DataStore'),
        ('SKU-E017', 'Desk Lamp LED', 44.99, 'LightWorks'),
        ('SKU-E018', 'Surge Protector', 29.99, 'PowerPlus'),
        ('SKU-E019', 'Wireless Earbuds', 69.99, 'SoundWave'),
        ('SKU-E020', 'Phone Stand', 15.99, 'DeskPro'),
    ],
    'Home & Garden': [
        ('SKU-H001', 'Garden Hose 50ft', 34.99, 'GreenThumb'),
        ('SKU-H002', 'Plant Pot Ceramic', 18.99, 'GreenThumb'),
        ('SKU-H003', 'LED String Lights', 22.99, 'LightWorks'),
        ('SKU-H004', 'Throw Pillow', 16.99, 'CozyHome'),
        ('SKU-H005', 'Kitchen Scale', 24.99, 'ChefPro'),
        ('SKU-H006', 'Cutting Board Bamboo', 19.99, 'ChefPro'),
        ('SKU-H007', 'Wall Clock', 27.99, 'TimePiece'),
        ('SKU-H008', 'Candle Set', 14.99, 'CozyHome'),
        ('SKU-H009', 'Door Mat', 21.99, 'HomeEssentials'),
        ('SKU-H010', 'Shower Curtain', 29.99, 'HomeEssentials'),
        ('SKU-H011', 'Picture Frame Set', 32.99, 'CozyHome'),
        ('SKU-H012', 'Herb Garden Kit', 26.99, 'GreenThumb'),
        ('SKU-H013', 'Dish Rack', 23.99, 'ChefPro'),
        ('SKU-H014', 'Bath Towel Set', 39.99, 'HomeEssentials'),
        ('SKU-H015', 'Storage Bins', 17.99, 'HomeEssentials'),
        ('SKU-H016', 'Watering Can', 15.99, 'GreenThumb'),
        ('SKU-H017', 'Spice Rack', 28.99, 'ChefPro'),
        ('SKU-H018', 'Welcome Sign', 12.99, 'CozyHome'),
        ('SKU-H019', 'Gardening Gloves', 11.99, 'GreenThumb'),
        ('SKU-H020', 'Soap Dispenser', 13.99, 'HomeEssentials'),
    ],
    'Sports': [
        ('SKU-S001', 'Yoga Mat', 29.99, 'FitGear'),
        ('SKU-S002', 'Resistance Bands Set', 19.99, 'FitGear'),
        ('SKU-S003', 'Water Bottle 32oz', 14.99, 'HydroFlow'),
        ('SKU-S004', 'Jump Rope', 12.99, 'FitGear'),
        ('SKU-S005', 'Foam Roller', 24.99, 'FitGear'),
        ('SKU-S006', 'Tennis Balls 3pk', 7.99, 'SportsMaster'),
        ('SKU-S007', 'Gym Bag', 34.99, 'FitGear'),
        ('SKU-S008', 'Sweatbands 2pk', 8.99, 'SportsMaster'),
        ('SKU-S009', 'Ab Wheel', 19.99, 'FitGear'),
        ('SKU-S010', 'Cooling Towel', 11.99, 'HydroFlow'),
        ('SKU-S011', 'Dumbbell 10lb', 22.99, 'IronWorks'),
        ('SKU-S012', 'Kettlebell 15lb', 34.99, 'IronWorks'),
        ('SKU-S013', 'Bike Light Set', 16.99, 'CycleMax'),
        ('SKU-S014', 'Swim Goggles', 13.99, 'AquaPro'),
        ('SKU-S015', 'Running Belt', 18.99, 'FitGear'),
        ('SKU-S016', 'Knee Brace', 21.99, 'SportsMaster'),
        ('SKU-S017', 'Basketball', 27.99, 'SportsMaster'),
        ('SKU-S018', 'Soccer Ball', 24.99, 'SportsMaster'),
        ('SKU-S019', 'Skipping Rope Pro', 15.99, 'FitGear'),
        ('SKU-S020', 'Grip Strengthener', 9.99, 'IronWorks'),
    ],
    'Clothing': [
        ('SKU-C001', 'Cotton T-Shirt', 19.99, 'ThreadCo'),
        ('SKU-C002', 'Denim Jeans', 49.99, 'ThreadCo'),
        ('SKU-C003', 'Running Shoes', 89.99, 'StridePro'),
        ('SKU-C004', 'Baseball Cap', 14.99, 'CapWorks'),
        ('SKU-C005', 'Wool Socks 3pk', 16.99, 'ThreadCo'),
        ('SKU-C006', 'Rain Jacket', 59.99, 'WeatherGuard'),
        ('SKU-C007', 'Leather Belt', 29.99, 'ThreadCo'),
        ('SKU-C008', 'Hiking Boots', 119.99, 'TrailBlazer'),
        ('SKU-C009', 'Scarf Knit', 18.99, 'ThreadCo'),
        ('SKU-C010', 'Sunglasses', 24.99, 'ShadeMax'),
        ('SKU-C011', 'Polo Shirt', 34.99, 'ThreadCo'),
        ('SKU-C012', 'Cargo Shorts', 29.99, 'ThreadCo'),
        ('SKU-C013', 'Fleece Hoodie', 44.99, 'ThreadCo'),
        ('SKU-C014', 'Sandals', 22.99, 'StridePro'),
        ('SKU-C015', 'Beanie Hat', 12.99, 'CapWorks'),
        ('SKU-C016', 'Windbreaker', 39.99, 'WeatherGuard'),
        ('SKU-C017', 'Tank Top', 14.99, 'ThreadCo'),
        ('SKU-C018', 'Swim Trunks', 24.99, 'ThreadCo'),
        ('SKU-C019', 'Dress Shirt', 39.99, 'ThreadCo'),
        ('SKU-C020', 'Winter Gloves', 17.99, 'WeatherGuard'),
    ],
}

# New products for Snapshot 2 (restocking)
NEW_PRODUCTS = {
    'Electronics': [
        ('SKU-E-N01', 'USB-C Cable 3ft', 8.99, 'CableCo'),
        ('SKU-E-N02', 'Webcam Ring Light', 19.99, 'LightWorks'),
        ('SKU-E-N03', 'Laptop Cooler', 32.99, 'TechCorp'),
        ('SKU-E-N04', 'Bluetooth Adapter', 11.99, 'TechCorp'),
        ('SKU-E-N05', 'Smart Light Bulb', 14.99, 'HomeSmart'),
    ],
    'Home & Garden': [
        ('SKU-H-N01', 'Succulent Planter', 21.99, 'GreenThumb'),
        ('SKU-H-N02', 'Kitchen Timer', 9.99, 'ChefPro'),
        ('SKU-H-N03', 'Towel Hooks Set', 13.99, 'HomeEssentials'),
        ('SKU-H-N04', 'Garden Kneeler', 27.99, 'GreenThumb'),
        ('SKU-H-N05', 'Compost Bin', 35.99, 'GreenThumb'),
    ],
    'Sports': [
        ('SKU-S-N01', 'Massage Gun Mini', 49.99, 'FitGear'),
        ('SKU-S-N02', 'Yoga Block', 12.99, 'FitGear'),
        ('SKU-S-N03', 'Speed Ladder', 22.99, 'SportsMaster'),
        ('SKU-S-N04', 'Wrist Wraps', 10.99, 'IronWorks'),
        ('SKU-S-N05', 'Agility Cones Set', 14.99, 'SportsMaster'),
    ],
    'Clothing': [
        ('SKU-C-N01', 'Compression Shorts', 24.99, 'StridePro'),
        ('SKU-C-N02', 'Bucket Hat', 16.99, 'CapWorks'),
        ('SKU-C-N03', 'Quarter Zip Pullover', 44.99, 'ThreadCo'),
        ('SKU-C-N04', 'Ankle Socks 6pk', 12.99, 'ThreadCo'),
        ('SKU-C-N05', 'Sports Bra', 29.99, 'StridePro'),
    ],
}

# 10 SKUs to discontinue in Snapshot 4
DISCONTINUED_SKUS = [
    'SKU-E007',  # HDMI Cable 6ft (Electronics)
    'SKU-E013',  # Screen Protector (Electronics)
    'SKU-H008',  # Candle Set (Home & Garden)
    'SKU-H018',  # Welcome Sign (Home & Garden)
    'SKU-H019',  # Gardening Gloves (Home & Garden)
    'SKU-S006',  # Tennis Balls 3pk (Sports)
    'SKU-S008',  # Sweatbands 2pk (Sports)
    'SKU-C004',  # Baseball Cap (Clothing)
    'SKU-C017',  # Tank Top (Clothing)
    'SKU-C018',  # Swim Trunks (Clothing)
]

# ── Step 1: Build initial rows ───────────────────────────────────────

base_date = date(2025, 1, 15)

def make_rows(products_dict, date_base):
    """Generate rows from a product dictionary."""
    rows = []
    for category in sorted(products_dict.keys()):
        for sku, name, price, supplier in products_dict[category]:
            qty = random.randint(10, 200)
            restock_date = date_base + timedelta(days=random.randint(0, 30))
            rows.append((
                sku,
                name,
                category,
                round(price, 2),
                qty,
                supplier,
                restock_date.isoformat(),
            ))
    return rows

initial_rows = make_rows(PRODUCTS, base_date)
print(f"Generated {len(initial_rows)} initial products")

schema = StructType([
    StructField("sku", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("quantity_on_hand", IntegerType(), False),
    StructField("supplier", StringType(), False),
    StructField("last_restocked", StringType(), False),
])

# ── Step 2: Create Iceberg V2 table and load data ────────────────────

spark.sql("CREATE NAMESPACE IF NOT EXISTS local.default")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.default.inventory (
    sku STRING,
    product_name STRING,
    category STRING,
    unit_price DOUBLE,
    quantity_on_hand INT,
    supplier STRING,
    last_restocked DATE
) USING iceberg
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
)
""")

# ── Snapshot 1: Initial stock load (80 products) ─────────────────────

df_initial = spark.createDataFrame(initial_rows, schema)
df_initial = df_initial.withColumn("last_restocked", F.col("last_restocked").cast("date"))
df_initial.coalesce(1).writeTo("local.default.inventory").append()

count1 = spark.sql("SELECT COUNT(*) AS cnt FROM local.default.inventory").collect()[0].cnt
print(f"Snapshot 1 (initial load): {count1} rows")

# ── Snapshot 2: Restocking — INSERT 20 new products ──────────────────

restock_date = date(2025, 3, 1)
new_rows = make_rows(NEW_PRODUCTS, restock_date)
print(f"Inserting {len(new_rows)} new products")

df_new = spark.createDataFrame(new_rows, schema)
df_new = df_new.withColumn("last_restocked", F.col("last_restocked").cast("date"))
df_new.coalesce(1).writeTo("local.default.inventory").append()

count2 = spark.sql("SELECT COUNT(*) AS cnt FROM local.default.inventory").collect()[0].cnt
print(f"Snapshot 2 (after restock): {count2} rows")

# ── Snapshot 3: Price corrections — UPDATE Electronics +8% ──────────

spark.sql("""
UPDATE local.default.inventory
SET unit_price = ROUND(unit_price * 1.08, 2)
WHERE category = 'Electronics'
""")
print("Snapshot 3: Electronics prices updated +8%")

count3 = spark.sql("SELECT COUNT(*) AS cnt FROM local.default.inventory").collect()[0].cnt
print(f"Snapshot 3 (after price update): {count3} rows")

# Verify Electronics price change
electronics_avg = spark.sql("""
    SELECT ROUND(AVG(unit_price), 2) AS avg_price
    FROM local.default.inventory
    WHERE category = 'Electronics'
""").collect()[0].avg_price
print(f"  Electronics avg price after +8%: {electronics_avg}")

# ── Snapshot 4: Discontinue 10 products ──────────────────────────────

sku_list = ", ".join(f"'{s}'" for s in DISCONTINUED_SKUS)
spark.sql(f"""
DELETE FROM local.default.inventory
WHERE sku IN ({sku_list})
""")
print(f"Snapshot 4: Deleted {len(DISCONTINUED_SKUS)} discontinued SKUs")

count4 = spark.sql("SELECT COUNT(*) AS cnt FROM local.default.inventory").collect()[0].cnt
print(f"Snapshot 4 (after discontinuation): {count4} rows")

# ── Verify final state ───────────────────────────────────────────────

print("\n=== Final State Verification ===")
print(f"Total rows: {count4}")

category_counts = spark.sql("""
    SELECT category, COUNT(*) AS cnt, SUM(quantity_on_hand) AS total_qty,
           ROUND(SUM(unit_price * quantity_on_hand), 2) AS inventory_value,
           ROUND(AVG(unit_price), 2) AS avg_price
    FROM local.default.inventory
    GROUP BY category
    ORDER BY category
""").collect()

for row in category_counts:
    print(f"  {row.category}: {row.cnt} products, qty={row.total_qty}, "
          f"value={row.inventory_value}, avg_price={row.avg_price}")

# Check new products present
new_count = spark.sql("""
    SELECT COUNT(*) AS cnt FROM local.default.inventory
    WHERE sku LIKE 'SKU-%-N%'
""").collect()[0].cnt
print(f"\nNew products (SKU-%-N%): {new_count}")

# Check discontinued absent
disc_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt FROM local.default.inventory
    WHERE sku IN ({sku_list})
""").collect()[0].cnt
print(f"Discontinued SKUs remaining: {disc_count}")

# Show snapshot history
print("\n=== Snapshot History ===")
history_df = spark.sql("SELECT * FROM local.default.inventory.history")
history_df.show(truncate=False)
print("History columns:", history_df.columns)

# ── Copy table to demo directory ─────────────────────────────────────

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
