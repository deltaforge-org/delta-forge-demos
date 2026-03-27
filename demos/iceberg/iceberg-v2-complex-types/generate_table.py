#!/usr/bin/env python3
"""
Generate Iceberg V2 table with complex types (struct, array) using PySpark.

Scenario: E-Commerce Order Processing — 100 orders with nested product items
(array of structs) and shipping address (struct). Tests Iceberg's support for
complex/nested column types.

Output: orders/ directory with Iceberg V2 metadata and data files.
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
TABLE_NAME = "orders"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_complex_types_warehouse"

# Clean previous runs
for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType, DateType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergComplexTypes")
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

PRODUCTS = {
    'Laptop':      (899.99, 1299.99),
    'Keyboard':    (29.99, 149.99),
    'Mouse':       (9.99, 79.99),
    'Monitor':     (199.99, 599.99),
    'Headphones':  (19.99, 299.99),
    'Webcam':      (29.99, 129.99),
    'USB Hub':     (12.99, 49.99),
    'Desk Lamp':   (15.99, 89.99),
    'Notebook':    (4.99, 19.99),
    'Pen Set':     (5.99, 24.99),
}

CITIES = [
    ('123 Main St',    'New York',      'NY', '10001'),
    ('456 Oak Ave',    'Los Angeles',   'CA', '90001'),
    ('789 Pine Rd',    'Chicago',       'IL', '60601'),
    ('321 Elm Blvd',   'Houston',       'TX', '77001'),
    ('654 Maple Dr',   'Phoenix',       'AZ', '85001'),
    ('987 Cedar Ln',   'Philadelphia',  'PA', '19101'),
    ('147 Birch Way',  'San Antonio',   'TX', '78201'),
    ('258 Walnut Ct',  'San Diego',     'CA', '92101'),
    ('369 Spruce Pl',  'Dallas',        'TX', '75201'),
    ('741 Willow St',  'Seattle',       'WA', '98101'),
    ('852 Ash Ave',    'Denver',        'CO', '80201'),
    ('963 Cherry Rd',  'Boston',        'MA', '02101'),
    ('111 Poplar Blvd', 'Atlanta',      'GA', '30301'),
    ('222 Hazel Dr',   'Miami',         'FL', '33101'),
    ('333 Laurel Ln',  'Portland',      'OR', '97201'),
]

STATUSES = ['Shipped', 'Delivered', 'Processing', 'Cancelled']
STATUS_WEIGHTS = [30, 40, 20, 10]

CUSTOMER_FIRST = ['Alice', 'Bob', 'Carol', 'David', 'Eve', 'Frank', 'Grace',
                   'Henry', 'Iris', 'Jack', 'Karen', 'Leo', 'Mia', 'Noah', 'Olivia']
CUSTOMER_LAST = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
                  'Miller', 'Davis', 'Rodriguez', 'Martinez']

base_date = date(2025, 1, 1)

rows = []
for order_id in range(1, 101):
    # Customer
    first = random.choice(CUSTOMER_FIRST)
    last = random.choice(CUSTOMER_LAST)
    customer_name = f"{first} {last}"

    # Order date: random within 2025 Q1
    order_date = base_date + timedelta(days=random.randint(0, 89))

    # Shipping address (struct)
    street, city, state, zip_code = random.choice(CITIES)

    # Items (array of structs): 1-5 items per order
    num_items = random.randint(1, 5)
    items = []
    order_total = 0.0
    chosen_products = random.sample(list(PRODUCTS.keys()), min(num_items, len(PRODUCTS)))
    for product_name in chosen_products:
        lo, hi = PRODUCTS[product_name]
        unit_price = round(random.uniform(lo, hi), 2)
        quantity = random.randint(1, 3)
        items.append(Row(product_name=product_name, quantity=quantity, unit_price=unit_price))
        order_total += quantity * unit_price

    order_total = round(order_total, 2)

    # Status
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

    # Notes: NULL for ~30%
    if random.random() < 0.3:
        notes = None
    else:
        note_options = [
            'Rush delivery requested',
            'Gift wrapping needed',
            'Leave at front door',
            'Signature required',
            'Fragile items',
            'Corporate purchase order',
            'Holiday special',
            'Return customer discount applied',
        ]
        notes = random.choice(note_options)

    rows.append(Row(
        order_id=order_id,
        customer_name=customer_name,
        order_date=order_date,
        shipping_address=Row(street=street, city=city, state=state, zip_code=zip_code),
        items=items,
        order_total=order_total,
        status=status,
        notes=notes,
    ))

# Define schema explicitly
schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_name", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("shipping_address", StructType([
        StructField("street", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("zip_code", StringType(), False),
    ]), False),
    StructField("items", ArrayType(StructType([
        StructField("product_name", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
    ])), False),
    StructField("order_total", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("notes", StringType(), True),
])

df = spark.createDataFrame(rows, schema)

print(f"Generated {df.count()} rows")
print(f"Schema:")
df.printSchema()

# ── Step 2: Create Iceberg V2 table and load data ────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.default")

spark.sql("""
CREATE TABLE IF NOT EXISTS local.default.orders (
    order_id INT,
    customer_name STRING,
    order_date DATE,
    shipping_address STRUCT<street: STRING, city: STRING, state: STRING, zip_code: STRING>,
    items ARRAY<STRUCT<product_name: STRING, quantity: INT, unit_price: DOUBLE>>,
    order_total DOUBLE,
    status STRING,
    notes STRING
) USING iceberg
TBLPROPERTIES ('format-version' = '2')
""")

# Insert data — coalesce to 1 for a single data file
df.coalesce(1).writeTo("local.default.orders").append()
print(f"Loaded data into Iceberg table")

# Verify
count = spark.sql("SELECT COUNT(*) as cnt FROM local.default.orders").collect()[0].cnt
print(f"Row count: {count}")

# Show sample
print("\nSample rows:")
spark.sql("SELECT order_id, customer_name, order_date, shipping_address.city, size(items) as item_count, order_total, status FROM local.default.orders ORDER BY order_id LIMIT 5").show(truncate=False)

# ── Step 3: Copy table to demo directory ──────────────────────────────
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
