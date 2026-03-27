#!/usr/bin/env python3
"""
Generate an Iceberg V2 table demonstrating schema evolution using PySpark.

Scenario: Employee directory — 360 total employees across 5 departments
(Engineering, Sales, Marketing, Finance, HR). The table undergoes schema
evolution across 4 snapshots:

  Snapshot 1: 300 employees with initial schema (emp_id, full_name, dept, salary, hire_date)
  Snapshot 2: ADD COLUMN title + INSERT 60 more employees with titles
  Snapshot 3: ADD COLUMN location + UPDATE some employees with locations
  Snapshot 4: RENAME COLUMN dept TO department

This exercises Iceberg's field-id stability for column renames and
NULL handling for columns added after initial data load.

Output: employee_directory/ directory with Iceberg V2 metadata showing
schema evolution history — ready for Delta Forge to read.
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
TABLE_NAME = "employee_directory"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_schema_evolution_warehouse"

# Clean previous runs
for d in [TABLE_OUTPUT, WAREHOUSE]:
    if os.path.exists(d):
        shutil.rmtree(d)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

spark = (
    SparkSession.builder
    .master("local[1]")
    .config("spark.sql.shuffle.partitions", "1")
    .appName("IcebergSchemaEvolutionGenerator")
    .config("spark.jars", ICEBERG_JAR)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", WAREHOUSE)
    .config("spark.sql.defaultCatalog", "local")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Step 1: Generate initial 300 employees ────────────────────────────
random.seed(2025)

DEPARTMENTS = ["Engineering", "Sales", "Marketing", "Finance", "HR"]
FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Hank",
    "Iris", "Jack", "Karen", "Leo", "Mona", "Nick", "Olivia", "Paul",
    "Quinn", "Rita", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zach", "Abby", "Brian", "Clara", "Derek"
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
    "Jackson", "White", "Harris", "Martin", "Thompson", "Moore", "Allen"
]
TITLES = [
    "Software Engineer", "Senior Engineer", "Staff Engineer", "Tech Lead",
    "Product Manager", "Senior PM", "Sales Rep", "Account Executive",
    "Marketing Analyst", "Content Strategist", "Financial Analyst",
    "Controller", "HR Specialist", "Recruiter", "Data Scientist"
]
LOCATIONS = [
    "New York", "San Francisco", "Chicago", "Austin", "Seattle",
    "Boston", "Denver", "Atlanta", "Portland", "Miami"
]

base_date = date(2018, 1, 1)

# Initial 300 employees: 60 per department
initial_rows = []
for i in range(300):
    dept_idx = i // 60  # 0-4, 60 per dept
    dept = DEPARTMENTS[dept_idx]
    first = FIRST_NAMES[i % len(FIRST_NAMES)]
    last = LAST_NAMES[i % len(LAST_NAMES)]
    full_name = f"{first} {last}"
    salary = round(50000 + random.random() * 100000, 2)
    hire_date = (base_date + timedelta(days=random.randint(0, 2000))).isoformat()

    initial_rows.append((i + 1, full_name, dept, salary, hire_date))

schema_initial = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("full_name", StringType(), False),
    StructField("dept", StringType(), False),
    StructField("salary", DoubleType(), False),
    StructField("hire_date", StringType(), False),
])

df_initial = spark.createDataFrame(initial_rows, schema_initial)
print(f"Generated {df_initial.count()} initial employees")

# ── Step 2: Create Iceberg V2 table and load initial data ─────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.hr")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.hr.{TABLE_NAME} (
        emp_id INT NOT NULL,
        full_name STRING NOT NULL,
        dept STRING NOT NULL,
        salary DOUBLE NOT NULL,
        hire_date STRING NOT NULL
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '2'
    )
""")

df_initial.coalesce(1).writeTo(f"local.hr.{TABLE_NAME}").append()
print(f"Snapshot 1: Loaded 300 employees into Iceberg V2 table")

count1 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.hr.{TABLE_NAME}").collect()[0].cnt
print(f"  Row count after snapshot 1: {count1}")

# ── Step 3: ADD COLUMN title + INSERT 60 more employees ───────────────
spark.sql(f"ALTER TABLE local.hr.{TABLE_NAME} ADD COLUMN title STRING")
print("Snapshot 2 prep: Added 'title' column")

# Generate 60 new employees with titles (emp_id 301-360)
new_rows = []
for i in range(60):
    dept_idx = i // 12  # 12 per dept
    dept = DEPARTMENTS[dept_idx]
    first = FIRST_NAMES[(i + 5) % len(FIRST_NAMES)]
    last = LAST_NAMES[(i + 7) % len(LAST_NAMES)]
    full_name = f"{first} {last}"
    salary = round(60000 + random.random() * 90000, 2)
    hire_date = (date(2024, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat()
    title = TITLES[i % len(TITLES)]

    new_rows.append((301 + i, full_name, dept, salary, hire_date, title))

schema_with_title = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("full_name", StringType(), False),
    StructField("dept", StringType(), False),
    StructField("salary", DoubleType(), False),
    StructField("hire_date", StringType(), False),
    StructField("title", StringType(), True),
])

df_new = spark.createDataFrame(new_rows, schema_with_title)
df_new.coalesce(1).writeTo(f"local.hr.{TABLE_NAME}").append()
print(f"Snapshot 2: Inserted 60 employees with titles")

count2 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.hr.{TABLE_NAME}").collect()[0].cnt
print(f"  Row count after snapshot 2: {count2}")

# ── Step 4: ADD COLUMN location + UPDATE some employees ───────────────
spark.sql(f"ALTER TABLE local.hr.{TABLE_NAME} ADD COLUMN location STRING")
print("Snapshot 3 prep: Added 'location' column")

# Update the 60 new employees (301-360) with locations
spark.sql(f"""
    UPDATE local.hr.{TABLE_NAME}
    SET location = CASE
        WHEN MOD(emp_id, 10) = 0 THEN 'New York'
        WHEN MOD(emp_id, 10) = 1 THEN 'San Francisco'
        WHEN MOD(emp_id, 10) = 2 THEN 'Chicago'
        WHEN MOD(emp_id, 10) = 3 THEN 'Austin'
        WHEN MOD(emp_id, 10) = 4 THEN 'Seattle'
        WHEN MOD(emp_id, 10) = 5 THEN 'Boston'
        WHEN MOD(emp_id, 10) = 6 THEN 'Denver'
        WHEN MOD(emp_id, 10) = 7 THEN 'Atlanta'
        WHEN MOD(emp_id, 10) = 8 THEN 'Portland'
        WHEN MOD(emp_id, 10) = 9 THEN 'Miami'
    END
    WHERE emp_id >= 301
""")
print(f"Snapshot 3: Updated employees 301-360 with locations")

count3 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.hr.{TABLE_NAME}").collect()[0].cnt
loc_not_null = spark.sql(f"SELECT COUNT(*) as cnt FROM local.hr.{TABLE_NAME} WHERE location IS NOT NULL").collect()[0].cnt
print(f"  Row count after snapshot 3: {count3}")
print(f"  Rows with location NOT NULL: {loc_not_null}")

# ── Step 5: RENAME COLUMN dept TO department ──────────────────────────
spark.sql(f"ALTER TABLE local.hr.{TABLE_NAME} RENAME COLUMN dept TO department")
print("Snapshot 4: Renamed 'dept' to 'department'")

count4 = spark.sql(f"SELECT COUNT(*) as cnt FROM local.hr.{TABLE_NAME}").collect()[0].cnt
print(f"  Row count after snapshot 4: {count4}")

# ── Step 6: Compute proof values ─────────────────────────────────────
print("\n=== Proof Values ===")

proofs = spark.sql(f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT department) AS dept_count,
        SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS null_title_count,
        SUM(CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END) AS has_title_count,
        SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS null_location_count,
        SUM(CASE WHEN location IS NOT NULL THEN 1 ELSE 0 END) AS has_location_count,
        ROUND(AVG(salary), 2) AS avg_salary,
        ROUND(MIN(salary), 2) AS min_salary,
        ROUND(MAX(salary), 2) AS max_salary,
        ROUND(SUM(salary), 2) AS total_salary
    FROM local.hr.{TABLE_NAME}
""").collect()[0]
for field in proofs.__fields__:
    print(f"  {field}: {getattr(proofs, field)}")

print("\n  Per-department counts:")
dept_counts = spark.sql(f"""
    SELECT department, COUNT(*) as cnt, ROUND(AVG(salary), 2) as avg_salary
    FROM local.hr.{TABLE_NAME}
    GROUP BY department ORDER BY department
""").collect()
for d in dept_counts:
    print(f"    {d.department}: count={d.cnt}, avg_salary={d.avg_salary}")

print("\n  Title distribution:")
title_dist = spark.sql(f"""
    SELECT
        SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS null_titles,
        SUM(CASE WHEN title IS NOT NULL THEN 1 ELSE 0 END) AS has_titles
    FROM local.hr.{TABLE_NAME}
""").collect()[0]
print(f"    NULL titles: {title_dist.null_titles}")
print(f"    Has titles: {title_dist.has_titles}")

print("\n  Location distribution:")
loc_dist = spark.sql(f"""
    SELECT
        SUM(CASE WHEN location IS NULL THEN 1 ELSE 0 END) AS null_locations,
        SUM(CASE WHEN location IS NOT NULL THEN 1 ELSE 0 END) AS has_locations
    FROM local.hr.{TABLE_NAME}
""").collect()[0]
print(f"    NULL locations: {loc_dist.null_locations}")
print(f"    Has locations: {loc_dist.has_locations}")

print("\n  Salary stats by department:")
salary_stats = spark.sql(f"""
    SELECT department,
           ROUND(SUM(salary), 2) as total_salary,
           ROUND(AVG(salary), 2) as avg_salary,
           COUNT(*) as cnt
    FROM local.hr.{TABLE_NAME}
    GROUP BY department ORDER BY department
""").collect()
for s in salary_stats:
    print(f"    {s.department}: total={s.total_salary}, avg={s.avg_salary}, count={s.cnt}")

# ── Step 7: Copy table to demo directory (without CRC files) ──────────
table_loc = f"{WAREHOUSE}/hr/{TABLE_NAME}"
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
