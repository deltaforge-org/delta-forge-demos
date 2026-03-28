#!/usr/bin/env python3
"""
Generate an Iceberg V3 table with intentional NULLs for edge-case testing.

Scenario: Hospital clinical lab — 50 lab results where some tests are
pending (NULL result_value), some automated (NULL lab_technician), some
non-standard (NULL reference ranges), and most have no notes (NULL notes).

This exercises Iceberg V3's NULL handling in statistics, column-level
min/max tracking, and predicate evaluation with missing data.

Output: lab_results/ directory with Iceberg V3 metadata chain.
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
TABLE_NAME = "lab_results"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_v3_null_warehouse"

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
    .appName("IcebergV3NullEdgeCasesGenerator")
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
random.seed(2025)

TESTS = {
    "Hemoglobin":    {"unit": "g/dL",    "ref_low": 12.0,  "ref_high": 17.5,  "range": (8.0, 20.0)},
    "Glucose":       {"unit": "mg/dL",   "ref_low": 70.0,  "ref_high": 100.0, "range": (50.0, 250.0)},
    "Cholesterol":   {"unit": "mg/dL",   "ref_low": 0.0,   "ref_high": 200.0, "range": (100.0, 350.0)},
    "Creatinine":    {"unit": "mg/dL",   "ref_low": 0.6,   "ref_high": 1.2,   "range": (0.3, 3.0)},
    "Platelet Count":{"unit": "K/uL",    "ref_low": 150.0, "ref_high": 400.0, "range": (50.0, 600.0)},
    "TSH":           {"unit": "mIU/L",   "ref_low": 0.4,   "ref_high": 4.0,   "range": (0.1, 10.0)},
    "ALT":           {"unit": "U/L",     "ref_low": 7.0,   "ref_high": 56.0,  "range": (5.0, 120.0)},
    "Vitamin D":     {"unit": "ng/mL",   "ref_low": 30.0,  "ref_high": 100.0, "range": (8.0, 150.0)},
    "CRP":           {"unit": "mg/L",    "ref_low": 0.0,   "ref_high": 3.0,   "range": (0.1, 50.0)},
    "Ferritin":      {"unit": "ng/mL",   "ref_low": 12.0,  "ref_high": 300.0, "range": (5.0, 500.0)},
}

PATIENTS = [
    "Alice Morgan", "Bob Fischer", "Carol Reeves", "Daniel Ortiz",
    "Emily Watson", "Frank Dubois", "Grace Nakamura", "Henry Kowalski",
    "Irene Svensson", "James Okafor", "Karen Petrova", "Leo Andersen",
    "Maria Gutierrez", "Nathan Brooks", "Olivia Henriksen",
]

TECHNICIANS = ["Dr. Smith", "Dr. Patel", "Dr. Kim", "Dr. Santos", "Dr. Berg"]

# NULL plan:
# - result_value: NULL for samples 5, 12, 23, 34, 45 (pending tests)
# - unit: NULL for samples 8, 37 (non-standard test)
# - reference_low/high: NULL for samples 8, 27, 37 (experimental tests)
# - is_critical: NULL for samples 3, 15, 28, 42 (not yet assessed)
# - lab_technician: NULL for samples 2, 10, 18, 25, 33, 41, 49 (automated)
# - notes: NULL for most — only samples 1, 5, 9, 12, 17, 20, 23, 30, 34,
#          38, 42, 45, 50 have notes (13 with notes, 37 without)

NULL_RESULT = {5, 12, 23, 34, 45}
NULL_UNIT = {8, 37}
NULL_REF = {8, 27, 37}
NULL_CRITICAL = {3, 15, 28, 42}
NULL_TECH = {2, 10, 18, 25, 33, 41, 49}
HAS_NOTES = {1, 5, 9, 12, 17, 20, 23, 30, 34, 38, 42, 45, 50}

NOTES_TEXT = {
    1:  "Routine annual checkup",
    5:  "Sample hemolyzed - retest pending",
    9:  "Fasting sample confirmed",
    12: "Patient declined redraw",
    17: "Repeat test ordered",
    20: "Post-medication follow-up",
    23: "Pending lab review",
    30: "Urgent flag reviewed by attending",
    34: "Equipment calibration in progress",
    38: "Second opinion requested",
    42: "Critical review pending",
    45: "Sample insufficient volume",
    50: "Discharge labs complete",
}

rows = []
test_names = list(TESTS.keys())
base_dates = [f"2025-03-{d:02d}" for d in range(1, 26)]

for sid in range(1, 51):
    patient = PATIENTS[(sid - 1) % len(PATIENTS)]
    test_name = test_names[(sid - 1) % len(test_names)]
    test_info = TESTS[test_name]

    # result_value
    if sid in NULL_RESULT:
        result_value = None
    else:
        result_value = round(random.uniform(*test_info["range"]), 2)

    # unit
    if sid in NULL_UNIT:
        unit = None
    else:
        unit = test_info["unit"]

    # reference ranges
    if sid in NULL_REF:
        ref_low = None
        ref_high = None
    else:
        ref_low = test_info["ref_low"]
        ref_high = test_info["ref_high"]

    # is_critical: 1 if result out of range, 0 if in range, NULL if not assessed
    if sid in NULL_CRITICAL:
        is_critical = None
    elif result_value is not None and ref_low is not None and ref_high is not None:
        is_critical = 1 if (result_value < ref_low or result_value > ref_high) else 0
    else:
        is_critical = None  # can't determine without result or reference

    # lab_technician
    if sid in NULL_TECH:
        technician = None
    else:
        technician = TECHNICIANS[(sid - 1) % len(TECHNICIANS)]

    # notes
    notes = NOTES_TEXT.get(sid, None)

    # collected_date
    collected_date = base_dates[(sid - 1) % len(base_dates)]

    rows.append((
        sid,
        patient,
        test_name,
        result_value,
        unit,
        ref_low,
        ref_high,
        is_critical,
        collected_date,
        technician,
        notes,
    ))

schema = StructType([
    StructField("sample_id", IntegerType(), False),
    StructField("patient_name", StringType(), False),
    StructField("test_name", StringType(), False),
    StructField("result_value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("reference_low", DoubleType(), True),
    StructField("reference_high", DoubleType(), True),
    StructField("is_critical", IntegerType(), True),
    StructField("collected_date", StringType(), False),
    StructField("lab_technician", StringType(), True),
    StructField("notes", StringType(), True),
])

df = spark.createDataFrame(rows, schema)

# ── Create Iceberg V3 table ──────────────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.clinical")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.clinical.{TABLE_NAME} (
        sample_id INT NOT NULL,
        patient_name STRING NOT NULL,
        test_name STRING NOT NULL,
        result_value DOUBLE,
        unit STRING,
        reference_low DOUBLE,
        reference_high DOUBLE,
        is_critical INT,
        collected_date STRING NOT NULL,
        lab_technician STRING,
        notes STRING
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '3',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read'
    )
""")

df.coalesce(1).writeTo(f"local.clinical.{TABLE_NAME}").append()

count = spark.sql(f"SELECT COUNT(*) as cnt FROM local.clinical.{TABLE_NAME}").collect()[0].cnt
print(f"Loaded {count} rows into Iceberg V3 table")

# ── Compute proof values ─────────────────────────────────────────────
print("\n=== Proof Values ===")
print(f"Total rows: {count}")

# NULL counts per column
null_counts = spark.sql(f"""
    SELECT
        SUM(CASE WHEN result_value IS NULL THEN 1 ELSE 0 END) as null_result,
        SUM(CASE WHEN unit IS NULL THEN 1 ELSE 0 END) as null_unit,
        SUM(CASE WHEN reference_low IS NULL THEN 1 ELSE 0 END) as null_ref_low,
        SUM(CASE WHEN reference_high IS NULL THEN 1 ELSE 0 END) as null_ref_high,
        SUM(CASE WHEN is_critical IS NULL THEN 1 ELSE 0 END) as null_critical,
        SUM(CASE WHEN lab_technician IS NULL THEN 1 ELSE 0 END) as null_technician,
        SUM(CASE WHEN notes IS NULL THEN 1 ELSE 0 END) as null_notes,
        COUNT(*) as total,
        COUNT(result_value) as non_null_result,
        COUNT(unit) as non_null_unit,
        COUNT(notes) as non_null_notes
    FROM local.clinical.{TABLE_NAME}
""").collect()[0]
print(f"\nNULL counts:")
for field in null_counts.__fields__:
    print(f"  {field}: {getattr(null_counts, field)}")

# Aggregates ignoring NULLs
agg_stats = spark.sql(f"""
    SELECT
        ROUND(AVG(result_value), 2) as avg_result,
        ROUND(MIN(result_value), 2) as min_result,
        ROUND(MAX(result_value), 2) as max_result,
        ROUND(SUM(result_value), 2) as sum_result,
        COUNT(DISTINCT test_name) as distinct_tests,
        COUNT(DISTINCT patient_name) as distinct_patients
    FROM local.clinical.{TABLE_NAME}
""").collect()[0]
print(f"\nAggregate stats (NULLs excluded by AVG/MIN/MAX/SUM):")
for field in agg_stats.__fields__:
    print(f"  {field}: {getattr(agg_stats, field)}")

# COALESCE proof
coalesce_result = spark.sql(f"""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN COALESCE(result_value, -1) = -1 THEN 1 ELSE 0 END) as coalesced_to_default,
        SUM(CASE WHEN COALESCE(lab_technician, 'Automated') = 'Automated' THEN 1 ELSE 0 END) as automated_count
    FROM local.clinical.{TABLE_NAME}
""").collect()[0]
print(f"\nCOALESCE proofs:")
for field in coalesce_result.__fields__:
    print(f"  {field}: {getattr(coalesce_result, field)}")

# Per-test stats
test_stats = spark.sql(f"""
    SELECT test_name,
           COUNT(*) as cnt,
           COUNT(result_value) as has_result,
           ROUND(AVG(result_value), 2) as avg_result
    FROM local.clinical.{TABLE_NAME}
    GROUP BY test_name ORDER BY test_name
""").collect()
print(f"\nPer-test stats:")
for r in test_stats:
    print(f"  {r.test_name}: count={r.cnt}, has_result={r.has_result}, avg={r.avg_result}")

# Critical counts
critical_stats = spark.sql(f"""
    SELECT
        SUM(CASE WHEN is_critical = 1 THEN 1 ELSE 0 END) as critical_count,
        SUM(CASE WHEN is_critical = 0 THEN 1 ELSE 0 END) as normal_count,
        SUM(CASE WHEN is_critical IS NULL THEN 1 ELSE 0 END) as unknown_count
    FROM local.clinical.{TABLE_NAME}
""").collect()[0]
print(f"\nCritical status:")
for field in critical_stats.__fields__:
    print(f"  {field}: {getattr(critical_stats, field)}")

# GROUP BY with NULLs (lab_technician)
tech_groups = spark.sql(f"""
    SELECT lab_technician, COUNT(*) as cnt
    FROM local.clinical.{TABLE_NAME}
    GROUP BY lab_technician
    ORDER BY lab_technician NULLS FIRST
""").collect()
print(f"\nGROUP BY lab_technician (including NULL group):")
for r in tech_groups:
    print(f"  {r.lab_technician}: {r.cnt}")

# IS DISTINCT FROM proof
distinct_from = spark.sql(f"""
    SELECT
        SUM(CASE WHEN result_value IS DISTINCT FROM reference_low THEN 1 ELSE 0 END) as diff_from_low,
        SUM(CASE WHEN NOT (result_value IS DISTINCT FROM NULL) THEN 1 ELSE 0 END) as result_is_null
    FROM local.clinical.{TABLE_NAME}
""").collect()[0]
print(f"\nIS DISTINCT FROM proofs:")
for field in distinct_from.__fields__:
    print(f"  {field}: {getattr(distinct_from, field)}")

# NULLIF proof
nullif_result = spark.sql(f"""
    SELECT
        SUM(CASE WHEN NULLIF(is_critical, 0) IS NULL THEN 1 ELSE 0 END) as nullif_zero_or_null
    FROM local.clinical.{TABLE_NAME}
""").collect()[0]
print(f"\nNULLIF(is_critical, 0) IS NULL count: {nullif_result.nullif_zero_or_null}")

# Per-patient counts
patient_stats = spark.sql(f"""
    SELECT patient_name, COUNT(*) as cnt,
           COUNT(result_value) as has_result
    FROM local.clinical.{TABLE_NAME}
    GROUP BY patient_name ORDER BY patient_name
""").collect()
print(f"\nPer-patient stats:")
for r in patient_stats:
    print(f"  {r.patient_name}: count={r.cnt}, has_result={r.has_result}")

# ── Copy table to demo directory ──────────────────────────────────────
table_loc = f"{WAREHOUSE}/clinical/{TABLE_NAME}"
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
