#!/usr/bin/env python3
"""
Generate an Iceberg V3 table with equality delete files.

Scenario: Healthcare EHR platform — 500 patient visit records across
5 hospitals. Four patients exercise their GDPR "right to erasure":
all records matching their patient_id are removed via equality deletes.

Equality deletes differ from position deletes: instead of listing
(file_path, row_position) pairs, the delete file contains column values
that identify rows to remove. Any row in ANY data file that matches
those values is logically deleted. This is powerful for GDPR-style
"forget me" requests where you don't know which data files contain
the user's records.

Strategy: Spark 4.0 does not produce equality deletes (it uses position
deletes or DVs). We create the base V3 table with Spark, then manually
construct the equality delete file and metadata using PyArrow + fastavro.

Output: patient_visits/ directory with Iceberg V3 metadata, data files,
and Parquet equality delete files — ready for DeltaForge to read.
"""
import os
import sys
import shutil
import json
import struct

# PySpark + Iceberg setup
ICEBERG_JAR = os.path.expanduser(
    "~/.ivy2.5.2/jars/org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-1.10.1.jar"
)
JAVA_HOME = os.path.expanduser("~/local/jdk")
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = f"{JAVA_HOME}/bin:{os.environ['PATH']}"

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_NAME = "patient_visits"
TABLE_OUTPUT = os.path.join(DEMO_DIR, TABLE_NAME)
WAREHOUSE = "/tmp/iceberg_v3_eqdelete_warehouse"

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
    .appName("IcebergV3EqualityDeleteGenerator")
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

random.seed(31415)

HOSPITALS = [
    "Mount-Sinai-NYC",
    "Mayo-Clinic-Rochester",
    "Johns-Hopkins-Baltimore",
    "Mass-General-Boston",
    "Cleveland-Clinic-OH",
]
DEPARTMENTS = [
    "Cardiology", "Oncology", "Neurology", "Orthopedics",
    "Emergency", "Pediatrics", "Radiology", "Dermatology",
]
PHYSICIANS = [f"Dr-{name}" for name in [
    "Agarwal", "Becker", "Chen", "Dubois", "Evans",
    "Fischer", "Garcia", "Huang", "Ivanova", "Jackson",
]]
DIAGNOSIS_CODES = [
    "I25.1", "C34.9", "G43.9", "M54.5", "J06.9",
    "E11.9", "K21.0", "N39.0", "F32.9", "L30.9",
]

# Generate 80 distinct patients — some with many visits, some with few
PATIENTS = [f"P-{i:04d}" for i in range(1, 81)]

# 4 patients to delete (GDPR erasure)
GDPR_PATIENTS = ["P-0012", "P-0025", "P-0041", "P-0067"]

rows = []
base_date = date(2024, 7, 1)
visit_id = 1

# Generate 500 visits, distributing across patients unevenly
# Some patients have more visits (chronic conditions)
for i in range(500):
    # Weight towards some patients having more visits
    if i < 40:
        # First 40 visits: heavily weight GDPR patients (ensure they have records)
        if i < 12:
            patient = "P-0012"   # 12 visits
        elif i < 22:
            patient = "P-0025"  # 10 visits
        elif i < 29:
            patient = "P-0041"  # 7 visits
        elif i < 34:
            patient = "P-0067"  # 5 visits
        else:
            patient = random.choice(PATIENTS)
    else:
        patient = random.choice(PATIENTS)

    hospital = random.choice(HOSPITALS)
    department = random.choice(DEPARTMENTS)
    physician = random.choice(PHYSICIANS)
    diagnosis = random.choice(DIAGNOSIS_CODES)
    treatment_cost = round(random.uniform(150.0, 25000.0), 2)
    is_emergency = random.random() < 0.15  # ~15% emergency
    visit_date = (base_date + timedelta(days=random.randint(0, 179))).isoformat()

    rows.append((
        f"V-{visit_id:05d}",
        patient,
        hospital,
        department,
        physician,
        diagnosis,
        treatment_cost,
        is_emergency,
        visit_date,
    ))
    visit_id += 1

schema = StructType([
    StructField("visit_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("hospital", StringType(), False),
    StructField("department", StringType(), False),
    StructField("attending_physician", StringType(), False),
    StructField("diagnosis_code", StringType(), False),
    StructField("treatment_cost", DoubleType(), False),
    StructField("is_emergency", BooleanType(), False),
    StructField("visit_date", StringType(), False),
])

df = spark.createDataFrame(rows, schema)
total_rows = df.count()
print(f"Generated {total_rows} rows")

# Count GDPR patient records
for p in GDPR_PATIENTS:
    cnt = df.filter(F.col("patient_id") == p).count()
    print(f"  {p}: {cnt} visits")

gdpr_total = df.filter(F.col("patient_id").isin(GDPR_PATIENTS)).count()
print(f"  Total GDPR rows to delete: {gdpr_total}")
print(f"  Expected post-delete count: {total_rows - gdpr_total}")

# ── Step 2: Create V3 table and load data (no deletes yet) ───────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.healthcare")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS local.healthcare.{TABLE_NAME} (
        visit_id STRING NOT NULL,
        patient_id STRING NOT NULL,
        hospital STRING NOT NULL,
        department STRING NOT NULL,
        attending_physician STRING NOT NULL,
        diagnosis_code STRING NOT NULL,
        treatment_cost DOUBLE NOT NULL,
        is_emergency BOOLEAN NOT NULL,
        visit_date STRING NOT NULL
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '3',
        'write.delete.mode' = 'merge-on-read'
    )
""")

df.coalesce(1).writeTo(f"local.healthcare.{TABLE_NAME}").append()
print(f"Loaded data into Iceberg V3 table (no deletes yet)")

# ── Step 3: Compute proof values BEFORE deletes (for the base data) ──
# We'll compute post-delete proofs by filtering in Python since we can't
# actually use equality deletes in Spark

# Get all rows from the table for proof computation
all_rows = spark.sql(f"SELECT * FROM local.healthcare.{TABLE_NAME}").collect()
print(f"Verified: {len(all_rows)} rows in table")

spark.stop()
print("Spark stopped. Now constructing equality delete files manually...")

# ── Step 4: Manually construct equality delete file + metadata ────────
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import uuid
import time

table_loc = f"{WAREHOUSE}/healthcare/{TABLE_NAME}"
data_dir = os.path.join(table_loc, "data")
metadata_dir = os.path.join(table_loc, "metadata")

# 4a. Write equality delete Parquet file
# The equality delete file contains one column: patient_id
# Each row represents a patient_id to delete
delete_table = pa.table({
    "patient_id": pa.array(GDPR_PATIENTS, type=pa.string()),
})

delete_filename = f"00000-0-{uuid.uuid4()}-00001-eq-deletes.parquet"
delete_filepath = os.path.join(data_dir, delete_filename)
pq.write_table(delete_table, delete_filepath)
delete_filesize = os.path.getsize(delete_filepath)
print(f"Wrote equality delete file: {delete_filename} ({delete_filesize} bytes)")

# 4b. Read existing metadata to understand the table state
meta_files = sorted([f for f in os.listdir(metadata_dir) if f.endswith(".metadata.json")])
latest_meta_path = os.path.join(metadata_dir, meta_files[-1])
with open(latest_meta_path) as f:
    meta = json.load(f)

# Get the current snapshot's manifest list
current_snap_id = meta["current-snapshot-id"]
current_snap = None
for snap in meta["snapshots"]:
    if snap["snapshot-id"] == current_snap_id:
        current_snap = snap
        break

# The patient_id field has field-id 2 in the schema
patient_id_field_id = None
for field in meta["schemas"][0]["fields"]:
    if field["name"] == "patient_id":
        patient_id_field_id = field["id"]
        break
print(f"patient_id field-id: {patient_id_field_id}")

# 4c. Read the existing data manifest to get the data file entry
existing_manifest_list_path = current_snap["manifest-list"]
# Normalize the path (strip file: prefix)
if existing_manifest_list_path.startswith("file:"):
    existing_manifest_list_path = existing_manifest_list_path.lstrip("file:")

with open(existing_manifest_list_path, "rb") as f:
    ml_reader = fastavro.reader(f)
    ml_schema = ml_reader.writer_schema
    manifest_list_entries = list(ml_reader)

print(f"Existing manifest list has {len(manifest_list_entries)} entries")

# Read the data manifest
data_manifest_path = manifest_list_entries[0]["manifest_path"]
if data_manifest_path.startswith("file:"):
    data_manifest_path = data_manifest_path.lstrip("file:")

with open(data_manifest_path, "rb") as f:
    m_reader = fastavro.reader(f)
    manifest_schema = m_reader.writer_schema
    data_manifest_entries = list(m_reader)

print(f"Data manifest has {len(data_manifest_entries)} entries")
data_file_entry = data_manifest_entries[0]
data_file_path = data_file_entry["data_file"]["file_path"]
print(f"Data file: ...{os.path.basename(data_file_path)}")

# 4d. Create a new manifest with the equality delete file entry
# We need to create a manifest entry for the equality delete file.
# The equality delete entry has content=2 and references the equality_ids.

new_manifest_id = str(uuid.uuid4())
new_manifest_filename = f"{new_manifest_id}-m0.avro"
new_manifest_path = os.path.join(metadata_dir, new_manifest_filename)

# Build the delete manifest entry, copying the structure from the data entry
# but with content=2 (equality deletes)
# fastavro uses raw Python values for union types, not {'long': val} dicts.
# Copy the structure from the existing data entry to ensure format compatibility.
delete_manifest_entry = {
    "status": 1,  # ADDED
    "snapshot_id": current_snap_id + 1,
    "sequence_number": 2,
    "file_sequence_number": 2,
    "data_file": {
        "content": 2,  # EQUALITY_DELETES
        "file_path": f"file:{delete_filepath}",
        "file_format": "PARQUET",
        "partition": data_file_entry["data_file"]["partition"],
        "record_count": len(GDPR_PATIENTS),
        "file_size_in_bytes": delete_filesize,
        "column_sizes": [
            {"key": patient_id_field_id, "value": delete_filesize}
        ],
        "value_counts": [
            {"key": patient_id_field_id, "value": len(GDPR_PATIENTS)}
        ],
        "null_value_counts": [
            {"key": patient_id_field_id, "value": 0}
        ],
        "nan_value_counts": [],
        "lower_bounds": [
            {"key": patient_id_field_id, "value": min(GDPR_PATIENTS).encode()}
        ],
        "upper_bounds": [
            {"key": patient_id_field_id, "value": max(GDPR_PATIENTS).encode()}
        ],
        "key_metadata": None,
        "split_offsets": [4],
        "equality_ids": [patient_id_field_id],
        "sort_order_id": 0,
        "first_row_id": None,
    },
}

# Carry forward the existing data file as EXISTING
existing_data_entry = dict(data_file_entry)
existing_data_entry["status"] = 0  # EXISTING
existing_data_entry["snapshot_id"] = current_snap_id
existing_data_entry["sequence_number"] = 1
existing_data_entry["file_sequence_number"] = 1

# Modify the manifest schema to support equality deletes
# Need to add equality_ids field if not present, and ensure first_row_id is present
# We'll use the same schema but need to handle it carefully.

# Read the manifest schema and check for equality_ids
manifest_schema_str = json.dumps(manifest_schema)
has_equality_ids = "equality_ids" in manifest_schema_str
print(f"Manifest schema has equality_ids: {has_equality_ids}")

# Write the new manifest with both entries
# We need to add snapshot-id and other metadata to the Avro file metadata
new_snap_id = current_snap_id + 1

# Build Avro metadata matching what Iceberg expects
avro_metadata = {
    "schema": json.dumps(meta["schemas"][0]),
    "schema-id": "0",
    "partition-spec": "[]",
    "partition-spec-id": "0",
    "format-version": "3",
    "content": "deletes",
}

# Write the delete manifest with ONLY the equality delete entry
# (the data file stays in the original data manifest)
with open(new_manifest_path, "wb") as f:
    fastavro.writer(
        f,
        manifest_schema,
        [delete_manifest_entry],
        metadata=avro_metadata,
    )

new_manifest_size = os.path.getsize(new_manifest_path)
print(f"Wrote delete manifest: {new_manifest_filename} ({new_manifest_size} bytes)")

# 4e. Create a new manifest list with TWO entries:
#     1. The ORIGINAL data manifest (content=0) — carried forward from parent snapshot
#     2. The NEW delete manifest (content=1) — contains the equality delete file
new_ml_id = str(uuid.uuid4())
new_snap_avro = f"snap-{new_snap_id}-1-{new_ml_id}.avro"
new_ml_path = os.path.join(metadata_dir, new_snap_avro)

# Carry forward the original data manifest list entry (content=0)
original_data_ml_entry = dict(manifest_list_entries[0])

# Create the new delete manifest list entry (content=1)
new_delete_ml_entry = {
    "manifest_path": f"file:{new_manifest_path}",
    "manifest_length": new_manifest_size,
    "partition_spec_id": 0,
    "content": 1,  # DELETES manifest
    "sequence_number": 2,
    "min_sequence_number": 2,
    "added_snapshot_id": new_snap_id,
    "added_files_count": 1,
    "existing_files_count": 0,
    "deleted_files_count": 0,
    "added_rows_count": len(GDPR_PATIENTS),
    "existing_rows_count": 0,
    "deleted_rows_count": 0,
    "partitions": [],
    "key_metadata": None,
}

# Check if manifest list schema has first_row_id
ml_schema_str = json.dumps(ml_schema)
if "first_row_id" in ml_schema_str:
    new_delete_ml_entry["first_row_id"] = None

avro_ml_metadata = {
    "snapshot-id": str(new_snap_id),
    "parent-snapshot-id": str(current_snap_id),
    "format-version": "3",
}

with open(new_ml_path, "wb") as f:
    fastavro.writer(
        f,
        ml_schema,
        [original_data_ml_entry, new_delete_ml_entry],
        metadata=avro_ml_metadata,
    )

print(f"Wrote manifest list: {new_snap_avro} ({os.path.getsize(new_ml_path)} bytes)")

# 4f. Update metadata.json with new snapshot
new_snap = {
    "sequence-number": 2,
    "snapshot-id": new_snap_id,
    "parent-snapshot-id": current_snap_id,
    "timestamp-ms": int(time.time() * 1000),
    "summary": {
        "operation": "delete",
        "added-delete-files": "1",
        "added-equality-deletes": str(len(GDPR_PATIENTS)),
        "total-records": str(total_rows),
        "total-data-files": "1",
        "total-delete-files": "1",
        "total-equality-deletes": str(len(GDPR_PATIENTS)),
        "total-position-deletes": "0",
        "total-files-size": str(
            data_file_entry["data_file"]["file_size_in_bytes"] + delete_filesize
        ),
        "engine-name": "manual-equality-delete-generator",
    },
    "manifest-list": f"file:{new_ml_path}",
    "schema-id": 0,
}

# Add first-row-id if V3 metadata has it
if "first-row-id" in current_snap:
    new_snap["first-row-id"] = current_snap.get("first-row-id", 0)
if "added-rows" in current_snap:
    new_snap["added-rows"] = 0

meta["snapshots"].append(new_snap)
meta["current-snapshot-id"] = new_snap_id
meta["last-sequence-number"] = 2
meta["last-updated-ms"] = new_snap["timestamp-ms"]
meta["refs"]["main"]["snapshot-id"] = new_snap_id

# Update snapshot log
meta["snapshot-log"].append({
    "timestamp-ms": new_snap["timestamp-ms"],
    "snapshot-id": new_snap_id,
})

# Write new metadata version
new_meta_version = len(meta_files) + 1
new_meta_filename = f"v{new_meta_version}.metadata.json"
new_meta_path = os.path.join(metadata_dir, new_meta_filename)

# Add metadata log entry for previous version
if "metadata-log" not in meta:
    meta["metadata-log"] = []
meta["metadata-log"].append({
    "timestamp-ms": meta["last-updated-ms"],
    "metadata-file": f"file:{latest_meta_path}",
})

with open(new_meta_path, "w") as f:
    json.dump(meta, f, indent=None)

print(f"Wrote metadata: {new_meta_filename} ({os.path.getsize(new_meta_path)} bytes)")

# ── Step 5: Compute proof values (post-delete) ──────────────────────
# Since we can't query through Spark anymore, compute proofs from the
# raw data, excluding GDPR patients
print("\n=== Proof Values (post-delete) ===")

# Re-read the Parquet data file directly
data_files = [f for f in os.listdir(data_dir) if f.endswith(".parquet") and "deletes" not in f]
data_parquet_path = os.path.join(data_dir, data_files[0])
full_table = pq.read_table(data_parquet_path)
full_df = full_table.to_pandas()

# Apply equality delete: remove rows where patient_id is in GDPR_PATIENTS
post_df = full_df[~full_df["patient_id"].isin(GDPR_PATIENTS)]
deleted_count = len(full_df) - len(post_df)

print(f"  total_rows (pre-delete): {len(full_df)}")
print(f"  deleted_rows: {deleted_count}")
print(f"  total_rows (post-delete): {len(post_df)}")
print(f"  hospital_count: {post_df['hospital'].nunique()}")
print(f"  department_count: {post_df['department'].nunique()}")
print(f"  patient_count: {post_df['patient_id'].nunique()}")
print(f"  gdpr_patient_rows: {len(post_df[post_df['patient_id'].isin(GDPR_PATIENTS)])}")
print(f"  total_cost: {round(post_df['treatment_cost'].sum(), 2)}")
print(f"  avg_cost: {round(post_df['treatment_cost'].mean(), 2)}")
print(f"  emergency_count: {int(post_df['is_emergency'].sum())}")

# Per-hospital counts
print("\n  Per-hospital counts:")
for hospital in sorted(post_df["hospital"].unique()):
    h_df = post_df[post_df["hospital"] == hospital]
    print(f"    {hospital}: count={len(h_df)}, avg_cost={round(h_df['treatment_cost'].mean(), 2)}, emergency={int(h_df['is_emergency'].sum())}")

# Per-department counts
print("\n  Per-department counts:")
for dept in sorted(post_df["department"].unique()):
    d_df = post_df[post_df["department"] == dept]
    print(f"    {dept}: {len(d_df)}")

# Physician workload
print("\n  Physician workload (top 5):")
phys_counts = post_df["attending_physician"].value_counts().head(5)
for phys, cnt in phys_counts.items():
    print(f"    {phys}: {cnt}")

# Diagnosis distribution
print("\n  Diagnosis distribution:")
diag_counts = post_df["diagnosis_code"].value_counts().sort_index()
for diag, cnt in diag_counts.items():
    print(f"    {diag}: {cnt}")

# ── Step 6: Copy table to demo directory ──────────────────────────────
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

print("\nDone!")
