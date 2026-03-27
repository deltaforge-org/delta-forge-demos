#!/usr/bin/env python3
"""
Analyze the Iceberg V2 copy-on-write shipments table.

Copy-on-write has NO delete files — data files are rewritten on UPDATE/DELETE.
This script reads the latest metadata JSON, traces through the manifest chain
to find the current snapshot's data files, and reads only those with DuckDB.
"""
import os
import json
import duckdb

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_DIR = os.path.join(DEMO_DIR, "shipments")
DATA_DIR = os.path.join(TABLE_DIR, "data")
META_DIR = os.path.join(TABLE_DIR, "metadata")

# ── Step 1: Find the latest metadata JSON ────────────────────────────
meta_files = sorted([f for f in os.listdir(META_DIR) if f.endswith(".metadata.json")])
latest_meta_path = os.path.join(META_DIR, meta_files[-1])

with open(latest_meta_path) as f:
    meta = json.load(f)

print(f"Metadata file: {meta_files[-1]}")
print(f"Format version: {meta['format-version']}")
print(f"Table properties:")
for k, v in meta.get("properties", {}).items():
    print(f"  {k} = {v}")

# ── Step 2: Show all snapshots ────────────────────────────────────────
print(f"\nSnapshots ({len(meta['snapshots'])}):")
for snap in meta["snapshots"]:
    summary = snap["summary"]
    op = summary.get("operation", "?")
    total = summary.get("total-records", "?")
    added_files = summary.get("added-data-files", "?")
    deleted_files = summary.get("deleted-data-files", "?")
    print(f"  {snap['snapshot-id']}: op={op}, total-records={total}, "
          f"+files={added_files}, -files={deleted_files}")

# ── Step 3: List data files — verify NO delete files ──────────────────
print("\nData files on disk:")
data_files = []
for f in sorted(os.listdir(DATA_DIR)):
    full = os.path.join(DATA_DIR, f)
    size = os.path.getsize(full)
    is_delete = "deletes" in f.lower()
    marker = " *** DELETE FILE ***" if is_delete else ""
    print(f"  {f} ({size:,} bytes){marker}")
    data_files.append((f, full, size, is_delete))

delete_count = sum(1 for _, _, _, d in data_files if d)
if delete_count == 0:
    print("  CONFIRMED: No delete files (copy-on-write working correctly)")
else:
    print(f"  WARNING: Found {delete_count} delete files!")

# ── Step 4: Find current snapshot's data file via manifest chain ──────
current_snap_id = meta["current-snapshot-id"]
current_snap = None
for snap in meta["snapshots"]:
    if snap["snapshot-id"] == current_snap_id:
        current_snap = snap
        break

print(f"\nCurrent snapshot: {current_snap_id}")
manifest_list_path = current_snap["manifest-list"]
print(f"Manifest list: {os.path.basename(manifest_list_path)}")

# Read manifest list (Avro) to find data manifests
con = duckdb.connect()

# The manifest list path in metadata may be absolute to the warehouse.
# We need to map it to our local copy.
manifest_list_basename = os.path.basename(manifest_list_path)
local_manifest_list = os.path.join(META_DIR, manifest_list_basename)

print(f"\nReading manifest list: {manifest_list_basename}")
manifest_entries = con.execute(f"""
    SELECT manifest_path, manifest_length, added_files_count,
           existing_files_count, deleted_files_count, content
    FROM read_avro('{local_manifest_list}')
""").fetchdf()
print(manifest_entries.to_string())

# Read each data manifest to find current data files
print("\nData files from manifests:")
current_data_files = []
for _, row in manifest_entries.iterrows():
    manifest_basename = os.path.basename(row["manifest_path"])
    local_manifest = os.path.join(META_DIR, manifest_basename)
    content_type = row.get("content", 0)  # 0=data, 1=deletes

    entries = con.execute(f"""
        SELECT data_file
        FROM read_avro('{local_manifest}')
        WHERE status != 2
    """).fetchdf()

    for _, entry in entries.iterrows():
        data_file_info = entry["data_file"]
        if isinstance(data_file_info, dict):
            file_path = data_file_info.get("file_path", "")
            file_basename = os.path.basename(file_path)
            content = data_file_info.get("content", 0)
            record_count = data_file_info.get("record_count", 0)
            print(f"  {file_basename}: content={content}, records={record_count}")
            if content == 0:  # data file (not delete)
                current_data_files.append(os.path.join(DATA_DIR, file_basename))

print(f"\nCurrent data files for reading: {len(current_data_files)}")
for f in current_data_files:
    print(f"  {os.path.basename(f)}")

# ── Step 5: Read current data files with DuckDB ──────────────────────
if len(current_data_files) == 1:
    query_path = f"'{current_data_files[0]}'"
else:
    paths = ", ".join(f"'{f}'" for f in current_data_files)
    query_path = f"[{paths}]"

con.execute(f"""
    CREATE TABLE shipments AS
    SELECT * FROM read_parquet({query_path})
""")

total_rows = con.execute("SELECT COUNT(*) FROM shipments").fetchone()[0]
print(f"\nTotal rows in current snapshot: {total_rows}")

# ── Step 6: Compute values for ASSERT statements ─────────────────────
print("\n" + "=" * 70)
print("ASSERT VALUES")
print("=" * 70)

# Q1: Full scan
print(f"\nQ1 — SELECT * — ROW_COUNT = {total_rows}")

# Q2: Status breakdown
print("\nQ2 — Status breakdown:")
status_df = con.execute("""
    SELECT status, COUNT(*) AS shipment_count
    FROM shipments
    GROUP BY status
    ORDER BY status
""").fetchdf()
print(status_df.to_string())

# Q3: Carrier analysis
print("\nQ3 — Carrier analysis:")
carrier_df = con.execute("""
    SELECT carrier,
           COUNT(*) AS shipment_count,
           ROUND(AVG(shipping_cost), 2) AS avg_cost
    FROM shipments
    GROUP BY carrier
    ORDER BY carrier
""").fetchdf()
print(carrier_df.to_string())

# Q4: Verify deleted shipments absent
deleted_ids = ['SHP-0009', 'SHP-0012', 'SHP-0015', 'SHP-0017', 'SHP-0020',
               'SHP-0022', 'SHP-0029', 'SHP-0031', 'SHP-0032', 'SHP-0036']
ids_str = ", ".join(f"'{s}'" for s in deleted_ids)
deleted_count = con.execute(f"""
    SELECT COUNT(*) FROM shipments
    WHERE shipment_id IN ({ids_str})
""").fetchone()[0]
print(f"\nQ4 — Deleted shipments present: {deleted_count} (expect 0)")

# Q5: Verify updated shipments — actual_delivery NOT NULL
delivered_count = con.execute("""
    SELECT COUNT(*) FROM shipments
    WHERE actual_delivery IS NOT NULL
""").fetchone()[0]
print(f"\nQ5 — Shipments with actual_delivery: {delivered_count}")

# Show which were originally delivered vs newly delivered
updated_ids = ['SHP-0001', 'SHP-0002', 'SHP-0003', 'SHP-0005', 'SHP-0008',
               'SHP-0010', 'SHP-0011', 'SHP-0018', 'SHP-0019', 'SHP-0024',
               'SHP-0027', 'SHP-0028', 'SHP-0033', 'SHP-0035', 'SHP-0049',
               'SHP-0050', 'SHP-0054', 'SHP-0057', 'SHP-0070', 'SHP-0074']
upd_str = ", ".join(f"'{s}'" for s in updated_ids)
updated_with_delivery = con.execute(f"""
    SELECT COUNT(*) FROM shipments
    WHERE shipment_id IN ({upd_str}) AND actual_delivery IS NOT NULL
""").fetchone()[0]
print(f"  Updated shipments with actual_delivery: {updated_with_delivery}")

# Q6: Priority breakdown
print("\nQ6 — Priority breakdown:")
priority_df = con.execute("""
    SELECT priority, COUNT(*) AS shipment_count
    FROM shipments
    GROUP BY priority
    ORDER BY priority
""").fetchdf()
print(priority_df.to_string())

# VERIFY: Grand totals
print("\nVERIFY — Grand totals:")
verify_df = con.execute("""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT carrier) AS carrier_count,
        COUNT(DISTINCT status) AS status_count,
        COUNT(DISTINCT priority) AS priority_count,
        SUM(CASE WHEN actual_delivery IS NOT NULL THEN 1 ELSE 0 END) AS delivered_with_date,
        ROUND(SUM(shipping_cost), 2) AS total_shipping_cost,
        ROUND(AVG(weight_kg), 2) AS avg_weight
    FROM shipments
""").fetchdf()
print(verify_df.to_string())

# Extra: show sample rows
print("\n=== Sample rows (first 10 by shipment_id) ===")
print(con.execute("""
    SELECT * FROM shipments ORDER BY shipment_id LIMIT 10
""").fetchdf().to_string())

# Extra: show the updated rows
print("\n=== Updated shipments (In Transit → Delivered) ===")
print(con.execute(f"""
    SELECT shipment_id, status, actual_delivery
    FROM shipments
    WHERE shipment_id IN ({upd_str})
    ORDER BY shipment_id
""").fetchdf().to_string())

# Remaining In Transit count
remaining_in_transit = con.execute("""
    SELECT COUNT(*) FROM shipments WHERE status = 'In Transit'
""").fetchone()[0]
print(f"\nRemaining In Transit: {remaining_in_transit}")

# Count statuses with more detail
print("\n=== Detailed status with NULL delivery check ===")
print(con.execute("""
    SELECT status,
           COUNT(*) as cnt,
           SUM(CASE WHEN actual_delivery IS NULL THEN 1 ELSE 0 END) as null_delivery
    FROM shipments
    GROUP BY status
    ORDER BY status
""").fetchdf().to_string())

con.close()
