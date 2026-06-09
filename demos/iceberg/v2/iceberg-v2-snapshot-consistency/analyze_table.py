#!/usr/bin/env python3
"""
Analyze Iceberg V2 inventory table parquet + delete files using DuckDB.

Reconstructs the final state by applying position deletes to the data files,
then computes all values needed for ASSERT statements in queries.sql.
"""
import os
import json
import duckdb

DEMO_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_DIR = os.path.join(DEMO_DIR, "inventory")
DATA_DIR = os.path.join(TABLE_DIR, "data")
META_DIR = os.path.join(TABLE_DIR, "metadata")

con = duckdb.connect()

# ---------------------------------------------------------------------------
# Step 0: Inventory all files
# ---------------------------------------------------------------------------
print("=== DATA FILES ===")
data_files = sorted(os.listdir(DATA_DIR))
parquet_data = []
parquet_deletes = []
for f in data_files:
    full = os.path.join(DATA_DIR, f)
    size = os.path.getsize(full)
    is_delete = "deletes" in f
    print(f"  {'[DELETE]' if is_delete else '[DATA]  '} {f} ({size:,} bytes)")
    if is_delete:
        parquet_deletes.append(full)
    else:
        parquet_data.append(full)

# ---------------------------------------------------------------------------
# Step 1: Inspect schemas
# ---------------------------------------------------------------------------
print("\n=== SCHEMA: first data file ===")
print(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_data[0]}')").fetchdf().to_string())

print("\n=== SCHEMA: first delete file ===")
print(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{parquet_deletes[0]}')").fetchdf().to_string())

# ---------------------------------------------------------------------------
# Step 2: Inspect delete files — see what they reference
# ---------------------------------------------------------------------------
print("\n=== DELETE FILE CONTENTS ===")
for df_path in parquet_deletes:
    fname = os.path.basename(df_path)
    rows = con.execute(f"SELECT * FROM read_parquet('{df_path}')").fetchdf()
    print(f"\n  {fname}: {len(rows)} position deletes")
    print(f"    Distinct file_path values:")
    for fp in rows['file_path'].unique():
        print(f"      {fp}")
    print(f"    Positions: {sorted(rows['pos'].tolist())}")

# ---------------------------------------------------------------------------
# Step 3: Load all data files with row positions
# ---------------------------------------------------------------------------
print("\n=== LOADING DATA FILES ===")
for i, dp in enumerate(parquet_data):
    tname = f"data_{i}"
    con.execute(f"""
        CREATE TABLE {tname} AS
        SELECT
            row_number() OVER () - 1 AS _pos,
            *
        FROM read_parquet('{dp}')
    """)
    cnt = con.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
    print(f"  {os.path.basename(dp)}: {cnt} rows -> table {tname}")

# ---------------------------------------------------------------------------
# Step 4: Load delete files and map to their target data files
# ---------------------------------------------------------------------------
print("\n=== MAPPING DELETES TO DATA FILES ===")

# Build a map: data_file_basename -> list of (pos) to delete
delete_map = {}  # data_file_path -> set of positions
for df_path in parquet_deletes:
    rows = con.execute(f"SELECT file_path, pos FROM read_parquet('{df_path}')").fetchdf()
    for _, row in rows.iterrows():
        target = row['file_path']
        pos = row['pos']
        if target not in delete_map:
            delete_map[target] = set()
        delete_map[target].add(pos)

for target, positions in delete_map.items():
    print(f"  Target: ...{os.path.basename(target)}")
    print(f"    Positions to delete: {sorted(positions)}")
    print(f"    Count: {len(positions)}")

# ---------------------------------------------------------------------------
# Step 5: Reconstruct final state
# ---------------------------------------------------------------------------
print("\n=== RECONSTRUCTING FINAL STATE ===")

# Match data files to their delete positions
# The file_path in delete files is the absolute path from Spark warehouse
# We match by basename
data_basename_map = {}  # basename -> table_name
for i, dp in enumerate(parquet_data):
    data_basename_map[os.path.basename(dp)] = f"data_{i}"

# Build final state: union all data files, excluding deleted positions
union_parts = []
for i, dp in enumerate(parquet_data):
    tname = f"data_{i}"
    basename = os.path.basename(dp)

    # Find delete positions for this data file
    del_positions = set()
    for target_path, positions in delete_map.items():
        if basename in target_path:
            del_positions.update(positions)

    if del_positions:
        pos_list = ", ".join(str(p) for p in sorted(del_positions))
        sql = f"SELECT sku, product_name, category, unit_price, quantity_on_hand, supplier, last_restocked FROM {tname} WHERE _pos NOT IN ({pos_list})"
        excluded = con.execute(f"SELECT COUNT(*) FROM {tname} WHERE _pos IN ({pos_list})").fetchone()[0]
        kept = con.execute(f"SELECT COUNT(*) FROM {tname} WHERE _pos NOT IN ({pos_list})").fetchone()[0]
        print(f"  {basename}: {excluded} rows deleted, {kept} rows kept")
    else:
        sql = f"SELECT sku, product_name, category, unit_price, quantity_on_hand, supplier, last_restocked FROM {tname}"
        cnt = con.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
        print(f"  {basename}: no deletes, {cnt} rows kept")

    union_parts.append(sql)

final_sql = " UNION ALL ".join(union_parts)
con.execute(f"CREATE TABLE final_state AS {final_sql}")

total_rows = con.execute("SELECT COUNT(*) FROM final_state").fetchone()[0]
print(f"\n  Final state total rows: {total_rows}")

# ---------------------------------------------------------------------------
# Step 6: Compute all ASSERT values
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("ASSERT VALUES FOR queries.sql")
print("=" * 70)

# Q1: Full scan — ROW_COUNT
print(f"\nQ1: Full scan ROW_COUNT = {total_rows}")

# Q2: Category breakdown
print("\nQ2: Category breakdown:")
cat_breakdown = con.execute("""
    SELECT category,
           COUNT(*) AS product_count,
           SUM(quantity_on_hand) AS total_qty
    FROM final_state
    GROUP BY category
    ORDER BY category
""").fetchdf()
print(cat_breakdown.to_string())

# Q3: Electronics avg price
elec_avg = con.execute("""
    SELECT ROUND(AVG(unit_price), 2) AS avg_price
    FROM final_state
    WHERE category = 'Electronics'
""").fetchone()[0]
print(f"\nQ3: Electronics avg unit_price = {elec_avg}")

# Q4: New products count (SKU-%-N%)
new_count = con.execute("""
    SELECT COUNT(*) FROM final_state WHERE sku LIKE 'SKU-%-N%'
""").fetchone()[0]
print(f"\nQ4: New products (SKU-%-N%) ROW_COUNT = {new_count}")

# Q5: Discontinued absent
disc_skus = [
    'SKU-E007', 'SKU-E013', 'SKU-H008', 'SKU-H018', 'SKU-H019',
    'SKU-S006', 'SKU-S008', 'SKU-C004', 'SKU-C017', 'SKU-C018'
]
disc_list = ", ".join(f"'{s}'" for s in disc_skus)
disc_count = con.execute(f"SELECT COUNT(*) FROM final_state WHERE sku IN ({disc_list})").fetchone()[0]
print(f"\nQ5: Discontinued SKUs remaining = {disc_count}")

# Q6: Supplier analysis
print("\nQ6: Supplier analysis:")
supplier_data = con.execute("""
    SELECT supplier, COUNT(*) AS product_count
    FROM final_state
    GROUP BY supplier
    ORDER BY product_count DESC, supplier
""").fetchdf()
print(supplier_data.to_string())
supplier_count = len(supplier_data)
print(f"  Distinct suppliers: {supplier_count}")

# Q7: Inventory value per category
print("\nQ7: Inventory value per category:")
inv_value = con.execute("""
    SELECT category,
           ROUND(SUM(unit_price * quantity_on_hand), 2) AS inventory_value
    FROM final_state
    GROUP BY category
    ORDER BY category
""").fetchdf()
print(inv_value.to_string())

# VERIFY: Grand totals
print("\nVERIFY: Grand totals:")
verify = con.execute("""
    SELECT
        COUNT(*) AS total_products,
        COUNT(DISTINCT category) AS category_count,
        ROUND(SUM(unit_price * quantity_on_hand), 2) AS total_inventory_value,
        ROUND(AVG(unit_price), 2) AS avg_price,
        SUM(quantity_on_hand) AS total_quantity
    FROM final_state
""").fetchdf()
print(verify.to_string())

total_products = verify['total_products'].iloc[0]
category_count = verify['category_count'].iloc[0]
total_inv_value = verify['total_inventory_value'].iloc[0]
avg_price = verify['avg_price'].iloc[0]
total_qty = verify['total_quantity'].iloc[0]

print(f"\n  total_products = {total_products}")
print(f"  category_count = {category_count}")
print(f"  total_inventory_value = {total_inv_value}")
print(f"  avg_price = {avg_price}")
print(f"  total_quantity = {total_qty}")

# Extra: verify new product details
print("\nNew products detail:")
new_prods = con.execute("""
    SELECT sku, product_name, category, unit_price
    FROM final_state
    WHERE sku LIKE 'SKU-%-N%'
    ORDER BY sku
""").fetchdf()
print(new_prods.to_string())

# Extra: verify discontinued are truly absent
print("\nDiscontinued SKUs check:")
for sku in disc_skus:
    cnt = con.execute(f"SELECT COUNT(*) FROM final_state WHERE sku = '{sku}'").fetchone()[0]
    print(f"  {sku}: {cnt} rows (should be 0)")

# Q8: Snapshot count from metadata
meta_files = sorted([f for f in os.listdir(META_DIR) if f.endswith(".metadata.json")])
latest_meta = os.path.join(META_DIR, meta_files[-1])
with open(latest_meta) as f:
    meta = json.load(f)

snapshot_count = len(meta.get("snapshots", []))
print(f"\nQ8: Snapshot count = {snapshot_count}")
for snap in meta.get("snapshots", []):
    summary = snap.get("summary", {})
    op = summary.get("operation", "unknown")
    total = summary.get("total-records", "?")
    print(f"  Snapshot {snap['snapshot-id']}: {op}, total-records={total}")

print("\n=== DONE ===")
