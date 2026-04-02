#!/usr/bin/env python3
"""
Iceberg UniForm Drop Columns (GDPR PII Removal) — Data Verification
======================================================================
Reads the user_profiles table purely through the Iceberg metadata chain
and verifies the final state after GDPR PII column drops:
  - 20 user profiles seeded with 9 columns (V1)
  - DROP COLUMN email (V2)
  - DROP COLUMN phone (V3)
  - DROP COLUMN ip_address (V4)
  - INSERT 4 new users with 6-column schema (V5)

Final state: 24 users, 6 columns (user_id, username, country,
signup_date, last_login, subscription_tier). PII columns removed.

Usage:
    python verify.py <data_root_path> [--verbose]

    data_root_path: parent folder containing user_profiles/

Requirements:
    pip install pyiceberg[pyarrow] fastavro
"""

import argparse
import glob
import gzip
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from verify_lib import (ok, fail, info,
    assert_row_count, assert_distinct_count,
    assert_count_where, assert_format_version,
    print_header, print_section, print_summary, exit_with_status)


# ---------------------------------------------------------------------------
# Custom reader: filters Parquet columns to current Iceberg schema
# (needed because DROP COLUMN leaves old data files with extra columns)
# ---------------------------------------------------------------------------
def _read_iceberg_table_drop(table_path):
    """Read table with column projection to current schema (handles DROP COLUMN)."""
    import fastavro
    import pyarrow as pa
    import pyarrow.parquet as pq

    meta_dir = os.path.join(table_path, "metadata")
    meta_files = sorted(glob.glob(os.path.join(meta_dir, "v*.metadata.json")))
    gz_files = sorted(glob.glob(os.path.join(meta_dir, "v*.metadata.json.gz")))
    all_meta = meta_files + gz_files
    if not all_meta:
        raise FileNotFoundError(f"No metadata files in {meta_dir}")

    latest_meta = all_meta[-1]
    if latest_meta.endswith(".gz"):
        with gzip.open(latest_meta, "rt") as f:
            metadata = json.load(f)
    else:
        with open(latest_meta) as f:
            metadata = json.load(f)

    fmt_version = metadata.get("format-version", 2)

    schema_fields = []
    if fmt_version == 1:
        schema = metadata.get("schema")
        schemas = metadata.get("schemas", [])
        if schema:
            schema_fields = schema.get("fields", [])
        elif schemas:
            schema_fields = schemas[-1].get("fields", [])
    else:
        schemas = metadata.get("schemas", [])
        if schemas:
            schema_fields = schemas[-1].get("fields", [])

    field_id_to_name = {f["id"]: f["name"] for f in schema_fields}

    snapshots = metadata.get("snapshots", [])
    if not snapshots:
        raise ValueError("No snapshots in metadata")

    latest_snap = snapshots[-1]
    ml_path_raw = latest_snap.get("manifest-list", "")

    def from_uri(u):
        return u.replace("file:///", "").replace("file://", "")

    ml_path = from_uri(ml_path_raw)
    if not os.path.isfile(ml_path):
        ml_path = os.path.join(table_path, "metadata", os.path.basename(ml_path))

    with open(ml_path, "rb") as f:
        ml_records = list(fastavro.reader(f))

    data_files = []
    for ml_rec in ml_records:
        m_path = from_uri(ml_rec.get("manifest_path", ""))
        if not os.path.isfile(m_path):
            m_path = os.path.join(table_path, "metadata", os.path.basename(m_path))
        with open(m_path, "rb") as f:
            for entry in fastavro.reader(f):
                df_entry = entry.get("data_file", entry)
                status = entry.get("status", 1)
                if status != 2:
                    fp = from_uri(df_entry.get("file_path", ""))
                    if not os.path.isfile(fp):
                        fp = os.path.join(table_path, os.path.basename(fp))
                    data_files.append(fp)

    tables = []
    for df_path in data_files:
        pf = pq.read_table(df_path)
        arrow_schema = pf.schema
        rename_map = {}
        for arrow_field in arrow_schema:
            md = arrow_field.metadata or {}
            fid = md.get(b"PARQUET:field_id")
            if fid is not None:
                fid_int = int(fid)
                if fid_int in field_id_to_name:
                    rename_map[arrow_field.name] = field_id_to_name[fid_int]
        if rename_map:
            # Only keep columns that are in the current schema
            new_names = [rename_map.get(c, c) for c in pf.column_names]
            pf = pf.rename_columns(new_names)
        # Filter to only columns in the current Iceberg schema
        current_cols = set(field_id_to_name.values())
        keep_cols = [c for c in pf.column_names if c in current_cols]
        pf = pf.select(keep_cols)
        tables.append(pf)

    return pa.concat_tables(tables), metadata


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
def verify_user_profiles(data_root, verbose=False):
    print_section("user_profiles — Drop Columns (GDPR PII Removal)")

    table_path = os.path.join(data_root, "user_profiles")
    table, metadata = _read_iceberg_table_drop(table_path)
    ok(f"Loaded {table.num_rows} rows, {len(table.column_names)} columns via Iceberg")

    if verbose:
        info(f"Columns: {table.column_names}")
        info("First 3 rows:")
        sample = table.slice(0, min(3, table.num_rows)).to_pydict()
        for i in range(min(3, table.num_rows)):
            row = {c: sample[c][i] for c in table.column_names}
            info(f"    {row}")

    assert_format_version(metadata, 1)

    # Final state: 24 users (20 original + 4 new)
    assert_row_count(table, 24)

    # PII columns should NOT be in the current Iceberg schema
    for dropped_col in ["email", "phone", "ip_address"]:
        if dropped_col not in table.column_names:
            ok(f"Dropped column '{dropped_col}' not in schema")
        else:
            fail(f"Dropped column '{dropped_col}' still in schema")

    # Remaining columns should include these 6
    expected_cols = {"user_id", "username", "country", "signup_date", "last_login", "subscription_tier"}
    actual_cols = set(table.column_names)
    missing = expected_cols - actual_cols
    if not missing:
        ok(f"All 6 expected columns present: {sorted(expected_cols)}")
    else:
        fail(f"Missing expected columns: {missing}")

    # 6 columns total
    if len(table.column_names) == 6:
        ok(f"Column count = 6 (PII columns removed)")
    else:
        fail(f"Column count = {len(table.column_names)}, expected 6")

    # 4 countries, 6 each
    assert_distinct_count(table, "country", 4)
    assert_count_where(table, "country", "DE", 6)
    assert_count_where(table, "country", "JP", 6)
    assert_count_where(table, "country", "UK", 6)
    assert_count_where(table, "country", "US", 6)

    # Subscription tier distribution
    assert_count_where(table, "subscription_tier", "enterprise", 6)
    assert_count_where(table, "subscription_tier", "free", 8)
    assert_count_where(table, "subscription_tier", "pro", 10)

    # Verify new users exist
    import pyarrow.compute as pc
    for uid, expected_country in [(21, "US"), (22, "UK"), (23, "DE"), (24, "JP")]:
        mask = pc.equal(table.column("user_id"), uid)
        filtered = table.filter(mask)
        if filtered.num_rows == 1:
            actual_country = filtered.column("country")[0].as_py()
            if actual_country == expected_country:
                ok(f"New user_id={uid} in country={expected_country}")
            else:
                fail(f"New user_id={uid} country={actual_country}, expected {expected_country}")
        else:
            fail(f"New user_id={uid} not found")


def main():
    parser = argparse.ArgumentParser(
        description="Verify Iceberg data for iceberg-uniform-schema-drop demo"
    )
    parser.add_argument(
        "data_root",
        help="Root path containing user_profiles/"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show sample data and column names"
    )
    args = parser.parse_args()

    data_root = os.path.abspath(args.data_root)

    print_header("Iceberg UniForm Drop Columns — Data Verification")
    print(f"  Data root: {data_root}")

    tbl_dir = os.path.join(data_root, "user_profiles")
    if not os.path.isdir(tbl_dir):
        print(f"\nError: {tbl_dir} not found")
        sys.exit(1)

    verify_user_profiles(data_root, verbose=args.verbose)

    print_summary()
    exit_with_status()


if __name__ == "__main__":
    main()
