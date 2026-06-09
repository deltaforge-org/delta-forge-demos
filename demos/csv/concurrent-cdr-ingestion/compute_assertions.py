#!/usr/bin/env python3
"""
Independent proof-value computation for the Concurrent CDR Ingestion demo.

Reads the generated region_*.csv files from ./data and prints every value that
queries.sql asserts on. Run this after generate_data.py and copy the printed
numbers into the ASSERT clauses. Re-run any time the data changes.

Run:  python3 compute_assertions.py
"""

import csv
import glob
import os
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")


def load_rows():
    rows = []
    for path in sorted(glob.glob(os.path.join(DATA_DIR, "region_*.csv"))):
        with open(path, newline="", encoding="utf-8") as fh:
            rows.extend(list(csv.DictReader(fh)))
    return rows


def is_blank(v):
    return v is None or v == ""


def main():
    rows = load_rows()
    n = len(rows)

    print("=" * 68)
    print("CONCURRENT CDR INGESTION  —  ASSERT proof values")
    print("=" * 68)

    # --- Totals / uniqueness ------------------------------------------------
    distinct_ids = len({r["cdr_id"] for r in rows})
    print(f"total_rows                 = {n}")
    print(f"distinct_cdr_id            = {distinct_ids}")
    print(f"distinct_regions           = {len({r['region_id'] for r in rows})}")
    print(f"history_versions (1 create + 10 inserts) = {1 + 10}")

    # --- Per region ---------------------------------------------------------
    per_region = defaultdict(int)
    region_name = {}
    for r in rows:
        rid = int(r["region_id"])
        per_region[rid] += 1
        region_name[rid] = r["region_name"]
    print("\n-- per region (region_id : name : count) --")
    for rid in sorted(per_region):
        print(f"  region {rid:>2} : {region_name[rid]:<13} : {per_region[rid]}")

    # --- Call type ----------------------------------------------------------
    by_type = defaultdict(int)
    dur_by_type = defaultdict(int)
    bytes_by_type = defaultdict(int)
    charge_by_type = defaultdict(float)
    for r in rows:
        t = r["call_type"]
        by_type[t] += 1
        dur_by_type[t] += int(r["duration_seconds"])
        if not is_blank(r["bytes_transferred"]):
            bytes_by_type[t] += int(r["bytes_transferred"])
        charge_by_type[t] += float(r["charge_amount"])
    print("\n-- by call_type (count / sum_duration / sum_bytes / sum_charge) --")
    for t in ("VOICE", "SMS", "DATA"):
        print(f"  {t:<5} count={by_type[t]:>6}  dur={dur_by_type[t]:>10}  "
              f"bytes={bytes_by_type[t]:>14}  charge={round(charge_by_type[t], 2)}")

    # --- Network type -------------------------------------------------------
    by_net = defaultdict(int)
    for r in rows:
        by_net[r["network_type"]] += 1
    print("\n-- by network_type --")
    for net in ("3G", "4G", "5G"):
        print(f"  {net} : {by_net[net]}")

    # --- Aggregates ---------------------------------------------------------
    total_duration = sum(int(r["duration_seconds"]) for r in rows)
    total_bytes = sum(int(r["bytes_transferred"]) for r in rows if not is_blank(r["bytes_transferred"]))
    total_charge = round(sum(float(r["charge_amount"]) for r in rows), 2)
    print("\n-- overall aggregates --")
    print(f"sum_duration_seconds       = {total_duration}")
    print(f"sum_bytes_transferred      = {total_bytes}")
    print(f"sum_charge_amount (approx) = {total_charge}")
    print(f"  charge BETWEEN bound     = {total_charge - 1.0} AND {total_charge + 1.0}")

    # --- NULL handling ------------------------------------------------------
    callee_null = sum(1 for r in rows if is_blank(r["callee_number"]))
    bytes_null = sum(1 for r in rows if is_blank(r["bytes_transferred"]))
    callee_present = sum(1 for r in rows if not is_blank(r["callee_number"]))
    print("\n-- NULL handling --")
    print(f"callee_number NULL (DATA sessions)        = {callee_null}")
    print(f"callee_number present (VOICE+SMS)          = {callee_present}")
    print(f"bytes_transferred NULL (VOICE+SMS)        = {bytes_null}")
    print(f"bytes_transferred present (DATA)           = {by_type['DATA']}")

    # --- Edge cases ---------------------------------------------------------
    dropped = sum(1 for r in rows if r["call_type"] == "VOICE" and int(r["duration_seconds"]) == 0)
    zero_charge = sum(1 for r in rows if float(r["charge_amount"]) == 0.0)
    print("\n-- edge cases --")
    print(f"dropped VOICE calls (duration=0)          = {dropped}")
    print(f"zero-charge rows (dropped voice)          = {zero_charge}")

    # --- Cross-checks -------------------------------------------------------
    print("\n-- cross-checks --")
    print(f"callee_null == DATA count                 : {callee_null == by_type['DATA']}")
    print(f"bytes_null  == VOICE+SMS count            : {bytes_null == by_type['VOICE'] + by_type['SMS']}")
    print(f"sum(per_region) == total_rows             : {sum(per_region.values()) == n}")
    print("=" * 68)


if __name__ == "__main__":
    main()
