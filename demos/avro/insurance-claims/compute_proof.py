#!/usr/bin/env python3
"""Read back generated Avro files and compute all assertion values."""

import os
import fastavro
from collections import Counter

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

files = {
    "claims_auto_v1.avro": [],
    "claims_home_v1.avro": [],
    "claims_auto_v2.avro": [],
}

for fname in files:
    path = os.path.join(DATA_DIR, fname)
    with open(path, "rb") as f:
        reader = fastavro.reader(f)
        for record in reader:
            files[fname].append(record)

all_records = []
for fname, recs in files.items():
    all_records.extend(recs)

print(f"=== FILE COUNTS ===")
for fname, recs in files.items():
    print(f"  {fname}: {len(recs)} rows")
print(f"  TOTAL: {len(all_records)} rows")

print(f"\n=== QUERY 1: Full scan ===")
print(f"  ROW_COUNT = {len(all_records)}")

print(f"\n=== QUERY 2: Schema evolution — NULL counts in adjuster_name/settlement_date ===")
# v1 files don't have adjuster_name/settlement_date -> they'll be NULL when merged
v1_records = files["claims_auto_v1.avro"] + files["claims_home_v1.avro"]
v2_records = files["claims_auto_v2.avro"]

null_adjuster_from_v1 = len(v1_records)  # All v1 records have NULL adjuster
null_adjuster_from_v2 = sum(1 for r in v2_records if r.get("adjuster_name") is None)
total_null_adjuster = null_adjuster_from_v1 + null_adjuster_from_v2
print(f"  null_adjuster_from_v1 = {null_adjuster_from_v1}")
print(f"  null_adjuster_from_v2 = {null_adjuster_from_v2}")
print(f"  total_null_adjuster = {total_null_adjuster}")

null_settlement_from_v1 = len(v1_records)
null_settlement_from_v2 = sum(1 for r in v2_records if r.get("settlement_date") is None)
total_null_settlement = null_settlement_from_v1 + null_settlement_from_v2
print(f"  null_settlement_from_v1 = {null_settlement_from_v1}")
print(f"  null_settlement_from_v2 = {null_settlement_from_v2}")
print(f"  total_null_settlement = {total_null_settlement}")

print(f"\n=== QUERY 3: Auto claims filter (files matching *auto*) ===")
auto_records = files["claims_auto_v1.avro"] + files["claims_auto_v2.avro"]
print(f"  ROW_COUNT = {len(auto_records)}")

print(f"\n=== QUERY 4: Claim type breakdown ===")
type_counts = Counter()
type_amounts = {}
for r in all_records:
    ct = r["claim_type"]
    type_counts[ct] += 1
    type_amounts.setdefault(ct, []).append(r["amount_claimed"])

print(f"  ROW_COUNT = {len(type_counts)}")
for ct in sorted(type_counts.keys()):
    avg = sum(type_amounts[ct]) / len(type_amounts[ct])
    print(f"  {ct}: count={type_counts[ct]}, avg_claimed={avg:.2f}")

print(f"\n=== QUERY 5: Status distribution ===")
status_counts = Counter()
status_approved = {}
for r in all_records:
    s = r["status"]
    status_counts[s] += 1
    status_approved.setdefault(s, 0.0)
    status_approved[s] += r["amount_approved"]

print(f"  ROW_COUNT = {len(status_counts)}")
for s in sorted(status_counts.keys()):
    print(f"  {s}: count={status_counts[s]}, sum_approved={status_approved[s]:.2f}")

print(f"\n=== QUERY 6: Approval rate ===")
approved_count = status_counts.get("Approved", 0)
denied_count = status_counts.get("Denied", 0)
total_decided = approved_count + denied_count
approval_rate = round(approved_count * 100.0 / total_decided, 1) if total_decided > 0 else 0
avg_approved_when_approved = sum(r["amount_approved"] for r in all_records if r["status"] == "Approved") / approved_count if approved_count > 0 else 0
print(f"  approved_count = {approved_count}")
print(f"  denied_count = {denied_count}")
print(f"  approval_rate = {approval_rate}")
print(f"  avg_approved_when_approved = {avg_approved_when_approved:.2f}")

print(f"\n=== QUERY 7: Sampled claims (max_rows=15 per file x 3 files) ===")
sampled = 15 * 3
print(f"  ROW_COUNT = {sampled}")

print(f"\n=== VERIFY: Grand totals ===")
total_rows = len(all_records)
sum_claimed = sum(r["amount_claimed"] for r in all_records)
sum_approved = sum(r["amount_approved"] for r in all_records)
distinct_statuses = len(set(r["status"] for r in all_records))
null_desc_count = sum(1 for r in all_records if r.get("description") is None)
print(f"  total_rows = {total_rows}")
print(f"  sum_claimed = {sum_claimed:.2f}")
print(f"  sum_approved = {sum_approved:.2f}")
print(f"  distinct_statuses = {distinct_statuses}")
print(f"  null_adjuster_count = {total_null_adjuster}")
print(f"  null_description_count = {null_desc_count}")

# File sizes
print(f"\n=== FILE SIZES ===")
total_size = 0
for fname in files:
    path = os.path.join(DATA_DIR, fname)
    sz = os.path.getsize(path)
    total_size += sz
    print(f"  {fname}: {sz} bytes")
print(f"  TOTAL: {total_size} bytes")
