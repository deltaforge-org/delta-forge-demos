#!/usr/bin/env python3
"""
Precompute ALL proof values for edi-hipaa-claims-financial demo.

This demo has 4 files: hipaa_835_claim_payment.edi (ST*837 professional),
hipaa_837D_dental_claim.edi (ST*837 dental), hipaa_837I_institutional_claim.edi
(ST*837 institutional), hipaa_820_payment.edi (ST*835 remittance).

Two tables:
  1. claims_header — ISA/GS/ST + materialized: clm_1, clm_2, sv1_1, sv1_2, sv2_1, sv2_2, sv3_1, sv3_2
  2. claims_remittance — ISA/GS/ST + materialized: bpr_1, bpr_2, clp_1, clp_2, clp_3, cas_1, cas_2, cas_3, svc_1, svc_2, svc_3

Note on file naming vs ST code:
  - hipaa_835_claim_payment.edi contains ST*837 (professional claim)
  - hipaa_820_payment.edi contains ST*835 (remittance advice)
"""

import re
import os
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__),
    "../delta-forge-demos/demos/edi/edi-hipaa-claims-financial/data")

def parse_x12(filepath):
    """Parse an X12 EDI file and return segments as list of (tag, [elements])."""
    with open(filepath, 'r') as f:
        content = f.read().strip()
    # Detect segment terminator (~ in all our files)
    segments = content.split('~')
    result = []
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        parts = seg.split('*')
        tag = parts[0].strip()
        elements = parts[1:] if len(parts) > 1 else []
        result.append((tag, elements))
    return result

def get_first(segments, tag):
    """Get first occurrence of a segment."""
    for t, elems in segments:
        if t == tag:
            return elems
    return None

def get_all(segments, tag):
    """Get all occurrences of a segment."""
    return [elems for t, elems in segments if t == tag]

def elem(elems, idx):
    """Safely get element at 0-based index."""
    if elems and idx < len(elems):
        return elems[idx]
    return None

# Parse all 4 files
files = {}
for fname in sorted(os.listdir(DATA_DIR)):
    if fname.endswith('.edi'):
        path = os.path.join(DATA_DIR, fname)
        files[fname] = parse_x12(path)

print("=" * 80)
print("EDI HIPAA Claims Financial — Proof Values")
print("=" * 80)

# ─── TABLE 1: claims_header ───
# Materialized: clm_1, clm_2, sv1_1, sv1_2, sv2_1, sv2_2, sv3_1, sv3_2
# + default ISA/GS/ST + df_file_name

print("\n### TABLE: claims_header (4 rows, one per file)")
print("Materialized paths: clm_1, clm_2, sv1_1, sv1_2, sv2_1, sv2_2, sv3_1, sv3_2\n")

header_rows = []
for fname, segs in sorted(files.items()):
    st = get_first(segs, 'ST')
    clm = get_first(segs, 'CLM')
    sv1 = get_first(segs, 'SV1')
    sv2 = get_first(segs, 'SV2')
    sv3 = get_first(segs, 'SV3')

    row = {
        'df_file_name': fname,
        'st_1': elem(st, 0),
        'clm_1': elem(clm, 0) if clm else None,
        'clm_2': elem(clm, 1) if clm else None,
        'sv1_1': elem(sv1, 0) if sv1 else None,
        'sv1_2': elem(sv1, 1) if sv1 else None,
        'sv2_1': elem(sv2, 0) if sv2 else None,
        'sv2_2': elem(sv2, 1) if sv2 else None,
        'sv3_1': elem(sv3, 0) if sv3 else None,
        'sv3_2': elem(sv3, 1) if sv3 else None,
    }
    header_rows.append(row)
    print(f"  {fname}:")
    print(f"    ST_1={row['st_1']}  CLM_1={row['clm_1']}  CLM_2={row['clm_2']}")
    print(f"    SV1_1={row['sv1_1']}  SV1_2={row['sv1_2']}")
    print(f"    SV2_1={row['sv2_1']}  SV2_2={row['sv2_2']}")
    print(f"    SV3_1={row['sv3_1']}  SV3_2={row['sv3_2']}")

# ─── Query 1: Claim Overview ───
print("\n" + "=" * 80)
print("Query 1: Claim Overview — all 4 rows")
print("=" * 80)
claim_rows = [r for r in header_rows]
print(f"ROW_COUNT = {len(claim_rows)}")
for r in claim_rows:
    print(f"  {r['df_file_name']}: st_1={r['st_1']}, clm_1={r['clm_1']}, clm_2={r['clm_2']}")

# Count rows where clm_1 IS NOT NULL (these are the actual claim submissions)
claims_with_clm = [r for r in header_rows if r['clm_1'] is not None]
print(f"\nRows with CLM (claim submissions): {len(claims_with_clm)}")

# ─── Query 2: Claim Charge Summary ───
# SELECT with WHERE clm_1 IS NOT NULL (only 837 files have CLM segments)
print("\n" + "=" * 80)
print("Query 2: Claim Charge Summary — only rows with CLM (claim submissions)")
print("=" * 80)
for r in claims_with_clm:
    print(f"  {r['df_file_name']}: claim_id={r['clm_1']}, charge_amount={r['clm_2']}")
print(f"ROW_COUNT = {len(claims_with_clm)}")

# Total charge amount
total_charge = sum(float(r['clm_2']) for r in claims_with_clm)
print(f"SUM(clm_2) total_charges = {total_charge}")

# ─── Query 3: Service Line Detail ───
# Each file may have multiple SV1/SV2/SV3 segments — but materialized_paths only captures FIRST
# So we show the first service line per claim
print("\n" + "=" * 80)
print("Query 3: Service Line Detail (first service per claim)")
print("=" * 80)
for r in claims_with_clm:
    svc_code = r['sv1_1'] or r['sv2_1'] or r['sv3_1'] or 'NULL'
    svc_charge = r['sv1_2'] or r['sv2_2'] or r['sv3_2'] or 'NULL'
    print(f"  {r['df_file_name']}: service_code={svc_code}, service_charge={svc_charge}")
print(f"ROW_COUNT = {len(claims_with_clm)}")

# ─── Query 4: Claim Type Distribution ───
print("\n" + "=" * 80)
print("Query 4: Claim Type Distribution (GROUP BY st_1)")
print("=" * 80)
type_counts = defaultdict(int)
for r in header_rows:
    type_counts[r['st_1']] += 1
for st1, cnt in sorted(type_counts.items()):
    print(f"  ST_1={st1}: count={cnt}")
print(f"ROW_COUNT = {len(type_counts)}")

# ─── TABLE 2: claims_remittance ───
print("\n" + "=" * 80)
print("TABLE: claims_remittance (4 rows)")
print("Materialized: bpr_1, bpr_2, clp_1, clp_2, clp_3, cas_1, cas_2, cas_3, svc_1, svc_2, svc_3")
print("=" * 80)

remit_rows = []
for fname, segs in sorted(files.items()):
    st = get_first(segs, 'ST')
    bpr = get_first(segs, 'BPR')
    clp = get_first(segs, 'CLP')
    cas = get_first(segs, 'CAS')
    svc = get_first(segs, 'SVC')

    row = {
        'df_file_name': fname,
        'st_1': elem(st, 0),
        'bpr_1': elem(bpr, 0) if bpr else None,
        'bpr_2': elem(bpr, 1) if bpr else None,
        'clp_1': elem(clp, 0) if clp else None,
        'clp_2': elem(clp, 1) if clp else None,
        'clp_3': elem(clp, 2) if clp else None,
        'cas_1': elem(cas, 0) if cas else None,
        'cas_2': elem(cas, 1) if cas else None,
        'cas_3': elem(cas, 2) if cas else None,
        'svc_1': elem(svc, 0) if svc else None,
        'svc_2': elem(svc, 1) if svc else None,
        'svc_3': elem(svc, 2) if svc else None,
    }
    remit_rows.append(row)
    print(f"  {fname}:")
    print(f"    BPR_1={row['bpr_1']} BPR_2={row['bpr_2']}")
    print(f"    CLP_1={row['clp_1']} CLP_2={row['clp_2']} CLP_3={row['clp_3']}")
    print(f"    CAS_1={row['cas_1']} CAS_2={row['cas_2']} CAS_3={row['cas_3']}")
    print(f"    SVC_1={row['svc_1']} SVC_2={row['svc_2']} SVC_3={row['svc_3']}")

# ─── Query 5: Payment & Remittance Overview ───
print("\n" + "=" * 80)
print("Query 5: Payment & Remittance — rows with BPR (payment info)")
print("=" * 80)
payment_rows = [r for r in remit_rows if r['bpr_1'] is not None]
for r in payment_rows:
    print(f"  {r['df_file_name']}: handling={r['bpr_1']}, amount={r['bpr_2']}")
print(f"ROW_COUNT = {len(payment_rows)}")

# ─── Query 6: Remittance Claim Detail ───
print("\n" + "=" * 80)
print("Query 6: Remittance Claim Detail — rows with CLP (claim-level payment)")
print("=" * 80)
clp_rows = [r for r in remit_rows if r['clp_1'] is not None]
for r in clp_rows:
    print(f"  {r['df_file_name']}: claim_ref={r['clp_1']}, status={r['clp_2']}, charged={r['clp_3']}")
print(f"ROW_COUNT = {len(clp_rows)}")

# ─── Query 7: Adjustment Analysis ───
print("\n" + "=" * 80)
print("Query 7: Adjustment Analysis — rows with CAS (claim adjustments)")
print("=" * 80)
cas_rows = [r for r in remit_rows if r['cas_1'] is not None]
for r in cas_rows:
    print(f"  {r['df_file_name']}: group={r['cas_1']}, reason={r['cas_2']}, amount={r['cas_3']}")
print(f"ROW_COUNT = {len(cas_rows)}")

# ─── Query 8: Financial Summary (JOIN both tables) ───
print("\n" + "=" * 80)
print("Query 8: Financial Summary — JOIN claims_header + claims_remittance")
print("=" * 80)
print("JOIN on df_file_name, combining claim charges with payment amounts")
for h_row in header_rows:
    r_row = next((r for r in remit_rows if r['df_file_name'] == h_row['df_file_name']), None)
    charge = h_row['clm_2'] if h_row['clm_2'] else 'NULL'
    payment = r_row['bpr_2'] if r_row and r_row['bpr_2'] else 'NULL'
    print(f"  {h_row['df_file_name']}: charge={charge}, payment={payment}")
print(f"ROW_COUNT = {len(header_rows)}")

# Totals for rows that have both
both_rows = [(h, r) for h, r in zip(header_rows, remit_rows)
             if h['clm_2'] is not None and r['bpr_2'] is not None]
if both_rows:
    total_charged = sum(float(h['clm_2']) for h, r in both_rows)
    total_paid = sum(float(r['bpr_2']) for h, r in both_rows)
    print(f"\nRows with both charge and payment: {len(both_rows)}")
    print(f"Total charged: {total_charged}")
    print(f"Total paid: {total_paid}")

# ─── Query 9: Charge vs Allowed (CAST + math) ───
print("\n" + "=" * 80)
print("Query 9: Charge vs Allowed — CAST amounts for write-off calculation")
print("=" * 80)
for r in clp_rows:
    charged = float(r['clp_3'])
    paid_row = next((p for p in payment_rows if p['df_file_name'] == r['df_file_name']), None)
    paid = float(paid_row['bpr_2']) if paid_row else 0
    writeoff = charged - paid
    pct = round((writeoff / charged) * 100, 1) if charged > 0 else 0
    print(f"  {r['df_file_name']}: charged={charged}, paid={paid}, writeoff={writeoff}, writeoff_pct={pct}%")

# ─── VERIFY: All Checks ───
print("\n" + "=" * 80)
print("VERIFY: All Checks")
print("=" * 80)
total_rows = len(header_rows)
claim_count = len(claims_with_clm)
payment_count = len(payment_rows)
print(f"  total_files_4: {'PASS' if total_rows == 4 else 'FAIL'} (actual: {total_rows})")
print(f"  claims_with_charges: {'PASS' if claim_count == 3 else 'FAIL'} (actual: {claim_count})")
print(f"  payment_records: {'PASS' if payment_count == 1 else 'FAIL'} (actual: {payment_count})")
# All claim amounts > 0
all_positive = all(float(r['clm_2']) > 0 for r in claims_with_clm)
print(f"  charges_positive: {'PASS' if all_positive else 'FAIL'}")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
