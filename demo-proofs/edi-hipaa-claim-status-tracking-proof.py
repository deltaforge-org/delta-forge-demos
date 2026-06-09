#!/usr/bin/env python3
"""
Precompute ALL proof values for edi-hipaa-claim-status-tracking demo.

This demo has 3 files:
  - hipaa_276_claim_status_request.edi (ST*276) — 3 patient inquiries in one transaction
  - hipaa_277_claim_status_response.edi (ST*277) — 3 patient responses in one transaction
  - hipaa_278_services_review.edi (ST*278) — 1 prior authorization review

Two tables:
  1. status_messages — ISA/GS/ST + materialized: bht_1, bht_2, nm1_1, nm1_2, nm1_3,
                       trn_1, trn_2, stc_1, stc_4, amt_1, amt_2, dtp_1, dtp_3
  2. status_details  — ISA/GS/ST + materialized: bht_1, bht_2, nm1_1, nm1_2, nm1_3,
                       svc_1, svc_2, svc_3, um_1, um_2, hi_1, ref_1, ref_2
"""

import os
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__),
    "../delta-forge-demos/demos/edi/edi-hipaa-claim-status-tracking/data")

def parse_x12(filepath):
    with open(filepath, 'r') as f:
        content = f.read().strip()
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
    for t, elems in segments:
        if t == tag:
            return elems
    return None

def get_all(segments, tag):
    return [elems for t, elems in segments if t == tag]

def elem(elems, idx):
    if elems and idx < len(elems):
        return elems[idx]
    return None

files = {}
for fname in sorted(os.listdir(DATA_DIR)):
    if fname.endswith('.edi'):
        path = os.path.join(DATA_DIR, fname)
        files[fname] = parse_x12(path)

print("=" * 80)
print("EDI HIPAA Claim Status Tracking — Proof Values")
print("=" * 80)

# ─── TABLE 1: status_messages (3 rows) ───
print("\n### TABLE: status_messages (3 rows)")
print("Materialized: bht_1, bht_2, nm1_1, nm1_2, nm1_3, trn_1, trn_2, stc_1, stc_4, amt_1, amt_2, dtp_1, dtp_3\n")

status_rows = []
for fname, segs in sorted(files.items()):
    st = get_first(segs, 'ST')
    gs = get_first(segs, 'GS')
    bht = get_first(segs, 'BHT')
    nm1 = get_first(segs, 'NM1')
    trn = get_first(segs, 'TRN')
    stc = get_first(segs, 'STC')
    amt = get_first(segs, 'AMT')
    dtp = get_first(segs, 'DTP')

    row = {
        'df_file_name': fname,
        'st_1': elem(st, 0),
        'gs_8': elem(gs, 7) if gs and len(gs) > 7 else None,
        'bht_1': elem(bht, 0) if bht else None,
        'bht_2': elem(bht, 1) if bht else None,
        'nm1_1': elem(nm1, 0) if nm1 else None,
        'nm1_2': elem(nm1, 1) if nm1 else None,
        'nm1_3': elem(nm1, 2) if nm1 else None,
        'trn_1': elem(trn, 0) if trn else None,
        'trn_2': elem(trn, 1) if trn else None,
        'stc_1': elem(stc, 0) if stc else None,
        'stc_4': elem(stc, 3) if stc and len(stc) > 3 else None,
        'amt_1': elem(amt, 0) if amt else None,
        'amt_2': elem(amt, 1) if amt else None,
        'dtp_1': elem(dtp, 0) if dtp else None,
        'dtp_3': elem(dtp, 2) if dtp and len(dtp) > 2 else None,
    }
    status_rows.append(row)
    print(f"  {fname}:")
    print(f"    ST_1={row['st_1']} GS_8={row['gs_8']}")
    print(f"    BHT_1={row['bht_1']} BHT_2={row['bht_2']}")
    print(f"    NM1_1={row['nm1_1']} NM1_2={row['nm1_2']} NM1_3={row['nm1_3']}")
    print(f"    TRN_1={row['trn_1']} TRN_2={row['trn_2']}")
    print(f"    STC_1={row['stc_1']} STC_4={row['stc_4']}")
    print(f"    AMT_1={row['amt_1']} AMT_2={row['amt_2']}")
    print(f"    DTP_1={row['dtp_1']} DTP_3={row['dtp_3']}")

# ─── Query 1: All Status Transactions ───
print("\n" + "=" * 80)
print("Query 1: All Status Transactions Overview")
print("=" * 80)
print(f"ROW_COUNT = {len(status_rows)}")
for r in status_rows:
    print(f"  {r['df_file_name']}: st_1={r['st_1']}, bht_2={r['bht_2']}, nm1_3={r['nm1_3']}")

# ─── Query 2: Transaction Type Classification ───
print("\n" + "=" * 80)
print("Query 2: Transaction Type Classification (CASE WHEN)")
print("=" * 80)
type_counts = defaultdict(int)
for r in status_rows:
    type_counts[r['st_1']] += 1
for st1, cnt in sorted(type_counts.items()):
    label = {'276': 'Claim Status Request', '277': 'Claim Status Response',
             '278': 'Health Services Review'}.get(st1, 'Unknown')
    print(f"  ST_1={st1} ({label}): count={cnt}")
print(f"ROW_COUNT = {len(type_counts)}")

# ─── Query 3: Request/Response Pair (276 → 277) ───
print("\n" + "=" * 80)
print("Query 3: Request/Response Pair — 276 → 277 matching")
print("=" * 80)
row_276 = next((r for r in status_rows if r['st_1'] == '276'), None)
row_277 = next((r for r in status_rows if r['st_1'] == '277'), None)
if row_276 and row_277:
    print(f"  Request (276): nm1_3={row_276['nm1_3']}, amt_2={row_276['amt_2']}")
    print(f"  Response (277): nm1_3={row_277['nm1_3']}, stc_1={row_277['stc_1']}")

# ─── TABLE 2: status_details (3 rows) ───
print("\n" + "=" * 80)
print("TABLE: status_details (3 rows)")
print("Materialized: bht_1, bht_2, nm1_1, nm1_2, nm1_3, svc_1, svc_2, svc_3, um_1, um_2, hi_1, ref_1, ref_2")
print("=" * 80)

detail_rows = []
for fname, segs in sorted(files.items()):
    st = get_first(segs, 'ST')
    bht = get_first(segs, 'BHT')
    nm1 = get_first(segs, 'NM1')
    svc = get_first(segs, 'SVC')
    um = get_first(segs, 'UM')
    hi = get_first(segs, 'HI')
    ref = get_first(segs, 'REF')

    row = {
        'df_file_name': fname,
        'st_1': elem(st, 0),
        'bht_1': elem(bht, 0) if bht else None,
        'bht_2': elem(bht, 1) if bht else None,
        'nm1_1': elem(nm1, 0) if nm1 else None,
        'nm1_2': elem(nm1, 1) if nm1 else None,
        'nm1_3': elem(nm1, 2) if nm1 else None,
        'svc_1': elem(svc, 0) if svc else None,
        'svc_2': elem(svc, 1) if svc else None,
        'svc_3': elem(svc, 2) if svc and len(svc) > 2 else None,
        'um_1': elem(um, 0) if um else None,
        'um_2': elem(um, 1) if um else None,
        'hi_1': elem(hi, 0) if hi else None,
        'ref_1': elem(ref, 0) if ref else None,
        'ref_2': elem(ref, 1) if ref else None,
    }
    detail_rows.append(row)
    print(f"  {fname}:")
    print(f"    BHT_1={row['bht_1']} BHT_2={row['bht_2']}")
    print(f"    NM1_1={row['nm1_1']} NM1_2={row['nm1_2']} NM1_3={row['nm1_3']}")
    print(f"    SVC_1={row['svc_1']} SVC_2={row['svc_2']} SVC_3={row['svc_3']}")
    print(f"    UM_1={row['um_1']} UM_2={row['um_2']} HI_1={row['hi_1']}")
    print(f"    REF_1={row['ref_1']} REF_2={row['ref_2']}")

# ─── Query 4: Claim Status Codes (277 only — STC segment) ───
print("\n" + "=" * 80)
print("Query 4: Claim Status Codes — STC from 277 response")
print("=" * 80)
stc_rows = [r for r in status_rows if r['stc_1'] is not None]
for r in stc_rows:
    print(f"  {r['df_file_name']}: stc_1={r['stc_1']}, stc_4(amount)={r['stc_4']}")
print(f"ROW_COUNT = {len(stc_rows)}")

# ─── Query 5: Claim Amounts at Stake ───
print("\n" + "=" * 80)
print("Query 5: Claim Amounts at Stake — AMT from 276 request")
print("=" * 80)
amt_rows = [r for r in status_rows if r['amt_2'] is not None]
for r in amt_rows:
    print(f"  {r['df_file_name']}: amt_1={r['amt_1']}, amt_2={r['amt_2']}")
print(f"ROW_COUNT = {len(amt_rows)}")
if amt_rows:
    total_amt = float(amt_rows[0]['amt_2'])
    print(f"  First AMT value: {total_amt}")

# ─── Query 6: Prior Authorization Review (278 only) ───
print("\n" + "=" * 80)
print("Query 6: Prior Authorization — UM + HI from 278")
print("=" * 80)
auth_rows = [r for r in detail_rows if r['um_1'] is not None]
for r in auth_rows:
    print(f"  {r['df_file_name']}: um_1={r['um_1']}, um_2={r['um_2']}, hi_1={r['hi_1']}")
print(f"ROW_COUNT = {len(auth_rows)}")

# ─── Query 7: Service Detail from Responses ───
print("\n" + "=" * 80)
print("Query 7: Service Detail — SVC from 277 response")
print("=" * 80)
svc_rows = [r for r in detail_rows if r['svc_1'] is not None]
for r in svc_rows:
    print(f"  {r['df_file_name']}: svc_1={r['svc_1']}, svc_2(charged)={r['svc_2']}, svc_3(paid)={r['svc_3']}")
print(f"ROW_COUNT = {len(svc_rows)}")

# ─── Query 8: Cross-Table JOIN status_messages + status_details ───
print("\n" + "=" * 80)
print("Query 8: Cross-Table JOIN — status_messages + status_details")
print("=" * 80)
for s_row in status_rows:
    d_row = next((r for r in detail_rows if r['df_file_name'] == s_row['df_file_name']), None)
    has_stc = 'Yes' if s_row['stc_1'] else 'No'
    has_um = 'Yes' if d_row and d_row['um_1'] else 'No'
    has_svc = 'Yes' if d_row and d_row['svc_1'] else 'No'
    print(f"  {s_row['df_file_name']}: st_1={s_row['st_1']}, has_stc={has_stc}, has_um={has_um}, has_svc={has_svc}")
print(f"ROW_COUNT = {len(status_rows)}")

# ─── VERIFY: All Checks ───
print("\n" + "=" * 80)
print("VERIFY: All Checks")
print("=" * 80)
total = len(status_rows)
type_count = len(type_counts)
stc_count = len(stc_rows)
auth_count = len(auth_rows)
print(f"  total_files_3: {'PASS' if total == 3 else 'FAIL'} (actual: {total})")
print(f"  transaction_types_3: {'PASS' if type_count == 3 else 'FAIL'} (actual: {type_count})")
print(f"  status_response: {'PASS' if stc_count == 1 else 'FAIL'} (actual: {stc_count})")
print(f"  auth_review: {'PASS' if auth_count == 1 else 'FAIL'} (actual: {auth_count})")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
