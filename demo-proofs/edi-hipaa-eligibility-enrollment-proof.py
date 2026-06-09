#!/usr/bin/env python3
"""
Precompute ALL proof values for edi-hipaa-eligibility-enrollment demo.

This demo has 3 files:
  - hipaa_270_eligibility_request.edi (ST*270)
  - hipaa_271_eligibility_response.edi (ST*271)
  - hipaa_834_benefit_enrollment.edi (ST*834)

Two tables:
  1. eligibility_messages — ISA/GS/ST + materialized: bht_1, bht_2, nm1_1, nm1_2, nm1_3,
                            trn_1, trn_2, eq_1, dmg_1, dmg_2
  2. enrollment_details   — ISA/GS/ST + materialized: bgn_1, bgn_2, ins_1, ins_7,
                            nm1_1, nm1_2, nm1_3, hd_1, hd_3, dtp_1, dtp_3, cob_1
"""

import os
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__),
    "../delta-forge-demos/demos/edi/edi-hipaa-eligibility-enrollment/data")

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
print("EDI HIPAA Eligibility & Enrollment — Proof Values")
print("=" * 80)

# ─── TABLE 1: eligibility_messages (3 rows) ───
print("\n### TABLE: eligibility_messages (3 rows)")
print("Materialized: bht_1, bht_2, nm1_1, nm1_2, nm1_3, trn_1, trn_2, eq_1, dmg_1, dmg_2\n")

elig_rows = []
for fname, segs in sorted(files.items()):
    st = get_first(segs, 'ST')
    gs = get_first(segs, 'GS')
    bht = get_first(segs, 'BHT')
    nm1 = get_first(segs, 'NM1')  # first NM1
    trn = get_first(segs, 'TRN')
    eq = get_first(segs, 'EQ')
    dmg = get_first(segs, 'DMG')
    bgn = get_first(segs, 'BGN')

    row = {
        'df_file_name': fname,
        'st_1': elem(st, 0),
        'gs_1': elem(gs, 0),
        'bht_1': elem(bht, 0) if bht else None,
        'bht_2': elem(bht, 1) if bht else None,
        'nm1_1': elem(nm1, 0) if nm1 else None,
        'nm1_2': elem(nm1, 1) if nm1 else None,
        'nm1_3': elem(nm1, 2) if nm1 else None,
        'trn_1': elem(trn, 0) if trn else None,
        'trn_2': elem(trn, 1) if trn else None,
        'eq_1': elem(eq, 0) if eq else None,
        'dmg_1': elem(dmg, 0) if dmg else None,
        'dmg_2': elem(dmg, 1) if dmg else None,
    }
    elig_rows.append(row)
    print(f"  {fname}:")
    print(f"    ST_1={row['st_1']} GS_1={row['gs_1']} BHT_1={row['bht_1']} BHT_2={row['bht_2']}")
    print(f"    NM1_1={row['nm1_1']} NM1_2={row['nm1_2']} NM1_3={row['nm1_3']}")
    print(f"    TRN_1={row['trn_1']} TRN_2={row['trn_2']} EQ_1={row['eq_1']}")
    print(f"    DMG_1={row['dmg_1']} DMG_2={row['dmg_2']}")

# ─── Query 1: All Transactions Overview ───
print("\n" + "=" * 80)
print("Query 1: All Transactions Overview")
print("=" * 80)
print(f"ROW_COUNT = {len(elig_rows)}")
for r in elig_rows:
    print(f"  {r['df_file_name']}: st_1={r['st_1']}, gs_1={r['gs_1']}")

# ─── Query 2: Functional Group Split ───
print("\n" + "=" * 80)
print("Query 2: Functional Group Split (HC vs BE)")
print("=" * 80)
gs_counts = defaultdict(int)
for r in elig_rows:
    gs_counts[r['gs_1']] += 1
for gs, cnt in sorted(gs_counts.items()):
    print(f"  GS_1={gs}: count={cnt}")
print(f"ROW_COUNT = {len(gs_counts)}")

# ─── Query 3: Eligibility Request/Response Pair (CTE demo) ───
print("\n" + "=" * 80)
print("Query 3: Eligibility Request/Response Pair — CTE")
print("=" * 80)
# 270 is the request, 271 is the response — match by TRN_2
row_270 = next((r for r in elig_rows if r['st_1'] == '270'), None)
row_271 = next((r for r in elig_rows if r['st_1'] == '271'), None)
if row_270 and row_271:
    print(f"  Request (270): TRN_2={row_270['trn_2']}, NM1_3={row_270['nm1_3']}")
    print(f"  Response (271): TRN_2={row_271['trn_2']}, NM1_3={row_271['nm1_3']}")
    # Check if TRN_2 matches (it should for paired transactions)
    match = row_270['trn_2'] == row_271['trn_2']
    print(f"  TRN_2 match: {match} ({row_270['trn_2']} vs {row_271['trn_2']})")
print("ROW_COUNT = 1 (joined pair)")

# ─── Query 4: Patient Demographics ───
print("\n" + "=" * 80)
print("Query 4: Patient Demographics — DMG segment")
print("=" * 80)
dmg_rows = [r for r in elig_rows if r['dmg_2'] is not None]
for r in dmg_rows:
    # DMG_2 is the date of birth
    print(f"  {r['df_file_name']}: dmg_1={r['dmg_1']}, dmg_2(dob)={r['dmg_2']}")
print(f"ROW_COUNT = {len(dmg_rows)}")

# ─── TABLE 2: enrollment_details (3 rows, but HD/BGN/INS only in 834) ───
print("\n" + "=" * 80)
print("TABLE: enrollment_details (3 rows)")
print("Materialized: bgn_1, bgn_2, ins_1, ins_7, nm1_1, nm1_2, nm1_3, hd_1, hd_3, dtp_1, dtp_3, cob_1")
print("=" * 80)

enroll_rows = []
for fname, segs in sorted(files.items()):
    st = get_first(segs, 'ST')
    bgn = get_first(segs, 'BGN')
    ins = get_first(segs, 'INS')
    nm1 = get_first(segs, 'NM1')
    hd = get_first(segs, 'HD')
    dtp = get_first(segs, 'DTP')
    cob = get_first(segs, 'COB')

    row = {
        'df_file_name': fname,
        'st_1': elem(st, 0),
        'bgn_1': elem(bgn, 0) if bgn else None,
        'bgn_2': elem(bgn, 1) if bgn else None,
        'ins_1': elem(ins, 0) if ins else None,
        'ins_7': elem(ins, 6) if ins and len(ins) > 6 else None,
        'nm1_1': elem(nm1, 0) if nm1 else None,
        'nm1_2': elem(nm1, 1) if nm1 else None,
        'nm1_3': elem(nm1, 2) if nm1 else None,
        'hd_1': elem(hd, 0) if hd else None,
        'hd_3': elem(hd, 2) if hd and len(hd) > 2 else None,
        'dtp_1': elem(dtp, 0) if dtp else None,
        'dtp_3': elem(dtp, 2) if dtp and len(dtp) > 2 else None,
        'cob_1': elem(cob, 0) if cob else None,
    }
    enroll_rows.append(row)
    print(f"  {fname}:")
    print(f"    BGN_1={row['bgn_1']} BGN_2={row['bgn_2']}")
    print(f"    INS_1={row['ins_1']} INS_7={row['ins_7']}")
    print(f"    NM1_1={row['nm1_1']} NM1_2={row['nm1_2']} NM1_3={row['nm1_3']}")
    print(f"    HD_1={row['hd_1']} HD_3={row['hd_3']}")
    print(f"    DTP_1={row['dtp_1']} DTP_3={row['dtp_3']}")
    print(f"    COB_1={row['cob_1']}")

# ─── Query 5: Enrollment Detail (834 only) ───
print("\n" + "=" * 80)
print("Query 5: Enrollment Detail — WHERE bgn_1 IS NOT NULL (834 only)")
print("=" * 80)
enroll_834 = [r for r in enroll_rows if r['bgn_1'] is not None]
for r in enroll_834:
    print(f"  {r['df_file_name']}: purpose={r['bgn_1']}, ref={r['bgn_2']}, ins={r['ins_1']}, employment={r['ins_7']}")
print(f"ROW_COUNT = {len(enroll_834)}")

# ─── Query 6: Plan Elections (HD segments in 834) ───
print("\n" + "=" * 80)
print("Query 6: Plan Elections — HD segments in 834 file")
print("=" * 80)
# The 834 file has multiple HD segments for different plan types
segs_834 = files.get('hipaa_834_benefit_enrollment.edi', [])
hd_segments = get_all(segs_834, 'HD')
print(f"  HD segments in 834: {len(hd_segments)}")
for i, hd in enumerate(hd_segments):
    plan_code = elem(hd, 0)
    plan_type = elem(hd, 2) if len(hd) > 2 else None
    print(f"    HD[{i+1}]: code={plan_code}, type={plan_type}")

# Note: Since materialized_paths captures only the FIRST HD, the table will show
# hd_1=021, hd_3=HLT for the 834 row. To show all 3 plans, we'd need the JSON.
# But we can still count plan types from the enrollment_details table.
print(f"\n  First HD (materialized): hd_1={enroll_834[0]['hd_1']}, hd_3={enroll_834[0]['hd_3']}")

# ─── Query 7: Coordination of Benefits ───
print("\n" + "=" * 80)
print("Query 7: Coordination of Benefits — COB segment")
print("=" * 80)
cob_rows = [r for r in enroll_rows if r['cob_1'] is not None]
for r in cob_rows:
    print(f"  {r['df_file_name']}: cob_1={r['cob_1']} (P=Primary)")
print(f"ROW_COUNT = {len(cob_rows)}")

# ─── Query 8: Cross-Table Analysis (JOIN eligibility_messages + enrollment_details) ───
print("\n" + "=" * 80)
print("Query 8: Cross-Table JOIN — eligibility_messages + enrollment_details")
print("=" * 80)
print("JOIN on df_file_name — combining eligibility BHT data with enrollment HD data")
for e_row in elig_rows:
    en_row = next((r for r in enroll_rows if r['df_file_name'] == e_row['df_file_name']), None)
    bht_purpose = e_row['bht_2'] if e_row['bht_2'] else 'NULL'
    has_enrollment = 'Yes' if en_row and en_row['bgn_1'] else 'No'
    has_eligibility = 'Yes' if e_row['bht_1'] else 'No'
    print(f"  {e_row['df_file_name']}: st_1={e_row['st_1']}, has_elig={has_eligibility}, has_enroll={has_enrollment}")
print(f"ROW_COUNT = {len(elig_rows)}")

# ─── VERIFY: All Checks ───
print("\n" + "=" * 80)
print("VERIFY: All Checks")
print("=" * 80)
total = len(elig_rows)
elig_pair = 1 if row_270 and row_271 else 0
enroll_count = len(enroll_834)
hc_count = gs_counts.get('HC', 0)
be_count = gs_counts.get('BE', 0)
print(f"  total_files_3: {'PASS' if total == 3 else 'FAIL'} (actual: {total})")
print(f"  eligibility_pair: {'PASS' if elig_pair == 1 else 'FAIL'}")
print(f"  enrollment_record: {'PASS' if enroll_count == 1 else 'FAIL'} (actual: {enroll_count})")
print(f"  hc_transactions: {'PASS' if hc_count == 2 else 'FAIL'} (actual: {hc_count})")
print(f"  be_transactions: {'PASS' if be_count == 1 else 'FAIL'} (actual: {be_count})")

print("\n" + "=" * 80)
print("DONE")
print("=" * 80)
