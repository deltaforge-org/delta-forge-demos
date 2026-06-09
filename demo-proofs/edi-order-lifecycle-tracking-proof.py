#!/usr/bin/env python3
"""Precompute ALL proof values for edi-order-lifecycle-tracking demo.

This demo has 14 X12 EDI files (supply chain: 850, 810, 855, 856, 857, 861, 997, 824).
One table: lifecycle_tracking — ISA/GS/ST + materialized BEG, BIG, BAK, BSN, BRA, BGN,
N1, CTT, REF fields.

Queries demonstrate cross-document lifecycle correlation: PO (850) → Ack (855)
→ Ship (856/857) → Invoice (810) → Receipt (861).
"""

import os
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__),
    "../delta-forge-demos/demos/edi/edi-order-lifecycle-tracking/data")


def parse_x12(filepath):
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        content = f.read().strip()
    segments = []
    for seg in content.split('~'):
        seg = seg.strip()
        if not seg:
            continue
        parts = seg.split('*')
        tag = parts[0].strip()
        elements = parts[1:] if len(parts) > 1 else []
        segments.append((tag, elements))
    return segments


def get_first(segments, tag):
    for t, elems in segments:
        if t == tag:
            return elems
    return None


def elem(elems, idx):
    if elems and idx < len(elems):
        val = elems[idx].strip()
        return val if val else None
    return None


# ── Parse all files ──
files = {}
for fname in sorted(os.listdir(DATA_DIR)):
    if fname.endswith('.edi'):
        path = os.path.join(DATA_DIR, fname)
        files[fname] = parse_x12(path)

print("=" * 80)
print("EDI Order Lifecycle Tracking — Proof Values")
print("=" * 80)
print(f"\nParsed {len(files)} files from {DATA_DIR}\n")

# ── Build rows with materialized fields ──
rows = []
for fname, segs in sorted(files.items()):
    st = get_first(segs, 'ST')
    beg = get_first(segs, 'BEG')
    big = get_first(segs, 'BIG')
    bak = get_first(segs, 'BAK')
    bsn = get_first(segs, 'BSN')
    bra = get_first(segs, 'BRA')
    bgn = get_first(segs, 'BGN')
    n1 = get_first(segs, 'N1')
    ctt = get_first(segs, 'CTT')
    ref = get_first(segs, 'REF')

    row = {
        'df_file_name': fname,
        'st_1': elem(st, 0),
        'beg_1': elem(beg, 0),
        'beg_3': elem(beg, 2),
        'beg_5': elem(beg, 4),
        'big_1': elem(big, 0),
        'big_2': elem(big, 1),
        'bak_1': elem(bak, 0),
        'bak_3': elem(bak, 2),
        'bak_4': elem(bak, 3),
        'bsn_2': elem(bsn, 1),
        'bsn_3': elem(bsn, 2),
        'bra_1': elem(bra, 0),
        'bgn_2': elem(bgn, 1),
        'n1_1': elem(n1, 0),
        'n1_2': elem(n1, 1),
        'ctt_1': elem(ctt, 0),
        'ref_1': elem(ref, 0),
        'ref_2': elem(ref, 1),
    }
    rows.append(row)


def lifecycle_stage(st_1):
    return {
        '850': 'Order', '855': 'Acknowledgment', '856': 'Shipment',
        '857': 'Shipment & Billing', '810': 'Invoice', '861': 'Receipt',
        '997': 'Acknowledgment (Func)', '824': 'Application Advice',
    }.get(st_1, 'Other')


def coalesce(*args):
    for a in args:
        if a is not None and a != '':
            return a
    return None


# ════════════════════════════════════════════════════════════════════════════
# QUERY 1: Lifecycle Overview
# ════════════════════════════════════════════════════════════════════════════
print("=" * 80)
print("QUERY 1: Lifecycle Overview — All Documents by Type")
print("=" * 80)
print(f"ROW_COUNT = {len(rows)}")

stage_order = {'850': 1, '855': 2, '856': 3, '857': 4, '810': 5, '861': 6, '997': 7, '824': 8}
for r in sorted(rows, key=lambda x: (stage_order.get(x['st_1'], 9), x['df_file_name'])):
    stage = lifecycle_stage(r['st_1'])
    doc_id = coalesce(r['beg_3'], r['big_2'], r['bsn_2'], r['bak_3'], r['bra_1'], r['bgn_2'])
    print(f"  {r['df_file_name']:50s}  stage={stage:25s}  document_id={doc_id}")

# Verify assertions
r_850 = next(r for r in rows if r['df_file_name'] == 'x12_850_purchase_order.edi')
assert lifecycle_stage(r_850['st_1']) == 'Order'
assert coalesce(r_850['beg_3'], r_850['big_2']) == '1000012'

r_810e = next(r for r in rows if r['df_file_name'] == 'x12_810_invoice_edifabric.edi')
assert lifecycle_stage(r_810e['st_1']) == 'Invoice'
assert coalesce(r_810e['beg_3'], r_810e['big_2']) == 'SG427254'

r_855 = next(r for r in rows if r['df_file_name'] == 'x12_855_purchase_order_ack.edi')
assert lifecycle_stage(r_855['st_1']) == 'Acknowledgment'
assert coalesce(r_855['beg_3'], r_855['big_2'], r_855['bsn_2'], r_855['bak_3']) == '1234567'

r_856 = next(r for r in rows if r['df_file_name'] == 'x12_856_ship_notice.edi')
assert lifecycle_stage(r_856['st_1']) == 'Shipment'

r_861 = next(r for r in rows if r['df_file_name'] == 'x12_861_receiving_advice.edi')
assert lifecycle_stage(r_861['st_1']) == 'Receipt'
print("  ✓ All Q1 assertions verified")


# ════════════════════════════════════════════════════════════════════════════
# QUERY 2: Lifecycle Stage Counts
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("QUERY 2: Lifecycle Stage Counts")
print("=" * 80)

stage_counts = defaultdict(int)
for r in rows:
    stage_counts[r['st_1']] += 1

distinct_stages = len(stage_counts)
print(f"ROW_COUNT = {distinct_stages}")

for st1, cnt in sorted(stage_counts.items(), key=lambda x: (-x[1], x[0])):
    print(f"  {lifecycle_stage(st1):25s}  (ST_1={st1})  stage_count={cnt}")

assert distinct_stages == 8
assert stage_counts['810'] == 5, f"Expected 5 invoices, got {stage_counts['810']}"
assert stage_counts['850'] == 3, f"Expected 3 orders, got {stage_counts['850']}"
assert stage_counts['855'] == 1
assert stage_counts['856'] == 1
assert stage_counts['857'] == 1
assert stage_counts['861'] == 1
print("  ✓ All Q2 assertions verified")


# ════════════════════════════════════════════════════════════════════════════
# QUERY 3: Purchase Order Detail (850s only)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("QUERY 3: Purchase Order Detail")
print("=" * 80)

po_rows = [r for r in rows if r['st_1'] == '850']
print(f"ROW_COUNT = {len(po_rows)}")

for r in sorted(po_rows, key=lambda x: x['df_file_name']):
    print(f"  {r['df_file_name']:50s}  po_number={r['beg_3']}  po_date={r['beg_5']}  "
          f"party_code={r['n1_1']}  party_name={r['n1_2']}  ref_type={r['ref_1']}  "
          f"ref_value={r['ref_2']}  line_items={r['ctt_1']}")

po_by_file = {r['df_file_name']: r for r in po_rows}
assert po_by_file['x12_850_purchase_order.edi']['beg_3'] == '1000012'
assert po_by_file['x12_850_purchase_order.edi']['beg_5'] == '20090827'
assert po_by_file['x12_850_purchase_order.edi']['n1_2'] == 'John Doe'
assert po_by_file['x12_850_purchase_order_a.edi']['beg_3'] == '4600000406'
assert po_by_file['x12_850_purchase_order_a.edi']['n1_2'] == 'Transplace Laredo'
assert po_by_file['x12_850_purchase_order_edifabric.edi']['beg_3'] == 'XX-1234'
assert po_by_file['x12_850_purchase_order_edifabric.edi']['n1_2'] == 'ABC AEROSPACE'
print("  ✓ All Q3 assertions verified")


# ════════════════════════════════════════════════════════════════════════════
# QUERY 4: Invoice Detail (810s only)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("QUERY 4: Invoice Detail")
print("=" * 80)

inv_rows = [r for r in rows if r['st_1'] == '810']
print(f"ROW_COUNT = {len(inv_rows)}")

for r in sorted(inv_rows, key=lambda x: x['df_file_name']):
    print(f"  {r['df_file_name']:50s}  invoice_number={r['big_2']}  invoice_date={r['big_1']}  "
          f"party_name={r['n1_2']}  ref_type={r['ref_1']}  ref_value={r['ref_2']}  "
          f"line_items={r['ctt_1']}")

inv_by_file = {r['df_file_name']: r for r in inv_rows}
assert inv_by_file['x12_810_invoice_a.edi']['big_2'] == 'DO091003TESTINV01'
assert inv_by_file['x12_810_invoice_a.edi']['big_1'] == '20030310'
assert inv_by_file['x12_810_invoice_a.edi']['n1_2'] == 'Aaron Copeland'
assert inv_by_file['x12_810_invoice_edifabric.edi']['big_2'] == 'SG427254'
assert inv_by_file['x12_810_invoice_edifabric.edi']['big_1'] == '20000513'
assert inv_by_file['x12_810_invoice_edifabric.edi']['n1_2'] == 'ABC AEROSPACE CORPORATION'
print("  ✓ All Q4 assertions verified")


# ════════════════════════════════════════════════════════════════════════════
# QUERY 5: Shipment & Fulfillment (856, 857, 861)
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("QUERY 5: Shipment & Fulfillment")
print("=" * 80)

fulfill_rows = [r for r in rows if r['st_1'] in ('856', '857', '861')]
print(f"ROW_COUNT = {len(fulfill_rows)}")

for r in sorted(fulfill_rows, key=lambda x: stage_order.get(x['st_1'], 9)):
    stage = lifecycle_stage(r['st_1'])
    print(f"  {r['df_file_name']:50s}  txn_type={r['st_1']}  stage={stage}  "
          f"shipment_id={r['bsn_2']}  shipment_date={r['bsn_3']}  receipt_id={r['bra_1']}  "
          f"party_code={r['n1_1']}  party_name={r['n1_2']}")

f_by_file = {r['df_file_name']: r for r in fulfill_rows}
assert f_by_file['x12_856_ship_notice.edi']['bsn_2'] == '01140824'
assert f_by_file['x12_856_ship_notice.edi']['bsn_3'] == '20051015'
assert f_by_file['x12_861_receiving_advice.edi']['bra_1'] == 'C000548241'
print("  ✓ All Q5 assertions verified")


# ════════════════════════════════════════════════════════════════════════════
# QUERY 6: Document Timeline
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("QUERY 6: Document Timeline")
print("=" * 80)

dated_rows = []
for r in rows:
    doc_date = coalesce(r['beg_5'], r['big_1'], r['bsn_3'], r['bak_4'])
    if doc_date is not None:
        dated_rows.append((doc_date, r))

dated_rows.sort(key=lambda x: x[0])
print(f"ROW_COUNT = {len(dated_rows)} (rows with document_date)")

for doc_date, r in dated_rows:
    stage = lifecycle_stage(r['st_1'])
    doc_id = coalesce(r['beg_3'], r['big_2'], r['bsn_2'], r['bak_3'], r['bra_1'], r['bgn_2'])
    print(f"  {doc_date}  {stage:25s}  document_id={doc_id}  file={r['df_file_name']}")

assert len(dated_rows) >= 10, f"Expected >= 10, got {len(dated_rows)}"

# Verify specific dates
dates_by_file = {r['df_file_name']: d for d, r in dated_rows}
assert dates_by_file['x12_850_purchase_order.edi'] == '20090827'
assert dates_by_file['x12_855_purchase_order_ack.edi'] == '20050102'
assert dates_by_file['x12_856_ship_notice.edi'] == '20051015'
assert dates_by_file['x12_810_invoice_edifabric.edi'] == '20000513'
print("  ✓ All Q6 assertions verified")


# ════════════════════════════════════════════════════════════════════════════
# QUERY 7: Trading Partner Activity Across Lifecycle Stages
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("QUERY 7: Trading Partner Activity")
print("=" * 80)

# Group by (n1_2, n1_1) where n1_2 is not null/empty
partner_data = defaultdict(lambda: {'stages': set(), 'count': 0, 'n1_1': None})
for r in rows:
    if r['n1_2'] is not None and r['n1_2'] != '':
        key = r['n1_2']
        partner_data[key]['stages'].add(lifecycle_stage(r['st_1']))
        partner_data[key]['count'] += 1
        partner_data[key]['n1_1'] = r['n1_1']

partners = sorted(partner_data.items(), key=lambda x: (-x[1]['count'], x[0]))
print(f"ROW_COUNT = {len(partners)} (distinct non-empty party names)")

for name, data in partners:
    stages = ', '.join(sorted(data['stages']))
    print(f"  {name:40s}  code={data['n1_1']}  count={data['count']}  stages={stages}")

assert len(partners) >= 4
assert partner_data['Aaron Copeland']['count'] == 4
assert partner_data['Aaron Copeland']['n1_1'] == 'SO'
print("  ✓ All Q7 assertions verified")


# ════════════════════════════════════════════════════════════════════════════
# VERIFY: All Checks
# ════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 80)
print("VERIFY: All Checks")
print("=" * 80)

# Check 1: lifecycle_has_14_rows
print(f"  lifecycle_has_14_rows:  {'PASS' if len(rows) == 14 else 'FAIL'}")

# Check 2: eight_txn_types
distinct_types = len(set(r['st_1'] for r in rows))
print(f"  eight_txn_types:       {'PASS' if distinct_types == 8 else 'FAIL'} (actual: {distinct_types})")

# Check 3: all_850s_have_beg3
bad_850s = [r for r in rows if r['st_1'] == '850' and (r['beg_3'] is None or r['beg_3'] == '')]
print(f"  all_850s_have_beg3:    {'PASS' if len(bad_850s) == 0 else 'FAIL'}")

# Check 4: all_810s_have_big2
bad_810s = [r for r in rows if r['st_1'] == '810' and (r['big_2'] is None or r['big_2'] == '')]
print(f"  all_810s_have_big2:    {'PASS' if len(bad_810s) == 0 else 'FAIL'}")

# Check 5: fulfillment_has_3_docs
fulfill_count = sum(1 for r in rows if r['st_1'] in ('856', '857', '861'))
print(f"  fulfillment_has_3_docs: {'PASS' if fulfill_count == 3 else 'FAIL'} (actual: {fulfill_count})")

print(f"\nROW_COUNT = 5 (all checks)")

print("\n" + "=" * 80)
print("DONE — All proof values verified")
print("=" * 80)
