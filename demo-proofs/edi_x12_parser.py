#!/usr/bin/env python3
"""
X12 EDI Parser for proof value computation.

Parses all 14 X12 EDI files from the supply-chain-x12 demo data directory
and extracts segment-level data for independent proof value computation.
"""

import os
import json
import re
from collections import defaultdict


DATA_DIR = os.path.join(
    os.path.dirname(__file__), "..",
    "delta-forge-demos", "demos", "edi", "edi-supply-chain-x12", "data"
)


def parse_x12_file(filepath):
    """Parse a single X12 EDI file into a structured transaction dict."""
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read().strip()

    # X12 uses ~ as segment terminator (sometimes with newlines)
    # Element separator is * (defined in ISA)
    # Sub-element separator is : (ISA-16)
    raw_segments = [s.strip() for s in content.split("~") if s.strip()]

    segments = []
    isa_fields = {}
    gs_fields = {}
    st_fields = {}

    for raw in raw_segments:
        elements = raw.split("*")
        seg_id = elements[0].strip()

        seg = {
            "segment": seg_id,
            "elements": elements[1:] if len(elements) > 1 else [],
        }
        segments.append(seg)

        # Extract envelope fields
        if seg_id == "ISA" and len(elements) >= 17:
            for i in range(1, min(17, len(elements))):
                isa_fields[f"ISA_{i}"] = elements[i].strip()
        elif seg_id == "GS" and len(elements) >= 9:
            for i in range(1, min(9, len(elements))):
                gs_fields[f"GS_{i}"] = elements[i].strip()
        elif seg_id == "ST" and len(elements) >= 3:
            st_fields["ST_1"] = elements[1].strip()
            st_fields["ST_2"] = elements[2].strip()

    filename = os.path.basename(filepath)

    return {
        "df_file_name": filename,
        "isa": isa_fields,
        "gs": gs_fields,
        "st": st_fields,
        "segments": segments,
        "segment_count": len(segments),
    }


def extract_segment_values(transaction, seg_id):
    """Extract all occurrences of a segment from a transaction.
    Returns list of element lists."""
    results = []
    for seg in transaction["segments"]:
        if seg["segment"] == seg_id:
            results.append(seg["elements"])
    return results


def get_first_segment_value(transaction, seg_id, element_index):
    """Get the first occurrence of a segment's element value (1-based index)."""
    occurrences = extract_segment_values(transaction, seg_id)
    if occurrences and len(occurrences[0]) >= element_index:
        val = occurrences[0][element_index - 1].strip()
        return val if val else None
    return None


def get_all_segment_values(transaction, seg_id, element_index):
    """Get all occurrences of a segment's element value (1-based index)."""
    results = []
    for occ in extract_segment_values(transaction, seg_id):
        if len(occ) >= element_index:
            val = occ[element_index - 1].strip()
            results.append(val if val else None)
        else:
            results.append(None)
    return results


def parse_all_files():
    """Parse all 14 EDI files and return list of transaction dicts."""
    transactions = []
    for filename in sorted(os.listdir(DATA_DIR)):
        if filename.endswith(".edi"):
            filepath = os.path.join(DATA_DIR, filename)
            txn = parse_x12_file(filepath)
            transactions.append(txn)
    return transactions


def build_segment_inventory(transactions):
    """Build a complete inventory of segment types across all transactions."""
    inventory = defaultdict(lambda: {"count": 0, "files": set()})
    for txn in transactions:
        seen_in_file = set()
        for seg in txn["segments"]:
            seg_id = seg["segment"]
            inventory[seg_id]["count"] += 1
            if seg_id not in seen_in_file:
                inventory[seg_id]["files"].add(txn["df_file_name"])
                seen_in_file.add(seg_id)
    return inventory


# ============================================================================
# Main: Parse and dump all data for proof computation
# ============================================================================

if __name__ == "__main__":
    transactions = parse_all_files()

    print(f"Parsed {len(transactions)} transactions from {DATA_DIR}")
    print()

    # ── Overview ──
    print("=" * 80)
    print("TRANSACTION OVERVIEW")
    print("=" * 80)
    for txn in transactions:
        print(f"  {txn['df_file_name']:45s}  ST_1={txn['st'].get('ST_1','?'):>4s}  "
              f"ISA_6={txn['isa'].get('ISA_6','?'):>16s}  "
              f"ISA_8={txn['isa'].get('ISA_8','?'):>16s}  "
              f"ISA_12={txn['isa'].get('ISA_12','?'):>6s}  "
              f"GS_1={txn['gs'].get('GS_1','?'):>3s}  "
              f"GS_8={txn['gs'].get('GS_8','?'):>8s}  "
              f"segs={txn['segment_count']}")

    # ── Segment inventory ──
    print()
    print("=" * 80)
    print("SEGMENT INVENTORY (across all transactions)")
    print("=" * 80)
    inventory = build_segment_inventory(transactions)
    for seg_id in sorted(inventory.keys()):
        info = inventory[seg_id]
        print(f"  {seg_id:>5s}: {info['count']:>4d} occurrences in {len(info['files']):>2d} files")

    # ── Demo 1: JSON segment extraction proofs ──
    print()
    print("=" * 80)
    print("DEMO 1: JSON SEGMENT EXTRACTION PROOFS")
    print("=" * 80)

    # Query 1: Segment count per transaction
    print("\nQ1: Segment count per transaction:")
    for txn in transactions:
        print(f"  {txn['df_file_name']:45s}  segment_count={txn['segment_count']}")

    # Query 2: Distinct segment types across all transactions
    all_seg_types = set()
    for txn in transactions:
        for seg in txn["segments"]:
            all_seg_types.add(seg["segment"])
    print(f"\nQ2: Total distinct segment types: {len(all_seg_types)}")
    print(f"  Segment types: {sorted(all_seg_types)}")

    # Segment type counts per file
    print("\nQ2b: Segment types per transaction:")
    for txn in transactions:
        seg_types = set(seg["segment"] for seg in txn["segments"])
        print(f"  {txn['df_file_name']:45s}  distinct_segments={len(seg_types)}")

    # Query 3: BEG segments (PO details) - extracted from JSON
    print("\nQ3: BEG segment values (PO details from JSON):")
    for txn in transactions:
        beg = extract_segment_values(txn, "BEG")
        if beg:
            for occ in beg:
                purpose = occ[0] if len(occ) > 0 else None
                po_num = occ[2] if len(occ) > 2 else None
                po_date = occ[4] if len(occ) > 4 else None
                print(f"  {txn['df_file_name']:45s}  BEG_1={purpose}  BEG_3={po_num}  BEG_5={po_date}")

    # Query 4: N1 party segments
    print("\nQ4: N1 party segments (all occurrences):")
    for txn in transactions:
        n1_list = extract_segment_values(txn, "N1")
        if n1_list:
            for i, n1 in enumerate(n1_list):
                entity_code = n1[0] if len(n1) > 0 else None
                name = n1[1] if len(n1) > 1 else None
                print(f"  {txn['df_file_name']:45s}  N1[{i}] code={entity_code}  name={name}")

    # Query 5: Line items (PO1 and IT1)
    print("\nQ5: Line items (PO1 + IT1 segments):")
    for txn in transactions:
        for seg_id in ["PO1", "IT1"]:
            items = extract_segment_values(txn, seg_id)
            if items:
                for i, item in enumerate(items):
                    line_num = item[0] if len(item) > 0 else None
                    qty = item[1] if len(item) > 1 else None
                    uom = item[2] if len(item) > 2 else None
                    price = item[3] if len(item) > 3 else None
                    print(f"  {txn['df_file_name']:45s}  {seg_id}[{i}] line={line_num} qty={qty} uom={uom} price={price}")

    # ── Demo 2: Repeating segments proofs ──
    print()
    print("=" * 80)
    print("DEMO 2: REPEATING SEGMENTS PROOFS")
    print("=" * 80)

    # N1 segment repeating analysis
    print("\nN1 occurrence counts per file:")
    for txn in transactions:
        n1_list = extract_segment_values(txn, "N1")
        n1_names = [n1[1] if len(n1) > 1 else "" for n1 in n1_list]
        print(f"  {txn['df_file_name']:45s}  N1_count={len(n1_list)}  names={n1_names}")

    # PO1 repeating analysis
    print("\nPO1 occurrence counts per file:")
    for txn in transactions:
        po1_list = extract_segment_values(txn, "PO1")
        if po1_list:
            print(f"  {txn['df_file_name']:45s}  PO1_count={len(po1_list)}")
            for i, po1 in enumerate(po1_list):
                print(f"    [{i}] elements={po1}")

    # IT1 repeating analysis
    print("\nIT1 occurrence counts per file:")
    for txn in transactions:
        it1_list = extract_segment_values(txn, "IT1")
        if it1_list:
            print(f"  {txn['df_file_name']:45s}  IT1_count={len(it1_list)}")
            for i, it1 in enumerate(it1_list):
                print(f"    [{i}] elements={it1}")

    # REF repeating analysis
    print("\nREF occurrence counts per file:")
    for txn in transactions:
        ref_list = extract_segment_values(txn, "REF")
        if ref_list:
            refs = [(r[0] if len(r) > 0 else "?", r[1] if len(r) > 1 else "?") for r in ref_list]
            print(f"  {txn['df_file_name']:45s}  REF_count={len(ref_list)}  refs={refs}")

    # HL repeating analysis
    print("\nHL occurrence counts per file:")
    for txn in transactions:
        hl_list = extract_segment_values(txn, "HL")
        if hl_list:
            print(f"  {txn['df_file_name']:45s}  HL_count={len(hl_list)}")
            for i, hl in enumerate(hl_list):
                hl_id = hl[0] if len(hl) > 0 else "?"
                parent = hl[1] if len(hl) > 1 else "?"
                level = hl[2] if len(hl) > 2 else "?"
                print(f"    [{i}] id={hl_id} parent={parent} level_code={level}")

    # ── Demo 3: Order lifecycle linking proofs ──
    print()
    print("=" * 80)
    print("DEMO 3: ORDER LIFECYCLE LINKING PROOFS")
    print("=" * 80)

    # Group by transaction type
    by_type = defaultdict(list)
    for txn in transactions:
        by_type[txn["st"]["ST_1"]].append(txn)

    print("\nTransactions by type:")
    for st1 in sorted(by_type.keys()):
        print(f"  {st1}: {[t['df_file_name'] for t in by_type[st1]]}")

    # Extract PO numbers from 850s
    print("\n850 Purchase Orders:")
    for txn in by_type.get("850", []):
        po_num = get_first_segment_value(txn, "BEG", 3)
        po_date = get_first_segment_value(txn, "BEG", 5)
        print(f"  {txn['df_file_name']:45s}  PO#={po_num}  Date={po_date}")

    # Extract from 855 PO Ack
    print("\n855 PO Acknowledgments:")
    for txn in by_type.get("855", []):
        bak = extract_segment_values(txn, "BAK")
        if bak:
            status = bak[0][0] if len(bak[0]) > 0 else None
            po_num = bak[0][2] if len(bak[0]) > 2 else None
            po_date = bak[0][3] if len(bak[0]) > 3 else None
            print(f"  {txn['df_file_name']:45s}  status={status}  PO#={po_num}  Date={po_date}")

    # Extract from 810 Invoices
    print("\n810 Invoices:")
    for txn in by_type.get("810", []):
        inv_date = get_first_segment_value(txn, "BIG", 1)
        inv_num = get_first_segment_value(txn, "BIG", 2)
        # Look for PO reference in REF segments
        refs = extract_segment_values(txn, "REF")
        po_refs = [(r[0], r[1]) for r in refs if len(r) > 1]
        print(f"  {txn['df_file_name']:45s}  Inv#={inv_num}  Date={inv_date}  REFs={po_refs}")

    # Extract from 856 Ship Notice
    print("\n856 Ship Notices:")
    for txn in by_type.get("856", []):
        bsn = extract_segment_values(txn, "BSN")
        if bsn:
            ship_id = bsn[0][1] if len(bsn[0]) > 1 else None
            ship_date = bsn[0][2] if len(bsn[0]) > 2 else None
            print(f"  {txn['df_file_name']:45s}  ShipID={ship_id}  Date={ship_date}")

    # Extract from 857 Ship/Bill
    print("\n857 Shipment & Billing:")
    for txn in by_type.get("857", []):
        bht = extract_segment_values(txn, "BHT")
        if bht:
            print(f"  {txn['df_file_name']:45s}  BHT={bht[0]}")

    # Extract from 861 Receiving Advice
    print("\n861 Receiving Advice:")
    for txn in by_type.get("861", []):
        bra = extract_segment_values(txn, "BRA")
        if bra:
            ref_num = bra[0][0] if len(bra[0]) > 0 else None
            print(f"  {txn['df_file_name']:45s}  BRA ref={ref_num}")

    # ── Demo 4: Compliance validation proofs ──
    print()
    print("=" * 80)
    print("DEMO 4: COMPLIANCE VALIDATION PROOFS")
    print("=" * 80)

    # 997 Functional Acknowledgment
    print("\n997 Functional Acknowledgment details:")
    for txn in by_type.get("997", []):
        ak1 = extract_segment_values(txn, "AK1")
        ak2 = extract_segment_values(txn, "AK2")
        ak3 = extract_segment_values(txn, "AK3")
        ak4 = extract_segment_values(txn, "AK4")
        ak5 = extract_segment_values(txn, "AK5")
        ak9 = extract_segment_values(txn, "AK9")
        print(f"  File: {txn['df_file_name']}")
        print(f"  AK1 (group ack): {ak1}")
        print(f"  AK2 (txn set):   {ak2}")
        print(f"  AK3 (seg error): {ak3}")
        print(f"  AK4 (elem error):{ak4}")
        print(f"  AK5 (txn status):{ak5}")
        print(f"  AK9 (group resp):{ak9}")

    # 824 Application Advice
    print("\n824 Application Advice details:")
    for txn in by_type.get("824", []):
        bgn = extract_segment_values(txn, "BGN")
        oti = extract_segment_values(txn, "OTI")
        ted = extract_segment_values(txn, "TED")
        n1 = extract_segment_values(txn, "N1")
        ref = extract_segment_values(txn, "REF")
        print(f"  File: {txn['df_file_name']}")
        print(f"  BGN (begin):     {bgn}")
        print(f"  OTI (orig txn):  {oti}")
        print(f"  TED (error):     {ted}")
        print(f"  N1  (parties):   {n1}")
        print(f"  REF (references):{ref}")

    # ── Summary statistics ──
    print()
    print("=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)

    # Total segments across all files
    total_segs = sum(txn["segment_count"] for txn in transactions)
    print(f"  Total transactions: {len(transactions)}")
    print(f"  Total segments:     {total_segs}")
    print(f"  Avg segs/txn:       {total_segs / len(transactions):.1f}")
    print(f"  Min segs:           {min(txn['segment_count'] for txn in transactions)}")
    print(f"  Max segs:           {max(txn['segment_count'] for txn in transactions)}")

    # Files with most N1 segments (for repeating segments demo)
    n1_counts = []
    for txn in transactions:
        n1_list = extract_segment_values(txn, "N1")
        n1_counts.append((txn["df_file_name"], len(n1_list)))
    n1_counts.sort(key=lambda x: -x[1])
    print(f"\nTop files by N1 count:")
    for fname, count in n1_counts[:5]:
        print(f"  {fname:45s}  N1_count={count}")

    # Files with PO1 line items
    po1_counts = []
    for txn in transactions:
        po1_list = extract_segment_values(txn, "PO1")
        if po1_list:
            po1_counts.append((txn["df_file_name"], len(po1_list)))
    print(f"\nFiles with PO1 line items:")
    for fname, count in po1_counts:
        print(f"  {fname:45s}  PO1_count={count}")

    # SAC charge analysis
    print("\nSAC charge segments:")
    for txn in transactions:
        sac_list = extract_segment_values(txn, "SAC")
        if sac_list:
            print(f"  {txn['df_file_name']:45s}  SAC_count={len(sac_list)}")
            for i, sac in enumerate(sac_list):
                print(f"    [{i}] {sac}")

    # TXI tax analysis
    print("\nTXI tax segments:")
    for txn in transactions:
        txi_list = extract_segment_values(txn, "TXI")
        if txi_list:
            print(f"  {txn['df_file_name']:45s}  TXI_count={len(txi_list)}")
            for i, txi in enumerate(txi_list):
                print(f"    [{i}] {txi}")
