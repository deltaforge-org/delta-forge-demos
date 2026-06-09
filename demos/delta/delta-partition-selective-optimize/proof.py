#!/usr/bin/env python3
"""
Proof-of-values script for delta-partition-selective-optimize demo.
Partition-Scoped Maintenance — Global e-commerce warehouse orders.

75 rows, 3 partitions (us-east-dc, eu-central-dc, ap-south-dc), 25 each.
"""
from decimal import Decimal, ROUND_HALF_UP
import json

# ============================================================================
# DATA DEFINITION — 75 rows
# ============================================================================

rows = [
    # --- us-east-dc (25 rows, ids 1-25) ---
    (1,  'ORD-1001', 'us-east-dc', 'electronics', 4, Decimal('149.99'), '2025-03-01', 'express'),
    (2,  'ORD-1002', 'us-east-dc', 'clothing',    2, Decimal('39.99'),  '2025-03-01', 'standard'),
    (3,  'ORD-1003', 'us-east-dc', 'food',        10, Decimal('12.50'),  '2025-03-02', 'standard'),
    (4,  'ORD-1004', 'us-east-dc', 'furniture',   1, Decimal('899.99'), '2025-03-02', 'express'),
    (5,  'ORD-1005', 'us-east-dc', 'toys',        3, Decimal('24.99'),  '2025-03-03', 'standard'),
    (6,  'ORD-1006', 'us-east-dc', 'electronics', 2, Decimal('349.99'), '2025-03-03', 'overnight'),
    (7,  'ORD-1007', 'us-east-dc', 'clothing',    5, Decimal('59.99'),  '2025-03-04', 'standard'),
    (8,  'ORD-1008', 'us-east-dc', 'food',        8, Decimal('9.99'),   '2025-03-04', 'express'),
    (9,  'ORD-1009', 'us-east-dc', 'furniture',   1, Decimal('549.99'), '2025-03-05', 'standard'),
    (10, 'ORD-1010', 'us-east-dc', 'toys',        6, Decimal('34.99'),  '2025-03-05', 'overnight'),
    (11, 'ORD-1011', 'us-east-dc', 'electronics', 1, Decimal('799.99'), '2025-03-06', 'express'),
    (12, 'ORD-1012', 'us-east-dc', 'clothing',    3, Decimal('29.99'),  '2025-03-06', 'standard'),
    (13, 'ORD-1013', 'us-east-dc', 'food',        15, Decimal('14.99'),  '2025-03-07', 'standard'),
    (14, 'ORD-1014', 'us-east-dc', 'furniture',   2, Decimal('419.99'), '2025-03-07', 'express'),
    (15, 'ORD-1015', 'us-east-dc', 'toys',        4, Decimal('19.99'),  '2025-03-08', 'standard'),
    (16, 'ORD-1016', 'us-east-dc', 'electronics', 3, Decimal('129.99'), '2025-03-08', 'overnight'),
    (17, 'ORD-1017', 'us-east-dc', 'clothing',    1, Decimal('89.99'),  '2025-03-09', 'express'),
    (18, 'ORD-1018', 'us-east-dc', 'food',        20, Decimal('11.99'),  '2025-03-09', 'standard'),
    (19, 'ORD-1019', 'us-east-dc', 'furniture',   1, Decimal('649.99'), '2025-03-10', 'express'),
    (20, 'ORD-1020', 'us-east-dc', 'toys',        7, Decimal('44.99'),  '2025-03-10', 'standard'),
    (21, 'ORD-1021', 'us-east-dc', 'electronics', 2, Decimal('219.99'), '2025-03-11', 'standard'),
    (22, 'ORD-1022', 'us-east-dc', 'clothing',    4, Decimal('49.99'),  '2025-03-11', 'express'),
    (23, 'ORD-1023', 'us-east-dc', 'food',        6, Decimal('18.99'),  '2025-03-12', 'overnight'),
    (24, 'ORD-1024', 'us-east-dc', 'furniture',   1, Decimal('329.99'), '2025-03-12', 'standard'),
    (25, 'ORD-1025', 'us-east-dc', 'toys',        2, Decimal('64.99'),  '2025-03-12', 'express'),

    # --- eu-central-dc (25 rows, ids 26-50) ---
    (26, 'ORD-2001', 'eu-central-dc', 'electronics', 3, Decimal('179.99'), '2025-03-01', 'standard'),
    (27, 'ORD-2002', 'eu-central-dc', 'clothing',    6, Decimal('44.99'),  '2025-03-01', 'express'),
    (28, 'ORD-2003', 'eu-central-dc', 'food',        12, Decimal('15.99'),  '2025-03-02', 'standard'),
    (29, 'ORD-2004', 'eu-central-dc', 'furniture',   1, Decimal('749.99'), '2025-03-02', 'overnight'),
    (30, 'ORD-2005', 'eu-central-dc', 'toys',        5, Decimal('29.99'),  '2025-03-03', 'standard'),
    (31, 'ORD-2006', 'eu-central-dc', 'electronics', 1, Decimal('599.99'), '2025-03-03', 'express'),
    (32, 'ORD-2007', 'eu-central-dc', 'clothing',    2, Decimal('79.99'),  '2025-03-04', 'standard'),
    (33, 'ORD-2008', 'eu-central-dc', 'food',        9, Decimal('11.49'),  '2025-03-04', 'express'),
    (34, 'ORD-2009', 'eu-central-dc', 'furniture',   1, Decimal('459.99'), '2025-03-05', 'standard'),
    (35, 'ORD-2010', 'eu-central-dc', 'toys',        8, Decimal('17.99'),  '2025-03-05', 'overnight'),
    (36, 'ORD-2011', 'eu-central-dc', 'electronics', 2, Decimal('299.99'), '2025-03-06', 'standard'),
    (37, 'ORD-2012', 'eu-central-dc', 'clothing',    3, Decimal('69.99'),  '2025-03-06', 'express'),
    (38, 'ORD-2013', 'eu-central-dc', 'food',        7, Decimal('22.99'),  '2025-03-07', 'standard'),
    (39, 'ORD-2014', 'eu-central-dc', 'furniture',   2, Decimal('369.99'), '2025-03-07', 'express'),
    (40, 'ORD-2015', 'eu-central-dc', 'toys',        4, Decimal('54.99'),  '2025-03-08', 'standard'),
    (41, 'ORD-2016', 'eu-central-dc', 'electronics', 1, Decimal('449.99'), '2025-03-08', 'overnight'),
    (42, 'ORD-2017', 'eu-central-dc', 'clothing',    5, Decimal('34.99'),  '2025-03-09', 'standard'),
    (43, 'ORD-2018', 'eu-central-dc', 'food',        14, Decimal('13.49'),  '2025-03-09', 'express'),
    (44, 'ORD-2019', 'eu-central-dc', 'furniture',   1, Decimal('579.99'), '2025-03-10', 'standard'),
    (45, 'ORD-2020', 'eu-central-dc', 'toys',        3, Decimal('42.99'),  '2025-03-10', 'express'),
    (46, 'ORD-2021', 'eu-central-dc', 'electronics', 4, Decimal('159.99'), '2025-03-11', 'standard'),
    (47, 'ORD-2022', 'eu-central-dc', 'clothing',    1, Decimal('99.99'),  '2025-03-11', 'overnight'),
    (48, 'ORD-2023', 'eu-central-dc', 'food',        11, Decimal('19.99'),  '2025-03-12', 'standard'),
    (49, 'ORD-2024', 'eu-central-dc', 'furniture',   1, Decimal('279.99'), '2025-03-12', 'express'),
    (50, 'ORD-2025', 'eu-central-dc', 'toys',        6, Decimal('39.99'),  '2025-03-12', 'standard'),

    # --- ap-south-dc (25 rows, ids 51-75) ---
    (51, 'ORD-3001', 'ap-south-dc', 'electronics', 2, Decimal('199.99'), '2025-03-01', 'express'),
    (52, 'ORD-3002', 'ap-south-dc', 'clothing',    4, Decimal('54.99'),  '2025-03-01', 'standard'),
    (53, 'ORD-3003', 'ap-south-dc', 'food',        8, Decimal('16.99'),  '2025-03-02', 'overnight'),
    (54, 'ORD-3004', 'ap-south-dc', 'furniture',   1, Decimal('699.99'), '2025-03-02', 'standard'),
    (55, 'ORD-3005', 'ap-south-dc', 'toys',        5, Decimal('22.99'),  '2025-03-03', 'express'),
    (56, 'ORD-3006', 'ap-south-dc', 'electronics', 3, Decimal('249.99'), '2025-03-03', 'standard'),
    (57, 'ORD-3007', 'ap-south-dc', 'clothing',    2, Decimal('74.99'),  '2025-03-04', 'express'),
    (58, 'ORD-3008', 'ap-south-dc', 'food',        15, Decimal('10.99'),  '2025-03-04', 'standard'),
    (59, 'ORD-3009', 'ap-south-dc', 'furniture',   1, Decimal('519.99'), '2025-03-05', 'overnight'),
    (60, 'ORD-3010', 'ap-south-dc', 'toys',        7, Decimal('27.99'),  '2025-03-05', 'standard'),
    (61, 'ORD-3011', 'ap-south-dc', 'electronics', 1, Decimal('849.99'), '2025-03-06', 'express'),
    (62, 'ORD-3012', 'ap-south-dc', 'clothing',    6, Decimal('32.99'),  '2025-03-06', 'standard'),
    (63, 'ORD-3013', 'ap-south-dc', 'food',        10, Decimal('21.49'),  '2025-03-07', 'standard'),
    (64, 'ORD-3014', 'ap-south-dc', 'furniture',   1, Decimal('389.99'), '2025-03-07', 'overnight'),
    (65, 'ORD-3015', 'ap-south-dc', 'toys',        3, Decimal('49.99'),  '2025-03-08', 'express'),
    (66, 'ORD-3016', 'ap-south-dc', 'electronics', 2, Decimal('169.99'), '2025-03-08', 'standard'),
    (67, 'ORD-3017', 'ap-south-dc', 'clothing',    1, Decimal('119.99'), '2025-03-09', 'standard'),
    (68, 'ORD-3018', 'ap-south-dc', 'food',        18, Decimal('9.99'),   '2025-03-09', 'express'),
    (69, 'ORD-3019', 'ap-south-dc', 'furniture',   1, Decimal('759.99'), '2025-03-10', 'standard'),
    (70, 'ORD-3020', 'ap-south-dc', 'toys',        4, Decimal('36.99'),  '2025-03-10', 'overnight'),
    (71, 'ORD-3021', 'ap-south-dc', 'electronics', 5, Decimal('109.99'), '2025-03-11', 'express'),
    (72, 'ORD-3022', 'ap-south-dc', 'clothing',    3, Decimal('64.99'),  '2025-03-11', 'standard'),
    (73, 'ORD-3023', 'ap-south-dc', 'food',        6, Decimal('24.99'),  '2025-03-12', 'standard'),
    (74, 'ORD-3024', 'ap-south-dc', 'furniture',   2, Decimal('449.99'), '2025-03-12', 'express'),
    (75, 'ORD-3025', 'ap-south-dc', 'toys',        9, Decimal('14.99'),  '2025-03-12', 'overnight'),
]

# Verify 75 rows
assert len(rows) == 75, f"Expected 75 rows, got {len(rows)}"

# Partition counts
us_east = [r for r in rows if r[2] == 'us-east-dc']
eu_central = [r for r in rows if r[2] == 'eu-central-dc']
ap_south = [r for r in rows if r[2] == 'ap-south-dc']
assert len(us_east) == 25
assert len(eu_central) == 25
assert len(ap_south) == 25

# ============================================================================
# EXPLORE: Baseline per-warehouse revenue
# ============================================================================

def revenue(row):
    return row[4] * row[5]  # quantity * unit_price

def partition_stats(partition, name):
    cnt = len(partition)
    total_rev = sum(revenue(r) for r in partition)
    products = len(set(r[3] for r in partition))
    avg_price = sum(r[5] for r in partition) / len(partition)
    return {
        'warehouse': name,
        'order_count': cnt,
        'total_revenue': total_rev,
        'distinct_products': products,
        'avg_unit_price': avg_price,
    }

print("=" * 70)
print("EXPLORE: Baseline — Per-Warehouse Stats")
print("=" * 70)
for part, name in [(us_east, 'us-east-dc'), (eu_central, 'eu-central-dc'), (ap_south, 'ap-south-dc')]:
    s = partition_stats(part, name)
    print(f"  {s['warehouse']}: count={s['order_count']}, revenue={s['total_revenue']:.2f}, "
          f"products={s['distinct_products']}, avg_price={s['avg_unit_price']:.2f}")

# Total baseline revenue
total_baseline_revenue = sum(revenue(r) for r in rows)
print(f"\n  Total baseline revenue: {total_baseline_revenue:.2f}")
print(f"  Total baseline count: {len(rows)}")

# Per-partition revenue
us_east_revenue = sum(revenue(r) for r in us_east)
eu_central_revenue = sum(revenue(r) for r in eu_central)
ap_south_revenue = sum(revenue(r) for r in ap_south)
print(f"  us-east-dc revenue: {us_east_revenue:.2f}")
print(f"  eu-central-dc revenue: {eu_central_revenue:.2f}")
print(f"  ap-south-dc revenue: {ap_south_revenue:.2f}")

# ============================================================================
# STEP 1: DELETE 5 from us-east-dc (cancelled/low-value orders)
# ============================================================================
# We pick 5 ids to delete: low-value or small quantity orders
# ids: 5 (toys, qty=3, $24.99), 12 (clothing, qty=3, $29.99),
#      15 (toys, qty=4, $19.99), 18 (food, qty=20, $11.99), 3 (food, qty=10, $12.50)
us_east_delete_ids = {3, 5, 12, 15, 18}

print("\n" + "=" * 70)
print("STEP 1: DELETE 5 from us-east-dc")
print("=" * 70)
deleted_us_east = [r for r in us_east if r[0] in us_east_delete_ids]
for r in deleted_us_east:
    print(f"  Deleting id={r[0]}, order_id={r[1]}, product={r[3]}, qty={r[4]}, price={r[5]}")
deleted_us_east_revenue = sum(revenue(r) for r in deleted_us_east)
print(f"  Revenue removed: {deleted_us_east_revenue:.2f}")

# ============================================================================
# STEP 2: DELETE 5 from eu-central-dc
# ============================================================================
eu_central_delete_ids = {28, 33, 40, 42, 48}

print("\n" + "=" * 70)
print("STEP 2: DELETE 5 from eu-central-dc")
print("=" * 70)
deleted_eu_central = [r for r in eu_central if r[0] in eu_central_delete_ids]
for r in deleted_eu_central:
    print(f"  Deleting id={r[0]}, order_id={r[1]}, product={r[3]}, qty={r[4]}, price={r[5]}")
deleted_eu_central_revenue = sum(revenue(r) for r in deleted_eu_central)
print(f"  Revenue removed: {deleted_eu_central_revenue:.2f}")

# ============================================================================
# STEP 3: UPDATE 3 orders in ap-south-dc to 'overnight' priority
# ============================================================================
ap_south_update_ids = {52, 58, 66}  # clothing std, food std, electronics std

print("\n" + "=" * 70)
print("STEP 3: UPDATE 3 in ap-south-dc — set priority='overnight'")
print("=" * 70)
updated_ap_south = [r for r in ap_south if r[0] in ap_south_update_ids]
for r in updated_ap_south:
    print(f"  Updating id={r[0]}, order_id={r[1]}, priority '{r[7]}' -> 'overnight'")

# ============================================================================
# Apply mutations to working dataset
# ============================================================================
after_mutations = []
for r in rows:
    rid = r[0]
    if rid in us_east_delete_ids or rid in eu_central_delete_ids:
        continue  # deleted
    if rid in ap_south_update_ids:
        # update priority to 'overnight'
        after_mutations.append((r[0], r[1], r[2], r[3], r[4], r[5], r[6], 'overnight'))
    else:
        after_mutations.append(r)

print("\n" + "=" * 70)
print("LEARN: Post-mutation counts")
print("=" * 70)
us_east_after = [r for r in after_mutations if r[2] == 'us-east-dc']
eu_central_after = [r for r in after_mutations if r[2] == 'eu-central-dc']
ap_south_after = [r for r in after_mutations if r[2] == 'ap-south-dc']

print(f"  us-east-dc: {len(us_east_after)} (was 25, deleted 5)")
print(f"  eu-central-dc: {len(eu_central_after)} (was 25, deleted 5)")
print(f"  ap-south-dc: {len(ap_south_after)} (was 25, updated 3)")
print(f"  Total: {len(after_mutations)}")

assert len(us_east_after) == 20
assert len(eu_central_after) == 20
assert len(ap_south_after) == 25
assert len(after_mutations) == 65

# ============================================================================
# Per-partition revenue AFTER mutations
# ============================================================================
us_east_rev_after = sum(revenue(r) for r in us_east_after)
eu_central_rev_after = sum(revenue(r) for r in eu_central_after)
ap_south_rev_after = sum(revenue(r) for r in ap_south_after)
total_rev_after = sum(revenue(r) for r in after_mutations)

print(f"\n  us-east-dc revenue after: {us_east_rev_after:.2f}")
print(f"  eu-central-dc revenue after: {eu_central_rev_after:.2f}")
print(f"  ap-south-dc revenue after: {ap_south_rev_after:.2f}")
print(f"  Total revenue after: {total_rev_after:.2f}")

# ============================================================================
# Per-priority counts (after mutations)
# ============================================================================
print("\n" + "=" * 70)
print("Per-priority counts after mutations")
print("=" * 70)
for prio in ['standard', 'express', 'overnight']:
    cnt = len([r for r in after_mutations if r[7] == prio])
    print(f"  {prio}: {cnt}")

# Per-warehouse, per-priority
for wh in ['us-east-dc', 'eu-central-dc', 'ap-south-dc']:
    part = [r for r in after_mutations if r[2] == wh]
    for prio in ['standard', 'express', 'overnight']:
        cnt = len([r for r in part if r[7] == prio])
        if cnt > 0:
            print(f"  {wh} / {prio}: {cnt}")

# ============================================================================
# Per-product category revenue after mutations
# ============================================================================
print("\n" + "=" * 70)
print("Per-product revenue after mutations")
print("=" * 70)
products = ['electronics', 'clothing', 'food', 'furniture', 'toys']
for prod in products:
    prod_rows = [r for r in after_mutations if r[3] == prod]
    cnt = len(prod_rows)
    rev = sum(revenue(r) for r in prod_rows)
    avg_qty = sum(r[4] for r in prod_rows) / len(prod_rows) if prod_rows else 0
    print(f"  {prod}: count={cnt}, revenue={rev:.2f}, avg_qty={avg_qty:.1f}")

# ============================================================================
# LEARN: Verify per-partition state before OPTIMIZE
# ============================================================================
print("\n" + "=" * 70)
print("LEARN: Per-partition state before OPTIMIZE")
print("=" * 70)

# Check specific orders still exist
for check_id in [1, 6, 11, 26, 31, 51, 61, 69]:
    found = [r for r in after_mutations if r[0] == check_id]
    if found:
        r = found[0]
        print(f"  id={r[0]} exists: {r[1]}, {r[2]}, {r[3]}, qty={r[4]}, price={r[5]}, priority={r[7]}")

# Check deleted orders are gone
for check_id in [3, 5, 12, 15, 18, 28, 33, 40, 42, 48]:
    found = [r for r in after_mutations if r[0] == check_id]
    assert len(found) == 0, f"id={check_id} should be deleted"
    print(f"  id={check_id} deleted: confirmed gone")

# Check updated orders have overnight priority
for check_id in [52, 58, 66]:
    found = [r for r in after_mutations if r[0] == check_id]
    assert found[0][7] == 'overnight', f"id={check_id} should be overnight"
    print(f"  id={check_id} priority='overnight': confirmed")

# ============================================================================
# Per-warehouse per-product counts after
# ============================================================================
print("\n" + "=" * 70)
print("Per-warehouse per-product counts")
print("=" * 70)
for wh in ['us-east-dc', 'eu-central-dc', 'ap-south-dc']:
    part = [r for r in after_mutations if r[2] == wh]
    for prod in products:
        cnt = len([r for r in part if r[3] == prod])
        print(f"  {wh} / {prod}: {cnt}")

# ============================================================================
# Overnight priority count per partition (for update verification)
# ============================================================================
print("\n" + "=" * 70)
print("Overnight priority per partition")
print("=" * 70)
for wh in ['us-east-dc', 'eu-central-dc', 'ap-south-dc']:
    part = [r for r in after_mutations if r[2] == wh]
    overnight = [r for r in part if r[7] == 'overnight']
    print(f"  {wh}: overnight_count={len(overnight)}")

# ============================================================================
# POST-OPTIMIZE checks (data doesn't change, just verify same values)
# ============================================================================
print("\n" + "=" * 70)
print("POST-OPTIMIZE: Final Verification Values")
print("=" * 70)
print(f"  total_count = {len(after_mutations)}")
print(f"  us_east_count = {len(us_east_after)}")
print(f"  eu_central_count = {len(eu_central_after)}")
print(f"  ap_south_count = {len(ap_south_after)}")
print(f"  total_revenue = {total_rev_after:.2f}")
print(f"  us_east_revenue = {us_east_rev_after:.2f}")
print(f"  eu_central_revenue = {eu_central_rev_after:.2f}")
print(f"  ap_south_revenue = {ap_south_rev_after:.2f}")

# Average unit price per warehouse after
for wh_name, wh_rows in [('us-east-dc', us_east_after), ('eu-central-dc', eu_central_after), ('ap-south-dc', ap_south_after)]:
    avg_up = sum(r[5] for r in wh_rows) / len(wh_rows)
    print(f"  {wh_name} avg_unit_price = {avg_up:.2f}")

# Distinct products per warehouse
for wh_name, wh_rows in [('us-east-dc', us_east_after), ('eu-central-dc', eu_central_after), ('ap-south-dc', ap_south_after)]:
    dp = len(set(r[3] for r in wh_rows))
    print(f"  {wh_name} distinct_products = {dp}")

# Total quantity per warehouse
for wh_name, wh_rows in [('us-east-dc', us_east_after), ('eu-central-dc', eu_central_after), ('ap-south-dc', ap_south_after)]:
    tq = sum(r[4] for r in wh_rows)
    print(f"  {wh_name} total_quantity = {tq}")

# Warehouse count
print(f"  warehouse_count = {len(set(r[2] for r in after_mutations))}")

# Min/max id remaining
all_ids = sorted([r[0] for r in after_mutations])
print(f"  min_id = {all_ids[0]}")
print(f"  max_id = {all_ids[-1]}")

# Check specific order after update: id=52 should be overnight
r52 = [r for r in after_mutations if r[0] == 52][0]
print(f"  id=52 priority = '{r52[7]}'")

# Check specific remaining order: id=1
r1 = [r for r in after_mutations if r[0] == 1][0]
print(f"  id=1 order_id = '{r1[1]}', priority = '{r1[7]}'")

# ============================================================================
# ROUND values for SQL ROUND() compatibility
# ============================================================================
# avg unit price per warehouse (ROUND to 2 decimals)
print("\n" + "=" * 70)
print("ROUNDED avg_unit_price per warehouse (for ASSERT)")
print("=" * 70)
for wh_name, wh_rows in [('us-east-dc', us_east_after), ('eu-central-dc', eu_central_after), ('ap-south-dc', ap_south_after)]:
    avg_up = sum(r[5] for r in wh_rows) / len(wh_rows)
    rounded = float(Decimal(str(avg_up)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    print(f"  {wh_name} ROUND(AVG(unit_price),2) = {rounded}")

# Total revenue ROUND to 2
total_rev_rounded = float(Decimal(str(total_rev_after)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
print(f"  Total revenue (rounded) = {total_rev_rounded}")

# ============================================================================
# EXPLORE: baseline per-warehouse revenue (for initial ASSERT)
# ============================================================================
print("\n" + "=" * 70)
print("BASELINE values for initial ASSERT")
print("=" * 70)
for wh_name, wh_rows in [('us-east-dc', us_east), ('eu-central-dc', eu_central), ('ap-south-dc', ap_south)]:
    rev = sum(revenue(r) for r in wh_rows)
    rounded = float(Decimal(str(rev)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
    print(f"  {wh_name} baseline_revenue = {rounded}")

# Total baseline
total_bl = sum(revenue(r) for r in rows)
print(f"  Total baseline revenue = {float(Decimal(str(total_bl)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))}")

# ============================================================================
# Generate SQL INSERT statements split by warehouse
# ============================================================================
print("\n" + "=" * 70)
print("SQL INSERT — us-east-dc")
print("=" * 70)
for part, name in [(us_east, 'us-east-dc'), (eu_central, 'eu-central-dc'), (ap_south, 'ap-south-dc')]:
    if name != 'us-east-dc':
        print(f"\n-- {name}")
    lines = []
    for i, r in enumerate(part):
        comma = ',' if i < len(part) - 1 else ';'
        lines.append(f"    ({r[0]:>2}, '{r[1]}', '{r[2]}', '{r[3]}', {r[4]:>2}, {r[5]:>8}, '{r[6]}', '{r[7]}')")
    for l in lines:
        print(l)

# ============================================================================
# Sum of deleted revenue
# ============================================================================
print("\n" + "=" * 70)
print("Deleted revenue details")
print("=" * 70)
print("us-east-dc deleted:")
for r in sorted(deleted_us_east, key=lambda x: x[0]):
    print(f"  id={r[0]}: {r[4]} x {r[5]} = {revenue(r)}")
print(f"  Total deleted: {deleted_us_east_revenue}")

print("eu-central-dc deleted:")
for r in sorted(deleted_eu_central, key=lambda x: x[0]):
    print(f"  id={r[0]}: {r[4]} x {r[5]} = {revenue(r)}")
print(f"  Total deleted: {deleted_eu_central_revenue}")

# ============================================================================
# Specific values for VERIFY section
# ============================================================================
print("\n" + "=" * 70)
print("VERIFY: Specific ASSERT values")
print("=" * 70)

# deleted ids as comma-separated
us_east_del_str = ', '.join(str(x) for x in sorted(us_east_delete_ids))
eu_central_del_str = ', '.join(str(x) for x in sorted(eu_central_delete_ids))
ap_south_upd_str = ', '.join(str(x) for x in sorted(ap_south_update_ids))
print(f"  us-east-dc delete ids: {us_east_del_str}")
print(f"  eu-central-dc delete ids: {eu_central_del_str}")
print(f"  ap-south-dc update ids: {ap_south_upd_str}")

# Count of overnight orders in ap-south-dc after update
ap_south_overnight = len([r for r in ap_south_after if r[7] == 'overnight'])
print(f"  ap-south-dc overnight count after: {ap_south_overnight}")

# original overnight in ap-south-dc
ap_south_overnight_orig = len([r for r in ap_south if r[7] == 'overnight'])
print(f"  ap-south-dc overnight count before: {ap_south_overnight_orig}")

# max revenue single order across all
max_rev_order = max(after_mutations, key=lambda r: revenue(r))
print(f"  Highest revenue order: id={max_rev_order[0]}, rev={revenue(max_rev_order)}")

# Verify us-east-dc baseline order_count per product
print("\n  us-east-dc product counts (baseline):")
for prod in products:
    cnt = len([r for r in us_east if r[3] == prod])
    print(f"    {prod}: {cnt}")

print("\n  us-east-dc product counts (after delete):")
for prod in products:
    cnt = len([r for r in us_east_after if r[3] == prod])
    print(f"    {prod}: {cnt}")

# ============================================================================
# REVENUE per partition ROUNDED for SQL comparison
# ============================================================================
print("\n" + "=" * 70)
print("FINAL SUMMARY — All ASSERT values")
print("=" * 70)

vals = {
    'baseline_total_count': 75,
    'baseline_us_east_count': 25,
    'baseline_eu_central_count': 25,
    'baseline_ap_south_count': 25,
    'baseline_total_revenue': float(Decimal(str(sum(revenue(r) for r in rows))).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
    'baseline_us_east_revenue': float(Decimal(str(us_east_revenue)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
    'baseline_eu_central_revenue': float(Decimal(str(eu_central_revenue)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
    'baseline_ap_south_revenue': float(Decimal(str(ap_south_revenue)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
    'after_total_count': 65,
    'after_us_east_count': 20,
    'after_eu_central_count': 20,
    'after_ap_south_count': 25,
    'after_total_revenue': float(Decimal(str(total_rev_after)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
    'after_us_east_revenue': float(Decimal(str(us_east_rev_after)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
    'after_eu_central_revenue': float(Decimal(str(eu_central_rev_after)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
    'after_ap_south_revenue': float(Decimal(str(ap_south_rev_after)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)),
    'ap_south_overnight_count': ap_south_overnight,
    'warehouse_count': 3,
    'min_id': all_ids[0],
    'max_id': all_ids[-1],
}

for k, v in vals.items():
    print(f"  {k} = {v}")

print("\nDone. All values verified.")
