#!/usr/bin/env python3
"""
Compute all proof values for the delta-partition-delete demo.

Real-world scenario: E-commerce fulfillment — warehouse orders partitioned by
region. Demonstrates partition-scoped DELETE, cross-partition DELETE, and
conditional DELETE with data predicates on a standard Delta table.

Table: warehouse_orders — 45 rows, partitioned by region (us-west, us-central, us-east)
"""
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict

# ============================================================================
# 45-row dataset: orders partitioned by region
# Columns: id, order_ref, region, product, category, quantity, unit_price, status, order_date
# ============================================================================

rows = [
    # ── us-west (15 rows) ──
    (1,  'ORD-1001', 'us-west',    'Laptop Pro',           'electronics', 2,  Decimal('899.99'), 'fulfilled', '2024-08-01'),
    (2,  'ORD-1002', 'us-west',    'Winter Jacket',        'clothing',    5,  Decimal('129.99'), 'fulfilled', '2024-08-02'),
    (3,  'ORD-1003', 'us-west',    'Wireless Headphones',  'electronics', 10, Decimal('79.99'),  'pending',   '2024-08-03'),
    (4,  'ORD-1004', 'us-west',    'Standing Desk',        'home',        1,  Decimal('549.99'), 'cancelled', '2024-08-04'),
    (5,  'ORD-1005', 'us-west',    'Running Shoes',        'sports',      3,  Decimal('159.99'), 'fulfilled', '2024-08-05'),
    (6,  'ORD-1006', 'us-west',    'Protein Bars (case)',   'food',       20, Decimal('34.99'),  'returned',  '2024-08-06'),
    (7,  'ORD-1007', 'us-west',    'USB-C Hub',            'electronics', 15, Decimal('49.99'),  'pending',   '2024-08-07'),
    (8,  'ORD-1008', 'us-west',    'Silk Scarf',           'clothing',    8,  Decimal('89.99'),  'cancelled', '2024-08-08'),
    (9,  'ORD-1009', 'us-west',    'Air Purifier',         'home',        2,  Decimal('299.99'), 'fulfilled', '2024-08-09'),
    (10, 'ORD-1010', 'us-west',    'Yoga Mat',             'sports',      12, Decimal('39.99'),  'returned',  '2024-08-10'),
    (11, 'ORD-1011', 'us-west',    'Tablet Stand',         'electronics', 6,  Decimal('29.99'),  'fulfilled', '2024-08-11'),
    (12, 'ORD-1012', 'us-west',    'Organic Coffee 5lb',   'food',        10, Decimal('44.99'),  'pending',   '2024-08-12'),
    (13, 'ORD-1013', 'us-west',    'Denim Jacket',         'clothing',    4,  Decimal('199.99'), 'cancelled', '2024-08-13'),
    (14, 'ORD-1014', 'us-west',    'Smart Thermostat',     'home',        3,  Decimal('249.99'), 'fulfilled', '2024-08-14'),
    (15, 'ORD-1015', 'us-west',    'Resistance Bands',     'sports',      25, Decimal('19.99'),  'pending',   '2024-08-15'),

    # ── us-central (15 rows) ──
    (16, 'ORD-1016', 'us-central', 'Monitor 27in',         'electronics', 3,  Decimal('449.99'), 'fulfilled', '2024-08-01'),
    (17, 'ORD-1017', 'us-central', 'Wool Sweater',         'clothing',    7,  Decimal('119.99'), 'pending',   '2024-08-02'),
    (18, 'ORD-1018', 'us-central', 'Robot Vacuum',         'home',        2,  Decimal('399.99'), 'fulfilled', '2024-08-03'),
    (19, 'ORD-1019', 'us-central', 'Dumbbells Pair',       'sports',      4,  Decimal('89.99'),  'cancelled', '2024-08-04'),
    (20, 'ORD-1020', 'us-central', 'Almonds Bulk',         'food',        15, Decimal('24.99'),  'fulfilled', '2024-08-05'),
    (21, 'ORD-1021', 'us-central', 'Mechanical Keyboard',  'electronics', 10, Decimal('149.99'), 'returned',  '2024-08-06'),
    (22, 'ORD-1022', 'us-central', 'Rain Jacket',          'clothing',    6,  Decimal('179.99'), 'fulfilled', '2024-08-07'),
    (23, 'ORD-1023', 'us-central', 'Bookshelf Oak',        'home',        1,  Decimal('349.99'), 'pending',   '2024-08-08'),
    (24, 'ORD-1024', 'us-central', 'Tennis Racket',        'sports',      3,  Decimal('199.99'), 'returned',  '2024-08-09'),
    (25, 'ORD-1025', 'us-central', 'Green Tea 100pk',      'food',        20, Decimal('29.99'),  'fulfilled', '2024-08-10'),
    (26, 'ORD-1026', 'us-central', 'Webcam HD',            'electronics', 8,  Decimal('69.99'),  'cancelled', '2024-08-11'),
    (27, 'ORD-1027', 'us-central', 'Linen Shirt',          'clothing',    5,  Decimal('59.99'),  'pending',   '2024-08-12'),
    (28, 'ORD-1028', 'us-central', 'Plant Pot Set',        'home',        12, Decimal('34.99'),  'fulfilled', '2024-08-13'),
    (29, 'ORD-1029', 'us-central', 'Jump Rope',            'sports',      10, Decimal('14.99'),  'pending',   '2024-08-14'),
    (30, 'ORD-1030', 'us-central', 'Protein Powder',       'food',        6,  Decimal('54.99'),  'cancelled', '2024-08-15'),

    # ── us-east (15 rows) ──
    (31, 'ORD-1031', 'us-east',    'Phone Case Premium',   'electronics', 20, Decimal('39.99'),  'fulfilled', '2024-08-01'),
    (32, 'ORD-1032', 'us-east',    'Hiking Boots',         'clothing',    3,  Decimal('219.99'), 'fulfilled', '2024-08-02'),
    (33, 'ORD-1033', 'us-east',    'LED Desk Lamp',        'home',        8,  Decimal('69.99'),  'pending',   '2024-08-03'),
    (34, 'ORD-1034', 'us-east',    'Basketball',           'sports',      6,  Decimal('29.99'),  'cancelled', '2024-08-04'),
    (35, 'ORD-1035', 'us-east',    'Dried Mango Case',     'food',        25, Decimal('19.99'),  'fulfilled', '2024-08-05'),
    (36, 'ORD-1036', 'us-east',    'Power Bank',           'electronics', 15, Decimal('59.99'),  'returned',  '2024-08-06'),
    (37, 'ORD-1037', 'us-east',    'Polo Shirt',           'clothing',    10, Decimal('49.99'),  'fulfilled', '2024-08-07'),
    (38, 'ORD-1038', 'us-east',    'Shower Head',          'home',        4,  Decimal('79.99'),  'pending',   '2024-08-08'),
    (39, 'ORD-1039', 'us-east',    'Foam Roller',          'sports',      8,  Decimal('24.99'),  'returned',  '2024-08-09'),
    (40, 'ORD-1040', 'us-east',    'Trail Mix Bulk',       'food',        30, Decimal('14.99'),  'fulfilled', '2024-08-10'),
    (41, 'ORD-1041', 'us-east',    'HDMI Cable 6ft',       'electronics', 50, Decimal('12.99'),  'fulfilled', '2024-08-11'),
    (42, 'ORD-1042', 'us-east',    'Canvas Tote',          'clothing',    15, Decimal('34.99'),  'cancelled', '2024-08-12'),
    (43, 'ORD-1043', 'us-east',    'Throw Pillow Set',     'home',        6,  Decimal('44.99'),  'pending',   '2024-08-13'),
    (44, 'ORD-1044', 'us-east',    'Water Bottle',         'sports',      20, Decimal('19.99'),  'cancelled', '2024-08-14'),
    (45, 'ORD-1045', 'us-east',    'Olive Oil 3L',         'food',        5,  Decimal('39.99'),  'pending',   '2024-08-15'),
]

# Column indices
ID, REF, REGION, PRODUCT, CATEGORY, QTY, PRICE, STATUS, DATE = range(9)

def r(val, places=2):
    """Round using ROUND_HALF_UP to match SQL ROUND behavior."""
    return val.quantize(Decimal(f'0.{"0"*places}'), rounding=ROUND_HALF_UP)

def line_total(row):
    return row[QTY] * row[PRICE]

def sum_line_totals(rows_):
    return sum(line_total(row) for row in rows_)

def count_by_status(rows_, status):
    return sum(1 for row in rows_ if row[STATUS] == status)

def count_by_region(rows_, region):
    return [row for row in rows_ if row[REGION] == region]

# ============================================================================
# BASELINE STATE (45 rows)
# ============================================================================
print("=" * 80)
print("BASELINE STATE — 45 rows")
print("=" * 80)

for region in ['us-west', 'us-central', 'us-east']:
    rr = count_by_region(rows, region)
    total = r(sum_line_totals(rr))
    fulfilled = count_by_status(rr, 'fulfilled')
    pending = count_by_status(rr, 'pending')
    cancelled = count_by_status(rr, 'cancelled')
    returned = count_by_status(rr, 'returned')
    print(f"  {region:12s}: count={len(rr)}, line_total={total}, "
          f"fulfilled={fulfilled}, pending={pending}, cancelled={cancelled}, returned={returned}")

grand = r(sum_line_totals(rows))
print(f"\n  Grand total line_total: {grand}")
print(f"  Total fulfilled: {count_by_status(rows, 'fulfilled')}")
print(f"  Total pending:   {count_by_status(rows, 'pending')}")
print(f"  Total cancelled: {count_by_status(rows, 'cancelled')}")
print(f"  Total returned:  {count_by_status(rows, 'returned')}")

# Per-region line totals for baseline ASSERT
print("\n  BASELINE ASSERT values:")
for region in ['us-central', 'us-east', 'us-west']:
    rr = count_by_region(rows, region)
    print(f"    ASSERT VALUE order_count = {len(rr)} WHERE region = '{region}'")
    print(f"    ASSERT VALUE total_value = {r(sum_line_totals(rr))} WHERE region = '{region}'")

# ============================================================================
# STEP 1: Partition-scoped DELETE — cancelled orders from us-west
# ============================================================================
print("\n" + "=" * 80)
print("STEP 1: DELETE cancelled from us-west")
print("=" * 80)

step1_deleted = [row for row in rows if row[REGION] == 'us-west' and row[STATUS] == 'cancelled']
print(f"  Rows deleted: {len(step1_deleted)}")
for row in step1_deleted:
    print(f"    id={row[ID]}, {row[PRODUCT]}, line_total={line_total(row)}")

data_after_step1 = [row for row in rows if not (row[REGION] == 'us-west' and row[STATUS] == 'cancelled')]
print(f"  Total rows after: {len(data_after_step1)}")
print(f"  ASSERT ROW_COUNT = {len(step1_deleted)}  (rows affected)")

# Post step 1 per-region counts
print("\n  Post STEP 1 per-region:")
for region in ['us-central', 'us-east', 'us-west']:
    rr = count_by_region(data_after_step1, region)
    print(f"    ASSERT VALUE order_count = {len(rr)} WHERE region = '{region}'")

# ============================================================================
# STEP 2: Cross-partition DELETE — all returned orders
# ============================================================================
print("\n" + "=" * 80)
print("STEP 2: DELETE all returned orders (cross-partition)")
print("=" * 80)

step2_deleted = [row for row in data_after_step1 if row[STATUS] == 'returned']
print(f"  Rows deleted: {len(step2_deleted)}")
for row in step2_deleted:
    print(f"    id={row[ID]}, region={row[REGION]}, {row[PRODUCT]}, line_total={line_total(row)}")

data_after_step2 = [row for row in data_after_step1 if row[STATUS] != 'returned']
print(f"  Total rows after: {len(data_after_step2)}")
print(f"  ASSERT ROW_COUNT = {len(step2_deleted)}  (rows affected)")

# Post step 2 per-region counts
print("\n  Post STEP 2 per-region:")
for region in ['us-central', 'us-east', 'us-west']:
    rr = count_by_region(data_after_step2, region)
    total = r(sum_line_totals(rr))
    print(f"    {region}: count={len(rr)}, total_value={total}")
    print(f"    ASSERT VALUE order_count = {len(rr)} WHERE region = '{region}'")
    print(f"    ASSERT VALUE total_value = {total} WHERE region = '{region}'")

# Verify no returned remain
returned_after = count_by_status(data_after_step2, 'returned')
print(f"\n  ASSERT VALUE returned_count = {returned_after}  (should be 0)")

# ============================================================================
# STEP 3: Partition-scoped DELETE — low-value pending from us-east
# ============================================================================
print("\n" + "=" * 80)
print("STEP 3: DELETE low-value pending from us-east (line_total < 500)")
print("=" * 80)

step3_deleted = [row for row in data_after_step2
                 if row[REGION] == 'us-east' and row[STATUS] == 'pending'
                 and line_total(row) < 500]
step3_kept = [row for row in data_after_step2
              if row[REGION] == 'us-east' and row[STATUS] == 'pending'
              and line_total(row) >= 500]

print(f"  Pending orders in us-east evaluated:")
for row in [r_ for r_ in data_after_step2 if r_[REGION] == 'us-east' and r_[STATUS] == 'pending']:
    lt = line_total(row)
    action = "DELETED" if lt < 500 else "KEPT"
    print(f"    id={row[ID]}, {row[PRODUCT]}, qty={row[QTY]} * price={row[PRICE]} = {lt} -> {action}")

print(f"\n  Rows deleted: {len(step3_deleted)}")
print(f"  Rows kept (>= 500): {len(step3_kept)}")

data_after_step3 = [row for row in data_after_step2
                    if not (row[REGION] == 'us-east' and row[STATUS] == 'pending'
                            and line_total(row) < 500)]
print(f"  Total rows after: {len(data_after_step3)}")
print(f"  ASSERT ROW_COUNT = {len(step3_deleted)}  (rows affected)")

# ============================================================================
# FINAL STATE
# ============================================================================
print("\n" + "=" * 80)
print("FINAL STATE")
print("=" * 80)

print(f"\n  Total rows: {len(data_after_step3)}")
print(f"  ASSERT ROW_COUNT = {len(data_after_step3)}  (total)")

print("\n  Per-region final:")
for region in ['us-central', 'us-east', 'us-west']:
    rr = count_by_region(data_after_step3, region)
    total = r(sum_line_totals(rr))
    fulfilled = count_by_status(rr, 'fulfilled')
    pending = count_by_status(rr, 'pending')
    cancelled = count_by_status(rr, 'cancelled')
    print(f"    {region}: count={len(rr)}, total_value={total}, "
          f"fulfilled={fulfilled}, pending={pending}, cancelled={cancelled}")
    print(f"    ASSERT VALUE order_count = {len(rr)} WHERE region = '{region}'")
    print(f"    ASSERT VALUE total_value = {total} WHERE region = '{region}'")

# Status breakdown
print("\n  Per-status final:")
for status in ['fulfilled', 'pending', 'cancelled']:
    cnt = count_by_status(data_after_step3, status)
    print(f"    {status}: {cnt}")

# Verify deleted rows are gone
all_deleted_ids = [row[ID] for row in step1_deleted + step2_deleted + step3_deleted]
print(f"\n  All deleted IDs: {all_deleted_ids}")
remaining_deleted = [row for row in data_after_step3 if row[ID] in all_deleted_ids]
print(f"  ASSERT VALUE cnt = {len(remaining_deleted)}  (should be 0, deleted IDs gone)")

# Grand total value
final_grand = r(sum_line_totals(data_after_step3))
print(f"\n  Grand total line_value: {final_grand}")

# Verify specific rows
print("\n  Spot checks:")
for check_id in [1, 3, 7, 16, 23, 31, 33]:
    match = [row for row in data_after_step3 if row[ID] == check_id]
    if match:
        row = match[0]
        print(f"    id={row[ID]}: region={row[REGION]}, status={row[STATUS]}, "
              f"line_total={line_total(row)} — PRESENT")
    else:
        print(f"    id={check_id}: DELETED")

# ============================================================================
# SANITY CHECKS
# ============================================================================
print("\n" + "=" * 80)
print("SANITY CHECKS")
print("=" * 80)

assert len(rows) == 45, f"Expected 45 rows, got {len(rows)}"
assert len(data_after_step1) == 42, f"After step1: expected 42, got {len(data_after_step1)}"
assert len(data_after_step2) == 36, f"After step2: expected 36, got {len(data_after_step2)}"
assert len(data_after_step3) == 33, f"After step3: expected 33, got {len(data_after_step3)}"
assert len(step1_deleted) == 3, f"Step1 deleted: expected 3, got {len(step1_deleted)}"
assert len(step2_deleted) == 6, f"Step2 deleted: expected 6, got {len(step2_deleted)}"
assert len(step3_deleted) == 3, f"Step3 deleted: expected 3, got {len(step3_deleted)}"
print("All sanity checks passed!")

# Verify no duplicate IDs
all_ids = [row[ID] for row in rows]
assert len(all_ids) == len(set(all_ids)), "Duplicate IDs found!"
print("No duplicate IDs!")

# Verify each region has exactly 15 rows at baseline
for region in ['us-west', 'us-central', 'us-east']:
    cnt = len(count_by_region(rows, region))
    assert cnt == 15, f"{region} expected 15 rows, got {cnt}"
print("Region distribution correct (15 each)!")
