#!/usr/bin/env python3
"""Independent verification of all ASSERT values in queries.sql."""
from decimal import Decimal, ROUND_HALF_UP

rows = [
    (1,  'ORD-1001', 'us-west',    'Laptop Pro',          'electronics', 2,  Decimal('899.99'), 'fulfilled', '2024-08-01'),
    (2,  'ORD-1002', 'us-west',    'Winter Jacket',       'clothing',    5,  Decimal('129.99'), 'fulfilled', '2024-08-02'),
    (3,  'ORD-1003', 'us-west',    'Wireless Headphones', 'electronics', 10, Decimal('79.99'),  'pending',   '2024-08-03'),
    (4,  'ORD-1004', 'us-west',    'Standing Desk',       'home',        1,  Decimal('549.99'), 'cancelled', '2024-08-04'),
    (5,  'ORD-1005', 'us-west',    'Running Shoes',       'sports',      3,  Decimal('159.99'), 'fulfilled', '2024-08-05'),
    (6,  'ORD-1006', 'us-west',    'Protein Bars (case)', 'food',       20, Decimal('34.99'),  'returned',  '2024-08-06'),
    (7,  'ORD-1007', 'us-west',    'USB-C Hub',           'electronics', 15, Decimal('49.99'),  'pending',   '2024-08-07'),
    (8,  'ORD-1008', 'us-west',    'Silk Scarf',          'clothing',    8,  Decimal('89.99'),  'cancelled', '2024-08-08'),
    (9,  'ORD-1009', 'us-west',    'Air Purifier',        'home',        2,  Decimal('299.99'), 'fulfilled', '2024-08-09'),
    (10, 'ORD-1010', 'us-west',    'Yoga Mat',            'sports',      12, Decimal('39.99'),  'returned',  '2024-08-10'),
    (11, 'ORD-1011', 'us-west',    'Tablet Stand',        'electronics', 6,  Decimal('29.99'),  'fulfilled', '2024-08-11'),
    (12, 'ORD-1012', 'us-west',    'Organic Coffee 5lb',  'food',        10, Decimal('44.99'),  'pending',   '2024-08-12'),
    (13, 'ORD-1013', 'us-west',    'Denim Jacket',        'clothing',    4,  Decimal('199.99'), 'cancelled', '2024-08-13'),
    (14, 'ORD-1014', 'us-west',    'Smart Thermostat',    'home',        3,  Decimal('249.99'), 'fulfilled', '2024-08-14'),
    (15, 'ORD-1015', 'us-west',    'Resistance Bands',    'sports',      25, Decimal('19.99'),  'pending',   '2024-08-15'),
    (16, 'ORD-1016', 'us-central', 'Monitor 27in',        'electronics', 3,  Decimal('449.99'), 'fulfilled', '2024-08-01'),
    (17, 'ORD-1017', 'us-central', 'Wool Sweater',        'clothing',    7,  Decimal('119.99'), 'pending',   '2024-08-02'),
    (18, 'ORD-1018', 'us-central', 'Robot Vacuum',        'home',        2,  Decimal('399.99'), 'fulfilled', '2024-08-03'),
    (19, 'ORD-1019', 'us-central', 'Dumbbells Pair',      'sports',      4,  Decimal('89.99'),  'cancelled', '2024-08-04'),
    (20, 'ORD-1020', 'us-central', 'Almonds Bulk',        'food',        15, Decimal('24.99'),  'fulfilled', '2024-08-05'),
    (21, 'ORD-1021', 'us-central', 'Mechanical Keyboard', 'electronics', 10, Decimal('149.99'), 'returned',  '2024-08-06'),
    (22, 'ORD-1022', 'us-central', 'Rain Jacket',         'clothing',    6,  Decimal('179.99'), 'fulfilled', '2024-08-07'),
    (23, 'ORD-1023', 'us-central', 'Bookshelf Oak',       'home',        1,  Decimal('349.99'), 'pending',   '2024-08-08'),
    (24, 'ORD-1024', 'us-central', 'Tennis Racket',       'sports',      3,  Decimal('199.99'), 'returned',  '2024-08-09'),
    (25, 'ORD-1025', 'us-central', 'Green Tea 100pk',     'food',        20, Decimal('29.99'),  'fulfilled', '2024-08-10'),
    (26, 'ORD-1026', 'us-central', 'Webcam HD',           'electronics', 8,  Decimal('69.99'),  'cancelled', '2024-08-11'),
    (27, 'ORD-1027', 'us-central', 'Linen Shirt',         'clothing',    5,  Decimal('59.99'),  'pending',   '2024-08-12'),
    (28, 'ORD-1028', 'us-central', 'Plant Pot Set',       'home',        12, Decimal('34.99'),  'fulfilled', '2024-08-13'),
    (29, 'ORD-1029', 'us-central', 'Jump Rope',           'sports',      10, Decimal('14.99'),  'pending',   '2024-08-14'),
    (30, 'ORD-1030', 'us-central', 'Protein Powder',      'food',        6,  Decimal('54.99'),  'cancelled', '2024-08-15'),
    (31, 'ORD-1031', 'us-east',    'Phone Case Premium',  'electronics', 20, Decimal('39.99'),  'fulfilled', '2024-08-01'),
    (32, 'ORD-1032', 'us-east',    'Hiking Boots',        'clothing',    3,  Decimal('219.99'), 'fulfilled', '2024-08-02'),
    (33, 'ORD-1033', 'us-east',    'LED Desk Lamp',       'home',        8,  Decimal('69.99'),  'pending',   '2024-08-03'),
    (34, 'ORD-1034', 'us-east',    'Basketball',          'sports',      6,  Decimal('29.99'),  'cancelled', '2024-08-04'),
    (35, 'ORD-1035', 'us-east',    'Dried Mango Case',    'food',        25, Decimal('19.99'),  'fulfilled', '2024-08-05'),
    (36, 'ORD-1036', 'us-east',    'Power Bank',          'electronics', 15, Decimal('59.99'),  'returned',  '2024-08-06'),
    (37, 'ORD-1037', 'us-east',    'Polo Shirt',          'clothing',    10, Decimal('49.99'),  'fulfilled', '2024-08-07'),
    (38, 'ORD-1038', 'us-east',    'Shower Head',         'home',        4,  Decimal('79.99'),  'pending',   '2024-08-08'),
    (39, 'ORD-1039', 'us-east',    'Foam Roller',         'sports',      8,  Decimal('24.99'),  'returned',  '2024-08-09'),
    (40, 'ORD-1040', 'us-east',    'Trail Mix Bulk',      'food',        30, Decimal('14.99'),  'fulfilled', '2024-08-10'),
    (41, 'ORD-1041', 'us-east',    'HDMI Cable 6ft',      'electronics', 50, Decimal('12.99'),  'fulfilled', '2024-08-11'),
    (42, 'ORD-1042', 'us-east',    'Canvas Tote',         'clothing',    15, Decimal('34.99'),  'cancelled', '2024-08-12'),
    (43, 'ORD-1043', 'us-east',    'Throw Pillow Set',    'home',        6,  Decimal('44.99'),  'pending',   '2024-08-13'),
    (44, 'ORD-1044', 'us-east',    'Water Bottle',        'sports',      20, Decimal('19.99'),  'cancelled', '2024-08-14'),
    (45, 'ORD-1045', 'us-east',    'Olive Oil 3L',        'food',        5,  Decimal('39.99'),  'pending',   '2024-08-15'),
]

ID, REF, REGION, PRODUCT, CATEGORY, QTY, PRICE, STATUS, DATE = range(9)

def r(val, places=2):
    return val.quantize(Decimal(f'0.{"0"*places}'), rounding=ROUND_HALF_UP)

def lt(row): return row[QTY] * row[PRICE]
def by_region(d, reg): return [x for x in d if x[REGION] == reg]
def by_status(d, st): return [x for x in d if x[STATUS] == st]

# V1 = baseline after INSERT
data_v1 = list(rows)
print("=== BASELINE (V1) ===")
print(f"Total: {len(data_v1)}")
for reg in ['us-central', 'us-east', 'us-west']:
    rr = by_region(data_v1, reg)
    tv = r(sum(lt(x) for x in rr))
    print(f"  {reg}: count={len(rr)}, total_value={tv}")

# V2: DELETE cancelled from us-west
data_v2 = [x for x in data_v1 if not (x[REGION] == 'us-west' and x[STATUS] == 'cancelled')]
print(f"\n=== STEP 1 DELETE (V2) ===")
print(f"Deleted: {len(data_v1) - len(data_v2)}")
print(f"Total: {len(data_v2)}")
for reg in ['us-central', 'us-east', 'us-west']:
    rr = by_region(data_v2, reg)
    print(f"  {reg}: count={len(rr)}, cancelled={len(by_status(rr, 'cancelled'))}")
print(f"  removed (V1-V2): {len(data_v1) - len(data_v2)}")

# V3: DELETE all returned
data_v3 = [x for x in data_v2 if x[STATUS] != 'returned']
print(f"\n=== STEP 2 DELETE (V3) ===")
print(f"Deleted: {len(data_v2) - len(data_v3)}")
print(f"Total: {len(data_v3)}")
for reg in ['us-central', 'us-east', 'us-west']:
    rr = by_region(data_v3, reg)
    tv = r(sum(lt(x) for x in rr))
    ret = len(by_status(rr, 'returned'))
    print(f"  {reg}: count={len(rr)}, total_value={tv}, returned={ret}")
print(f"  returned_count (all): {len(by_status(data_v3, 'returned'))}")

# V4: DELETE low-value pending from us-east
data_v4 = [x for x in data_v3 if not (x[REGION] == 'us-east' and x[STATUS] == 'pending' and lt(x) < 500)]
print(f"\n=== STEP 3 DELETE (V4) ===")
print(f"Deleted: {len(data_v3) - len(data_v4)}")
print(f"Total: {len(data_v4)}")
for reg in ['us-central', 'us-east', 'us-west']:
    rr = by_region(data_v4, reg)
    pending = len(by_status(rr, 'pending'))
    print(f"  {reg}: count={len(rr)}, pending={pending}")

east_pending = [x for x in data_v4 if x[REGION] == 'us-east' and x[STATUS] == 'pending']
for x in east_pending:
    print(f"  surviving: id={x[ID]}, line_total={r(lt(x))}")

print(f"\n=== VERSION HISTORY ===")
print(f"  v1={len(data_v1)}, v2={len(data_v2)}, v3={len(data_v3)}, v4={len(data_v4)}")

print(f"\n=== VERIFY ===")
print(f"  total_rows: {len(data_v4)}")
for reg in ['us-west', 'us-central', 'us-east']:
    print(f"  {reg}: {len(by_region(data_v4, reg))}")
deleted_ids = [4, 8, 13, 6, 10, 21, 24, 36, 39, 38, 43, 45]
remaining_deleted = [x for x in data_v4 if x[ID] in deleted_ids]
print(f"  deleted_ids_remaining: {len(remaining_deleted)}")
print(f"  returned_remaining: {len(by_status(data_v4, 'returned'))}")
print(f"  pending_remaining: {len(by_status(data_v4, 'pending'))}")
print(f"  fulfilled_remaining: {len(by_status(data_v4, 'fulfilled'))}")
print(f"  final_total_value: {r(sum(lt(x) for x in data_v4))}")
