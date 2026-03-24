#!/usr/bin/env python3
"""
Proof script for delta-merge-dv-lifecycle demo.

Retail product catalog sync: ERP supplier feed merges into product catalog.
MERGE generates deletion vectors, OPTIMIZE materializes them.

Tables:
  product_catalog (40 rows) - target
  supplier_feed   (20 rows) - source: 10 updates, 5 deletes (stock=0), 5 inserts
"""
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict

# ===========================================================================
# TABLE 1: product_catalog — 40 products, 10 per category
# ===========================================================================
# Columns: sku, name, category, price, stock, supplier, last_updated

product_catalog = [
    # --- Electronics (10) ---
    ('ELEC-1001', 'Wireless Bluetooth Earbuds',    'electronics', Decimal('49.99'),  120, 'AudioWave',     '2025-03-01'),
    ('ELEC-1002', '27-Inch 4K Monitor',            'electronics', Decimal('349.99'), 35,  'DisplayPro',    '2025-03-01'),
    ('ELEC-1003', 'Mechanical Gaming Keyboard',    'electronics', Decimal('89.99'),  75,  'KeyTech',       '2025-03-01'),
    ('ELEC-1004', 'USB-C Docking Station',         'electronics', Decimal('129.99'), 50,  'HubConnect',    '2025-03-01'),
    ('ELEC-1005', 'Portable Power Bank 20000mAh',  'electronics', Decimal('39.99'),  200, 'ChargePlus',    '2025-03-01'),
    ('ELEC-1006', 'Noise-Cancelling Headphones',   'electronics', Decimal('199.99'), 40,  'AudioWave',     '2025-03-01'),
    ('ELEC-1007', 'Wireless Ergonomic Mouse',      'electronics', Decimal('34.99'),  150, 'ClickGear',     '2025-03-01'),
    ('ELEC-1008', 'Smart LED Desk Lamp',           'electronics', Decimal('59.99'),  80,  'LumiTech',      '2025-03-01'),
    ('ELEC-1009', 'Webcam 1080p Autofocus',        'electronics', Decimal('69.99'),  65,  'VisionCam',     '2025-03-01'),
    ('ELEC-1010', 'Surge Protector 8-Outlet',      'electronics', Decimal('24.99'),  300, 'PowerGuard',    '2025-03-01'),

    # --- Clothing (10) ---
    ('CLTH-2001', 'Cotton Crew-Neck T-Shirt',      'clothing',    Decimal('19.99'),  500, 'ThreadCo',      '2025-03-01'),
    ('CLTH-2002', 'Slim-Fit Chino Pants',          'clothing',    Decimal('44.99'),  200, 'ThreadCo',      '2025-03-01'),
    ('CLTH-2003', 'Waterproof Rain Jacket',        'clothing',    Decimal('79.99'),  90,  'OutdoorEdge',   '2025-03-01'),
    ('CLTH-2004', 'Merino Wool Sweater',           'clothing',    Decimal('64.99'),  110, 'WoolCraft',     '2025-03-01'),
    ('CLTH-2005', 'Running Shoes Lightweight',     'clothing',    Decimal('109.99'), 70,  'StridePro',     '2025-03-01'),
    ('CLTH-2006', 'Denim Jacket Classic',          'clothing',    Decimal('59.99'),  130, 'ThreadCo',      '2025-03-01'),
    ('CLTH-2007', 'Athletic Shorts Mesh',          'clothing',    Decimal('24.99'),  350, 'StridePro',     '2025-03-01'),
    ('CLTH-2008', 'Flannel Button-Down Shirt',     'clothing',    Decimal('34.99'),  180, 'ThreadCo',      '2025-03-01'),
    ('CLTH-2009', 'Insulated Winter Boots',        'clothing',    Decimal('89.99'),  55,  'OutdoorEdge',   '2025-03-01'),
    ('CLTH-2010', 'Leather Belt Reversible',       'clothing',    Decimal('29.99'),  250, 'LeatherKing',   '2025-03-01'),

    # --- Home (10) ---
    ('HOME-3001', 'Stainless Steel Water Bottle',  'home',        Decimal('22.99'),  400, 'HomeEssentials', '2025-03-01'),
    ('HOME-3002', 'Bamboo Cutting Board Set',      'home',        Decimal('29.99'),  180, 'KitchenCraft',   '2025-03-01'),
    ('HOME-3003', 'Memory Foam Pillow',            'home',        Decimal('39.99'),  150, 'SleepWell',      '2025-03-01'),
    ('HOME-3004', 'Ceramic Coffee Mug Set',        'home',        Decimal('18.99'),  300, 'KitchenCraft',   '2025-03-01'),
    ('HOME-3005', 'Aromatherapy Diffuser',         'home',        Decimal('34.99'),  95,  'ZenHome',        '2025-03-01'),
    ('HOME-3006', 'Cotton Bath Towel Set',         'home',        Decimal('44.99'),  120, 'HomeEssentials', '2025-03-01'),
    ('HOME-3007', 'Wall-Mounted Shelf Set',        'home',        Decimal('54.99'),  60,  'WoodWorks',      '2025-03-01'),
    ('HOME-3008', 'Non-Stick Cookware Set',        'home',        Decimal('89.99'),  45,  'KitchenCraft',   '2025-03-01'),
    ('HOME-3009', 'LED String Lights 50ft',        'home',        Decimal('16.99'),  250, 'LumiTech',       '2025-03-01'),
    ('HOME-3010', 'Vacuum Storage Bags 10-Pack',   'home',        Decimal('14.99'),  200, 'HomeEssentials', '2025-03-01'),

    # --- Food (10) ---
    ('FOOD-4001', 'Organic Coffee Beans 2lb',      'food',        Decimal('24.99'),  350, 'BeanOrigin',     '2025-03-01'),
    ('FOOD-4002', 'Extra Virgin Olive Oil 1L',     'food',        Decimal('16.99'),  280, 'MedHarvest',     '2025-03-01'),
    ('FOOD-4003', 'Raw Honey Wildflower 32oz',     'food',        Decimal('14.99'),  220, 'NaturePure',     '2025-03-01'),
    ('FOOD-4004', 'Dried Mango Slices 1lb',        'food',        Decimal('9.99'),   400, 'TropiFruit',     '2025-03-01'),
    ('FOOD-4005', 'Quinoa Grain Organic 5lb',      'food',        Decimal('18.99'),  160, 'GrainWorks',     '2025-03-01'),
    ('FOOD-4006', 'Dark Chocolate Bar 85%',        'food',        Decimal('5.99'),   600, 'CocoaCraft',     '2025-03-01'),
    ('FOOD-4007', 'Matcha Green Tea Powder',       'food',        Decimal('29.99'),  140, 'TeaLeaf',        '2025-03-01'),
    ('FOOD-4008', 'Mixed Nut Butter 16oz',         'food',        Decimal('12.99'),  250, 'NutHouse',       '2025-03-01'),
    ('FOOD-4009', 'Sparkling Water Variety 24pk',  'food',        Decimal('11.99'),  180, 'FizzCo',         '2025-03-01'),
    ('FOOD-4010', 'Protein Bar Sampler 12-Pack',   'food',        Decimal('22.99'),  300, 'FitFuel',        '2025-03-01'),
]

# ===========================================================================
# TABLE 2: supplier_feed — 20 rows
# ===========================================================================
# 10 updates (matched, stock>0): price and/or stock changes
# 5  deletes  (matched, stock=0): discontinued items
# 5  inserts  (not matched): new products

supplier_feed = [
    # --- 10 UPDATES: matched SKUs with new price/stock ---
    # ELEC-1001: price drop 49.99→44.99, stock 120→140
    ('ELEC-1001', 'Wireless Bluetooth Earbuds',    'electronics', Decimal('44.99'),  140, 'AudioWave',     '2025-03-15'),
    # ELEC-1005: price drop 39.99→34.99, stock 200→250
    ('ELEC-1005', 'Portable Power Bank 20000mAh',  'electronics', Decimal('34.99'),  250, 'ChargePlus',    '2025-03-15'),
    # ELEC-1008: price up 59.99→64.99, stock 80→60
    ('ELEC-1008', 'Smart LED Desk Lamp',           'electronics', Decimal('64.99'),  60,  'LumiTech',      '2025-03-15'),
    # CLTH-2002: price up 44.99→49.99, stock 200→175
    ('CLTH-2002', 'Slim-Fit Chino Pants',          'clothing',    Decimal('49.99'),  175, 'ThreadCo',      '2025-03-15'),
    # CLTH-2005: price drop 109.99→99.99, stock 70→85
    ('CLTH-2005', 'Running Shoes Lightweight',     'clothing',    Decimal('99.99'),  85,  'StridePro',     '2025-03-15'),
    # HOME-3003: same price, stock 150→180
    ('HOME-3003', 'Memory Foam Pillow',            'home',        Decimal('39.99'),  180, 'SleepWell',     '2025-03-15'),
    # HOME-3006: price up 44.99→49.99, stock 120→100
    ('HOME-3006', 'Cotton Bath Towel Set',         'home',        Decimal('49.99'),  100, 'HomeEssentials', '2025-03-15'),
    # FOOD-4001: price up 24.99→27.99, stock 350→320
    ('FOOD-4001', 'Organic Coffee Beans 2lb',      'food',        Decimal('27.99'),  320, 'BeanOrigin',    '2025-03-15'),
    # FOOD-4006: same price, stock 600→700
    ('FOOD-4006', 'Dark Chocolate Bar 85%',        'food',        Decimal('5.99'),   700, 'CocoaCraft',    '2025-03-15'),
    # FOOD-4009: price drop 11.99→10.99, stock 180→220
    ('FOOD-4009', 'Sparkling Water Variety 24pk',  'food',        Decimal('10.99'),  220, 'FizzCo',        '2025-03-15'),

    # --- 5 DELETES: matched SKUs with stock=0 (discontinued) ---
    ('ELEC-1009', 'Webcam 1080p Autofocus',        'electronics', Decimal('69.99'),  0,   'VisionCam',     '2025-03-15'),
    ('CLTH-2008', 'Flannel Button-Down Shirt',     'clothing',    Decimal('34.99'),  0,   'ThreadCo',      '2025-03-15'),
    ('HOME-3009', 'LED String Lights 50ft',        'home',        Decimal('16.99'),  0,   'LumiTech',      '2025-03-15'),
    ('FOOD-4003', 'Raw Honey Wildflower 32oz',     'food',        Decimal('14.99'),  0,   'NaturePure',    '2025-03-15'),
    ('FOOD-4008', 'Mixed Nut Butter 16oz',         'food',        Decimal('12.99'),  0,   'NutHouse',      '2025-03-15'),

    # --- 5 INSERTS: new SKUs not in catalog ---
    ('ELEC-1011', 'Wireless Charging Pad',         'electronics', Decimal('29.99'),  100, 'ChargePlus',    '2025-03-15'),
    ('CLTH-2011', 'UV-Protection Sunglasses',      'clothing',    Decimal('39.99'),  160, 'OutdoorEdge',   '2025-03-15'),
    ('HOME-3011', 'Cast Iron Skillet 12-Inch',     'home',        Decimal('44.99'),  80,  'KitchenCraft',  '2025-03-15'),
    ('FOOD-4011', 'Organic Maple Syrup 16oz',      'food',        Decimal('13.99'),  200, 'NaturePure',    '2025-03-15'),
    ('FOOD-4012', 'Cold Brew Coffee Concentrate',  'food',        Decimal('19.99'),  150, 'BeanOrigin',    '2025-03-15'),
]

# ===========================================================================
# Helper functions
# ===========================================================================
def avg_price(rows):
    """Average price rounded to 2 decimal places."""
    prices = [r[3] for r in rows]
    return (sum(prices) / len(prices)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

def category_rows(rows, cat):
    return [r for r in rows if r[2] == cat]

def total_stock(rows):
    return sum(r[4] for r in rows)

def sql_val(v):
    """Format a value for SQL INSERT."""
    if isinstance(v, Decimal):
        return str(v)
    elif isinstance(v, int):
        return str(v)
    elif isinstance(v, str):
        return f"'{v}'"
    return str(v)

def sql_row(row):
    return f"    ({', '.join(sql_val(v) for v in row)})"

# ===========================================================================
# PRE-MERGE: Baseline assertions
# ===========================================================================
print("=" * 80)
print("PRE-MERGE BASELINE — product_catalog (40 rows)")
print("=" * 80)

assert len(product_catalog) == 40
categories = ['electronics', 'clothing', 'home', 'food']
for cat in categories:
    cr = category_rows(product_catalog, cat)
    print(f"  {cat}: count={len(cr)}, avg_price={avg_price(cr)}, total_stock={total_stock(cr)}")

# Baseline category aggregates
baseline_agg = {}
for cat in categories:
    cr = category_rows(product_catalog, cat)
    baseline_agg[cat] = {
        'count': len(cr),
        'avg_price': avg_price(cr),
        'total_stock': total_stock(cr),
    }

print(f"\n  Total products: {len(product_catalog)}")
print(f"  Total stock: {total_stock(product_catalog)}")
overall_avg = avg_price(product_catalog)
print(f"  Overall avg price: {overall_avg}")

# ===========================================================================
# SUPPLIER FEED PREVIEW
# ===========================================================================
print("\n" + "=" * 80)
print("SUPPLIER FEED PREVIEW — supplier_feed (20 rows)")
print("=" * 80)

catalog_skus = {r[0] for r in product_catalog}
feed_skus = {r[0] for r in supplier_feed}

updates = [r for r in supplier_feed if r[0] in catalog_skus and r[4] > 0]
deletes = [r for r in supplier_feed if r[0] in catalog_skus and r[4] == 0]
inserts = [r for r in supplier_feed if r[0] not in catalog_skus]

print(f"  Updates (matched, stock>0): {len(updates)} rows")
print(f"  Deletes (matched, stock=0): {len(deletes)} rows")
print(f"  Inserts (not matched):      {len(inserts)} rows")
assert len(updates) == 10
assert len(deletes) == 5
assert len(inserts) == 5
assert len(supplier_feed) == 20

print("\n  SKUs to UPDATE:", [r[0] for r in updates])
print("  SKUs to DELETE:", [r[0] for r in deletes])
print("  SKUs to INSERT:", [r[0] for r in inserts])

# ===========================================================================
# SIMULATE MERGE
# ===========================================================================
print("\n" + "=" * 80)
print("MERGE SIMULATION")
print("=" * 80)

# Build catalog as dict keyed by SKU
catalog_dict = {r[0]: list(r) for r in product_catalog}
feed_dict = {r[0]: list(r) for r in supplier_feed}

merge_updated = 0
merge_deleted = 0
merge_inserted = 0
deleted_skus = []

for sku, feed_row in feed_dict.items():
    if sku in catalog_dict:
        if feed_row[4] > 0:
            # WHEN MATCHED AND source.stock > 0 THEN UPDATE
            catalog_dict[sku] = feed_row
            merge_updated += 1
        else:
            # WHEN MATCHED AND source.stock = 0 THEN DELETE
            deleted_skus.append(sku)
            del catalog_dict[sku]
            merge_deleted += 1
    else:
        # WHEN NOT MATCHED THEN INSERT
        catalog_dict[sku] = feed_row
        merge_inserted += 1

total_affected = merge_updated + merge_deleted + merge_inserted
print(f"  Rows updated:  {merge_updated}")
print(f"  Rows deleted:  {merge_deleted}")
print(f"  Rows inserted: {merge_inserted}")
print(f"  Total affected (MERGE row_count): {total_affected}")
print(f"  Deleted SKUs: {deleted_skus}")

# Post-merge catalog as list
post_merge = [tuple(v) for v in catalog_dict.values()]
post_merge.sort(key=lambda r: r[0])

print(f"\n  Post-merge total rows: {len(post_merge)}")
assert len(post_merge) == 40  # 40 - 5 + 5 = 40

# ===========================================================================
# POST-MERGE ASSERTIONS
# ===========================================================================
print("\n" + "=" * 80)
print("POST-MERGE ASSERTIONS")
print("=" * 80)

# Per-category post-merge
post_agg = {}
for cat in categories:
    cr = category_rows(post_merge, cat)
    ap = avg_price(cr)
    ts = total_stock(cr)
    post_agg[cat] = {'count': len(cr), 'avg_price': ap, 'total_stock': ts}
    print(f"  {cat}: count={len(cr)}, avg_price={ap}, total_stock={ts}")

print(f"\n  Total products: {len(post_merge)}")
print(f"  Total stock: {total_stock(post_merge)}")
post_overall_avg = avg_price(post_merge)
print(f"  Overall avg price: {post_overall_avg}")

# Verify specific updated prices
print("\n  SPECIFIC PRICE CHECKS:")
for sku in ['ELEC-1001', 'ELEC-1005', 'CLTH-2005', 'FOOD-4001']:
    row = catalog_dict[sku]
    print(f"    {sku} ({row[1]}): price={row[3]}, stock={row[4]}")

# Verify deletions
print("\n  DELETION CHECKS:")
for sku in deleted_skus:
    assert sku not in catalog_dict
    print(f"    {sku} — confirmed absent from catalog")

# Verify new inserts
print("\n  INSERT CHECKS:")
for sku in ['ELEC-1011', 'CLTH-2011', 'HOME-3011', 'FOOD-4011', 'FOOD-4012']:
    row = catalog_dict[sku]
    print(f"    {sku} ({row[1]}): price={row[3]}, stock={row[4]}")

# Count unchanged rows
unchanged_count = 0
for r in post_merge:
    if r[6] == '2025-03-01':  # last_updated unchanged
        unchanged_count += 1
updated_or_new = len(post_merge) - unchanged_count
print(f"\n  Unchanged rows: {unchanged_count}")
print(f"  Updated or new rows (date=2025-03-15): {updated_or_new}")

# ===========================================================================
# FINAL VERIFY aggregates
# ===========================================================================
print("\n" + "=" * 80)
print("FINAL VERIFY SECTION VALUES")
print("=" * 80)

# deleted count check
deleted_sku_check_count = sum(1 for r in post_merge if r[0] in deleted_skus)
print(f"  Deleted SKUs remaining: {deleted_sku_check_count} (expect 0)")

# new products count
new_sku_count = sum(1 for r in post_merge if r[0] in ['ELEC-1011', 'CLTH-2011', 'HOME-3011', 'FOOD-4011', 'FOOD-4012'])
print(f"  New SKUs present: {new_sku_count} (expect 5)")

# specific values for ASSERT
earbuds_price = catalog_dict['ELEC-1001'][3]
earbuds_stock = catalog_dict['ELEC-1001'][4]
print(f"  ELEC-1001 price: {earbuds_price} (expect 44.99)")
print(f"  ELEC-1001 stock: {earbuds_stock} (expect 140)")

coffee_price = catalog_dict['FOOD-4001'][3]
coffee_stock = catalog_dict['FOOD-4001'][4]
print(f"  FOOD-4001 price: {coffee_price} (expect 27.99)")
print(f"  FOOD-4001 stock: {coffee_stock} (expect 320)")

shoes_price = catalog_dict['CLTH-2005'][3]
print(f"  CLTH-2005 price: {shoes_price} (expect 99.99)")

charging_pad_price = catalog_dict['ELEC-1011'][3]
print(f"  ELEC-1011 price: {charging_pad_price} (expect 29.99)")

maple_syrup_stock = catalog_dict['FOOD-4011'][4]
print(f"  FOOD-4011 stock: {maple_syrup_stock} (expect 200)")

# Confirm category counts post-merge
print(f"\n  electronics count: {post_agg['electronics']['count']}")
print(f"  clothing count:    {post_agg['clothing']['count']}")
print(f"  home count:        {post_agg['home']['count']}")
print(f"  food count:        {post_agg['food']['count']}")

# ===========================================================================
# GENERATE SQL INSERT STATEMENTS
# ===========================================================================
print("\n" + "=" * 80)
print("SQL INSERT — product_catalog")
print("=" * 80)
print("INSERT INTO {{zone_name}}.delta_demos.product_catalog VALUES")
lines = []
for r in product_catalog:
    lines.append(sql_row(r))
print(",\n".join(lines) + ";")

print("\n" + "=" * 80)
print("SQL INSERT — supplier_feed")
print("=" * 80)
print("INSERT INTO {{zone_name}}.delta_demos.supplier_feed VALUES")
lines = []
for r in supplier_feed:
    lines.append(sql_row(r))
print(",\n".join(lines) + ";")

# ===========================================================================
# DESCRIBE HISTORY expected row counts
# ===========================================================================
# v0: CREATE product_catalog
# v1: INSERT product_catalog
# v2: CREATE supplier_feed
# v3: INSERT supplier_feed
# v4: MERGE
# After OPTIMIZE on product_catalog:
# product_catalog history: CREATE(v0), INSERT(v1), MERGE(v2), OPTIMIZE(v3) = 4 rows
# We only DESCRIBE HISTORY on product_catalog
print("\n" + "=" * 80)
print("DESCRIBE HISTORY — product_catalog")
print("=" * 80)
print("  Expected versions: v0 (CREATE), v1 (INSERT), v2 (MERGE), v3 (OPTIMIZE) = 4 rows")

# ===========================================================================
# SUMMARY OF ALL ASSERT VALUES
# ===========================================================================
print("\n" + "=" * 80)
print("COMPLETE ASSERT VALUE REFERENCE")
print("=" * 80)

print("""
--- BASELINE (pre-merge) ---
product_catalog ROW_COUNT = 40
  category ROW_COUNT = 4 (4 categories)
  electronics: product_count=10, avg_price={elec_base_avg}, total_stock={elec_base_stock}
  clothing:    product_count=10, avg_price={clth_base_avg}, total_stock={clth_base_stock}
  home:        product_count=10, avg_price={home_base_avg}, total_stock={home_base_stock}
  food:        product_count=10, avg_price={food_base_avg}, total_stock={food_base_stock}

--- SUPPLIER FEED PREVIEW ---
supplier_feed ROW_COUNT = 20
  feed_updates (stock > 0 AND sku in catalog) = 10
  feed_deletes (stock = 0) = 5
  feed_inserts (sku not in catalog) = 5

--- MERGE ---
MERGE ROW_COUNT = 20

--- POST-MERGE ---
product_catalog ROW_COUNT = 40
  electronics: product_count={elec_post_count}, avg_price={elec_post_avg}, total_stock={elec_post_stock}
  clothing:    product_count={clth_post_count}, avg_price={clth_post_avg}, total_stock={clth_post_stock}
  home:        product_count={home_post_count}, avg_price={home_post_avg}, total_stock={home_post_stock}
  food:        product_count={food_post_count}, avg_price={food_post_avg}, total_stock={food_post_stock}

--- SPECIFIC VALUES ---
  ELEC-1001 price = {earbuds_p}, stock = {earbuds_s}
  FOOD-4001 price = {coffee_p}, stock = {coffee_s}
  CLTH-2005 price = {shoes_p}
  Deleted SKUs remaining = 0
  New SKUs count = 5
  Unchanged rows = {unchanged}
  Updated/new rows (2025-03-15) = {updated_new}

--- DESCRIBE HISTORY ---
  product_catalog history ROW_COUNT = 4
""".format(
    elec_base_avg=baseline_agg['electronics']['avg_price'],
    elec_base_stock=baseline_agg['electronics']['total_stock'],
    clth_base_avg=baseline_agg['clothing']['avg_price'],
    clth_base_stock=baseline_agg['clothing']['total_stock'],
    home_base_avg=baseline_agg['home']['avg_price'],
    home_base_stock=baseline_agg['home']['total_stock'],
    food_base_avg=baseline_agg['food']['avg_price'],
    food_base_stock=baseline_agg['food']['total_stock'],
    elec_post_count=post_agg['electronics']['count'],
    elec_post_avg=post_agg['electronics']['avg_price'],
    elec_post_stock=post_agg['electronics']['total_stock'],
    clth_post_count=post_agg['clothing']['count'],
    clth_post_avg=post_agg['clothing']['avg_price'],
    clth_post_stock=post_agg['clothing']['total_stock'],
    home_post_count=post_agg['home']['count'],
    home_post_avg=post_agg['home']['avg_price'],
    home_post_stock=post_agg['home']['total_stock'],
    food_post_count=post_agg['food']['count'],
    food_post_avg=post_agg['food']['avg_price'],
    food_post_stock=post_agg['food']['total_stock'],
    earbuds_p=earbuds_price,
    earbuds_s=earbuds_stock,
    coffee_p=coffee_price,
    coffee_s=coffee_stock,
    shoes_p=shoes_price,
    unchanged=unchanged_count,
    updated_new=updated_or_new,
))
