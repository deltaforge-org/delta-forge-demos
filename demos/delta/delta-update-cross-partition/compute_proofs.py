#!/usr/bin/env python3
"""
Compute all proof values for the delta-update-cross-partition demo.
SaaS billing platform: subscription pricing changes across ALL regions.
"""
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict

# ============================================================================
# 60-row dataset: subscriptions partitioned by region
# Columns: id, customer, region, plan, monthly_fee, usage_gb, signup_date, status
# ============================================================================

rows = [
    # ── americas (20 rows) ──
    (1,  'Acme Corp',          'americas', 'starter',       Decimal('29.99'),  50,   '2024-01-15', 'active'),
    (2,  'Beta Industries',    'americas', 'professional',  Decimal('99.99'),  250,  '2024-02-01', 'active'),
    (3,  'Cloud Nine LLC',     'americas', 'enterprise',    Decimal('299.99'), 2000, '2023-06-10', 'active'),
    (4,  'DataStream Inc',     'americas', 'starter',       Decimal('39.99'),  150,  '2024-03-20', 'trial'),
    (5,  'Eagle Software',     'americas', 'professional',  Decimal('129.99'), 400,  '2023-11-05', 'active'),
    (6,  'FreshBooks Co',      'americas', 'starter',       Decimal('49.99'),  120,  '2024-04-12', 'active'),
    (7,  'GridPower Systems',  'americas', 'enterprise',    Decimal('399.99'), 3500, '2023-03-01', 'active'),
    (8,  'HyperScale Labs',    'americas', 'professional',  Decimal('149.99'), 600,  '2023-09-15', 'active'),
    (9,  'Infinity Analytics', 'americas', 'starter',       Decimal('34.99'),  30,   '2024-05-01', 'trial'),
    (10, 'JetBridge Tech',     'americas', 'professional',  Decimal('109.99'), 320,  '2024-01-22', 'active'),
    (11, 'Keystone Data',      'americas', 'enterprise',    Decimal('449.99'), 4200, '2023-01-10', 'active'),
    (12, 'Lumen Insights',     'americas', 'starter',       Decimal('44.99'),  95,   '2024-06-01', 'active'),
    (13, 'MetaFlow Corp',      'americas', 'professional',  Decimal('119.99'), 180,  '2023-12-18', 'active'),
    (14, 'NexGen Solutions',   'americas', 'starter',       Decimal('29.99'),  15,   '2024-07-10', 'trial'),
    (15, 'Orbit Systems',      'americas', 'enterprise',    Decimal('349.99'), 2800, '2023-05-20', 'active'),
    (16, 'PulseMetrics',       'americas', 'professional',  Decimal('139.99'), 510,  '2024-02-14', 'active'),
    (17, 'QuickLedger Inc',    'americas', 'starter',       Decimal('39.99'),  60,   '2024-08-01', 'active'),
    (18, 'RapidScale AI',      'americas', 'professional',  Decimal('99.99'),  200,  '2023-10-30', 'suspended'),
    (19, 'SkyVault Storage',   'americas', 'enterprise',    Decimal('499.99'), 5000, '2023-02-15', 'active'),
    (20, 'TerraNode Labs',     'americas', 'starter',       Decimal('34.99'),  200,  '2024-09-05', 'trial'),

    # ── europe (20 rows) ──
    (21, 'Albion Digital',     'europe', 'starter',       Decimal('29.99'),  40,   '2024-01-20', 'active'),
    (22, 'BerlinTech GmbH',   'europe', 'professional',  Decimal('119.99'), 350,  '2023-08-15', 'active'),
    (23, 'Cypher Security',   'europe', 'enterprise',    Decimal('349.99'), 2500, '2023-04-01', 'active'),
    (24, 'DublinStack Ltd',   'europe', 'starter',       Decimal('44.99'),  110,  '2024-03-10', 'trial'),
    (25, 'EuroCloud SAS',     'europe', 'professional',  Decimal('139.99'), 450,  '2023-11-20', 'active'),
    (26, 'FluxData BV',       'europe', 'starter',       Decimal('39.99'),  70,   '2024-05-15', 'active'),
    (27, 'GenevaLabs SA',     'europe', 'enterprise',    Decimal('399.99'), 3200, '2023-02-28', 'active'),
    (28, 'Helsinki IO Oy',    'europe', 'professional',  Decimal('109.99'), 280,  '2024-01-05', 'active'),
    (29, 'IstanbulOps AS',    'europe', 'starter',       Decimal('34.99'),  55,   '2024-06-20', 'trial'),
    (30, 'JohannesTech AB',   'europe', 'professional',  Decimal('149.99'), 520,  '2023-09-01', 'active'),
    (31, 'KrakowSoft Sp',     'europe', 'enterprise',    Decimal('449.99'), 4000, '2023-01-15', 'active'),
    (32, 'LisbonAI Lda',      'europe', 'starter',       Decimal('49.99'),  130,  '2024-07-01', 'active'),
    (33, 'MadridFlow SL',     'europe', 'professional',  Decimal('129.99'), 380,  '2024-02-10', 'active'),
    (34, 'NordicEdge AS',     'europe', 'starter',       Decimal('29.99'),  20,   '2024-08-15', 'trial'),
    (35, 'OsloMetrics AS',    'europe', 'enterprise',    Decimal('299.99'), 1800, '2023-06-25', 'active'),
    (36, 'PragueData sro',    'europe', 'professional',  Decimal('99.99'),  150,  '2023-12-01', 'suspended'),
    (37, 'RigaStack SIA',     'europe', 'starter',       Decimal('44.99'),  85,   '2024-04-20', 'active'),
    (38, 'SofiaLabs EOOD',    'europe', 'professional',  Decimal('109.99'), 220,  '2024-03-15', 'active'),
    (39, 'TallinnOps OU',     'europe', 'enterprise',    Decimal('349.99'), 2600, '2023-07-10', 'active'),
    (40, 'UppsalaAI AB',      'europe', 'starter',       Decimal('39.99'),  180,  '2024-09-01', 'trial'),

    # ── asia-pacific (20 rows) ──
    (41, 'AsiaPay Ltd',       'asia-pacific', 'starter',       Decimal('34.99'),  60,   '2024-02-01', 'active'),
    (42, 'BangkokCloud Co',   'asia-pacific', 'professional',  Decimal('99.99'),  200,  '2023-10-15', 'active'),
    (43, 'ChennaiByte Pvt',   'asia-pacific', 'enterprise',    Decimal('299.99'), 1900, '2023-05-01', 'active'),
    (44, 'DelhiScale Pvt',    'asia-pacific', 'starter',       Decimal('29.99'),  25,   '2024-04-10', 'trial'),
    (45, 'ExcelTech Japan',   'asia-pacific', 'professional',  Decimal('149.99'), 550,  '2023-08-20', 'active'),
    (46, 'FujiData KK',       'asia-pacific', 'starter',       Decimal('44.99'),  100,  '2024-06-15', 'active'),
    (47, 'GuangzhouOps',      'asia-pacific', 'enterprise',    Decimal('449.99'), 4500, '2023-01-20', 'active'),
    (48, 'HanoiStack JSC',    'asia-pacific', 'professional',  Decimal('119.99'), 300,  '2024-01-10', 'active'),
    (49, 'IndigoLabs Pte',    'asia-pacific', 'starter',       Decimal('39.99'),  250,  '2024-07-20', 'trial'),
    (50, 'JakartaFlow PT',    'asia-pacific', 'professional',  Decimal('139.99'), 480,  '2023-11-10', 'active'),
    (51, 'KualaOps Sdn',      'asia-pacific', 'enterprise',    Decimal('399.99'), 3800, '2023-03-15', 'active'),
    (52, 'LuzonTech Inc',     'asia-pacific', 'starter',       Decimal('49.99'),  140,  '2024-08-01', 'active'),
    (53, 'MumbaiMetrics',     'asia-pacific', 'professional',  Decimal('129.99'), 420,  '2024-02-20', 'active'),
    (54, 'NanjiSoft Ltd',     'asia-pacific', 'starter',       Decimal('34.99'),  35,   '2024-05-25', 'active'),
    (55, 'OsakaPlatform',     'asia-pacific', 'enterprise',    Decimal('349.99'), 2200, '2023-06-05', 'active'),
    (56, 'PerthData Pty',     'asia-pacific', 'professional',  Decimal('109.99'), 260,  '2023-12-20', 'suspended'),
    (57, 'QuezonEdge Inc',    'asia-pacific', 'starter',       Decimal('29.99'),  120,  '2024-09-10', 'trial'),
    (58, 'RangoonAI Ltd',     'asia-pacific', 'professional',  Decimal('99.99'),  160,  '2024-03-05', 'active'),
    (59, 'SingaporeLabs',     'asia-pacific', 'enterprise',    Decimal('499.99'), 4800, '2023-02-01', 'active'),
    (60, 'TokyoVault KK',     'asia-pacific', 'starter',       Decimal('44.99'),  90,   '2024-04-30', 'active'),
]

def r(val, places=2):
    """Round using ROUND_HALF_UP to match SQL ROUND behavior."""
    return val.quantize(Decimal(f'0.{"0"*places}'), rounding=ROUND_HALF_UP)

# Deep copy for mutation
data = [list(row) for row in rows]
# Column indices
ID, CUSTOMER, REGION, PLAN, FEE, USAGE, SIGNUP, STATUS = range(8)

print("=" * 80)
print("BASELINE STATE (60 rows)")
print("=" * 80)

# ── EXPLORE: Baseline per-region plan distribution ──
print("\n── EXPLORE: Per-region plan distribution ──")
for region in ['americas', 'asia-pacific', 'europe']:
    region_rows = [r for r in data if r[REGION] == region]
    plans = defaultdict(int)
    for r_ in region_rows:
        plans[r_[PLAN]] += 1
    print(f"  {region}: {dict(plans)}, total={len(region_rows)}")

print("\n── EXPLORE: Avg monthly_fee per plan ──")
for plan in ['starter', 'professional', 'enterprise']:
    plan_rows = [r_ for r_ in data if r_[PLAN] == plan]
    avg_fee = r(sum(r_[FEE] for r_ in plan_rows) / len(plan_rows))
    print(f"  {plan}: count={len(plan_rows)}, avg_fee={avg_fee}")

# Count professionals per region (for STEP 1)
print("\n── Professional plan rows per region (pre-update) ──")
for region in ['americas', 'asia-pacific', 'europe']:
    pros = [r_ for r_ in data if r_[REGION] == region and r_[PLAN] == 'professional']
    fees = [r_[FEE] for r_ in pros]
    print(f"  {region}: count={len(pros)}, fees={fees}")

# ============================================================================
# STEP 1: Cross-partition UPDATE — increase fee by 10% for all 'professional'
# ============================================================================
print("\n" + "=" * 80)
print("STEP 1: UPDATE professional fees +10% (cross-partition)")
print("=" * 80)

step1_updated = 0
step1_by_region = defaultdict(int)
for row in data:
    if row[PLAN] == 'professional':
        old_fee = row[FEE]
        new_fee = r(old_fee * Decimal('1.10'))
        row[FEE] = new_fee
        step1_updated += 1
        step1_by_region[row[REGION]] += 1

print(f"  Total rows updated: {step1_updated}")
for region in ['americas', 'asia-pacific', 'europe']:
    print(f"  {region}: {step1_by_region[region]} rows updated")

# ── LEARN: Verify STEP 1 ──
print("\n── LEARN: Updated professional fees per region ──")
for region in ['americas', 'asia-pacific', 'europe']:
    pros = [r_ for r_ in data if r_[REGION] == region and r_[PLAN] == 'professional']
    count = len(pros)
    avg_fee = r(sum(r_[FEE] for r_ in pros) / len(pros))
    min_fee = min(r_[FEE] for r_ in pros)
    max_fee = max(r_[FEE] for r_ in pros)
    print(f"  {region}: count={count}, avg_fee={avg_fee}, min_fee={min_fee}, max_fee={max_fee}")

# Show all professional fees for verification
print("\n── All professional fees after +10% ──")
for row in data:
    if row[PLAN] == 'professional':
        print(f"  id={row[ID]:2d}, region={row[REGION]:13s}, fee={row[FEE]}")

# ============================================================================
# STEP 2: Cross-partition UPDATE — suspend trials with usage > 100gb
# ============================================================================
print("\n" + "=" * 80)
print("STEP 2: UPDATE suspend trial accounts with usage > 100gb (cross-partition)")
print("=" * 80)

step2_updated = 0
step2_by_region = defaultdict(int)
step2_ids = []
for row in data:
    if row[STATUS] == 'trial' and row[USAGE] > 100:
        row[STATUS] = 'suspended'
        step2_updated += 1
        step2_by_region[row[REGION]] += 1
        step2_ids.append(row[ID])

print(f"  Total rows updated: {step2_updated}")
print(f"  IDs affected: {step2_ids}")
for region in ['americas', 'asia-pacific', 'europe']:
    print(f"  {region}: {step2_by_region[region]} rows updated")

# ── LEARN: Verify STEP 2 ──
print("\n── LEARN: Suspended accounts per region ──")
for region in ['americas', 'asia-pacific', 'europe']:
    suspended = [r_ for r_ in data if r_[REGION] == region and r_[STATUS] == 'suspended']
    print(f"  {region}: {len(suspended)} suspended")

print("\n── All trial rows (showing which got suspended) ──")
for row in rows:  # original
    orig_row = [r_ for r_ in data if r_[ID] == row[ID]][0]
    if row[STATUS] == 'trial':
        print(f"  id={row[ID]:2d}, region={row[REGION]:13s}, usage={row[USAGE]:4d}gb, "
              f"status_now={orig_row[STATUS]}, changed={'YES' if orig_row[STATUS] != row[STATUS] else 'no'}")

# ============================================================================
# STEP 3: Partition-aligned UPDATE — 5% discount for enterprise in asia-pacific
# ============================================================================
print("\n" + "=" * 80)
print("STEP 3: UPDATE enterprise asia-pacific fees -5% (partition-aligned)")
print("=" * 80)

step3_updated = 0
step3_by_region = defaultdict(int)
for row in data:
    if row[PLAN] == 'enterprise' and row[REGION] == 'asia-pacific':
        old_fee = row[FEE]
        new_fee = r(old_fee * Decimal('0.95'))
        row[FEE] = new_fee
        step3_updated += 1
        step3_by_region[row[REGION]] += 1
        print(f"  id={row[ID]}, {old_fee} -> {new_fee}")

print(f"  Total rows updated: {step3_updated}")
for region in ['americas', 'asia-pacific', 'europe']:
    cnt = step3_by_region.get(region, 0)
    if cnt > 0:
        print(f"  {region}: {cnt} rows updated")
    else:
        print(f"  {region}: 0 rows updated (partition NOT touched)")

# ── LEARN: Verify only asia-pacific was affected ──
print("\n── LEARN: Enterprise fees per region (after STEP 3) ──")
for region in ['americas', 'asia-pacific', 'europe']:
    ents = [r_ for r_ in data if r_[REGION] == region and r_[PLAN] == 'enterprise']
    fees = [r_[FEE] for r_ in ents]
    avg_fee = r(sum(fees) / len(fees))
    print(f"  {region}: count={len(ents)}, fees={fees}, avg={avg_fee}")

# ============================================================================
# FINAL STATE
# ============================================================================
print("\n" + "=" * 80)
print("FINAL STATE (after OPTIMIZE)")
print("=" * 80)

# Per-region revenue summary
print("\n── EXPLORE: Per-region revenue summary ──")
for region in ['americas', 'asia-pacific', 'europe']:
    region_rows = [r_ for r_ in data if r_[REGION] == region]
    total_rev = r(sum(r_[FEE] for r_ in region_rows))
    active = len([r_ for r_ in region_rows if r_[STATUS] == 'active'])
    suspended = len([r_ for r_ in region_rows if r_[STATUS] == 'suspended'])
    trial = len([r_ for r_ in region_rows if r_[STATUS] == 'trial'])
    print(f"  {region}: total_revenue={total_rev}, active={active}, suspended={suspended}, trial={trial}")

# ============================================================================
# VERIFY values
# ============================================================================
print("\n" + "=" * 80)
print("VERIFY: All assertion values")
print("=" * 80)

total_rows = len(data)
print(f"\n  total_rows = {total_rows}")

for region in ['americas', 'asia-pacific', 'europe']:
    cnt = len([r_ for r_ in data if r_[REGION] == region])
    print(f"  {region}_count = {cnt}")

# Total revenue
total_rev = r(sum(r_[FEE] for r_ in data))
print(f"\n  total_revenue = {total_rev}")

# Professional fee checks (after +10%)
for region in ['americas', 'asia-pacific', 'europe']:
    pros = [r_ for r_ in data if r_[REGION] == region and r_[PLAN] == 'professional']
    avg = r(sum(r_[FEE] for r_ in pros) / len(pros))
    print(f"  avg_pro_fee_{region} = {avg}")

# Status counts
for status in ['active', 'trial', 'suspended']:
    cnt = len([r_ for r_ in data if r_[STATUS] == status])
    print(f"  {status}_count = {cnt}")

# Suspended count per region
for region in ['americas', 'asia-pacific', 'europe']:
    cnt = len([r_ for r_ in data if r_[REGION] == region and r_[STATUS] == 'suspended'])
    print(f"  suspended_{region} = {cnt}")

# Enterprise fee checks (asia-pacific got discount, others unchanged)
for region in ['americas', 'asia-pacific', 'europe']:
    ents = [r_ for r_ in data if r_[REGION] == region and r_[PLAN] == 'enterprise']
    avg = r(sum(r_[FEE] for r_ in ents) / len(ents))
    total = r(sum(r_[FEE] for r_ in ents))
    print(f"  enterprise_{region}: avg={avg}, total={total}")

# Per-plan average fee
print("\n── Per-plan avg fee (final) ──")
for plan in ['starter', 'professional', 'enterprise']:
    plan_rows = [r_ for r_ in data if r_[PLAN] == plan]
    avg = r(sum(r_[FEE] for r_ in plan_rows) / len(plan_rows))
    print(f"  {plan}: count={len(plan_rows)}, avg_fee={avg}")

# Specific fee spot checks
print("\n── Spot-check specific rows ──")
for check_id in [2, 5, 8, 22, 25, 30, 42, 45, 50, 43, 47, 55, 59]:
    row = [r_ for r_ in data if r_[ID] == check_id][0]
    print(f"  id={row[ID]:2d}, region={row[REGION]:13s}, plan={row[PLAN]:13s}, fee={row[FEE]}, status={row[STATUS]}")

# Step 1 details for ASSERT
print("\n── STEP 1 ASSERT values ──")
print(f"  step1_rows_updated = {step1_updated}")
for region in ['americas', 'asia-pacific', 'europe']:
    cnt = step1_by_region[region]
    print(f"  step1_{region}_updated = {cnt}")
# Updated fees per region count
for region in ['americas', 'asia-pacific', 'europe']:
    pros = [r_ for r_ in data if r_[REGION] == region and r_[PLAN] == 'professional']
    print(f"  step1_verify_{region}_pro_count = {len(pros)}")

# Step 2 details for ASSERT
print(f"\n── STEP 2 ASSERT values ──")
print(f"  step2_rows_updated = {step2_updated}")
print(f"  step2_ids = {step2_ids}")

# Regions with step2 updates
step2_regions_hit = len([k for k, v in step2_by_region.items() if v > 0])
print(f"  step2_regions_affected = {step2_regions_hit}")

# Step 3 details for ASSERT
print(f"\n── STEP 3 ASSERT values ──")
print(f"  step3_rows_updated = {step3_updated}")
# Only asia-pacific enterprise fees changed
ap_ent = [r_ for r_ in data if r_[REGION] == 'asia-pacific' and r_[PLAN] == 'enterprise']
am_ent = [r_ for r_ in data if r_[REGION] == 'americas' and r_[PLAN] == 'enterprise']
eu_ent = [r_ for r_ in data if r_[REGION] == 'europe' and r_[PLAN] == 'enterprise']
print(f"  asia-pacific enterprise avg = {r(sum(r_[FEE] for r_ in ap_ent) / len(ap_ent))}")
print(f"  americas enterprise avg    = {r(sum(r_[FEE] for r_ in am_ent) / len(am_ent))}")
print(f"  europe enterprise avg      = {r(sum(r_[FEE] for r_ in eu_ent) / len(eu_ent))}")

# Per-region revenue summary for final EXPLORE
print("\n── FINAL EXPLORE: Per-region revenue ──")
for region in ['americas', 'asia-pacific', 'europe']:
    region_rows = [r_ for r_ in data if r_[REGION] == region]
    total_rev = r(sum(r_[FEE] for r_ in region_rows))
    plan_counts = defaultdict(int)
    for r_ in region_rows:
        plan_counts[r_[PLAN]] += 1
    avg = r(total_rev / len(region_rows))
    print(f"  {region}: total_revenue={total_rev}, avg_fee={avg}, plans={dict(plan_counts)}")

# Grand total revenue
grand = r(sum(r_[FEE] for r_ in data))
print(f"\n  grand_total_revenue = {grand}")
grand_avg = r(grand / len(data))
print(f"  grand_avg_fee = {grand_avg}")

# ============================================================================
# Print INSERT statements split by region
# ============================================================================
print("\n" + "=" * 80)
print("INSERT STATEMENTS (for setup.sql)")
print("=" * 80)

for region in ['americas', 'europe', 'asia-pacific']:
    region_rows = [r for r in rows if r[REGION] == region]
    print(f"\n-- Region: {region} ({len(region_rows)} rows)")
    print(f"INSERT INTO {{{{zone_name}}}}.delta_demos.subscriptions VALUES")
    for i, row in enumerate(region_rows):
        comma = ',' if i < len(region_rows) - 1 else ';'
        id_, cust, reg, plan, fee, usage, signup, status = row
        print(f"    ({id_:2d}, '{cust}', '{reg}', '{plan}', {fee}, {usage:4d}, '{signup}', '{status}'){comma}")
