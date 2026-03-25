#!/usr/bin/env python3
"""Compute ALL proof values for the overflow-detection demo."""

INT_MAX = 2_147_483_647

# ── 30 baseline rows ──
rows = [
    (1,  'ACCT-1001', 'deposit',    500000,    500000),
    (2,  'ACCT-1001', 'deposit',    750000,    1250000),
    (3,  'ACCT-1001', 'withdrawal', -120000,   1130000),
    (4,  'ACCT-1001', 'deposit',    2000000,   3130000),
    (5,  'ACCT-1001', 'withdrawal', -450000,   2680000),
    (6,  'ACCT-2001', 'deposit',    1000000,   1000000),
    (7,  'ACCT-2001', 'deposit',    3000000,   4000000),
    (8,  'ACCT-2001', 'withdrawal', -800000,   3200000),
    (9,  'ACCT-2001', 'deposit',    5000000,   8200000),
    (10, 'ACCT-2001', 'withdrawal', -1500000,  6700000),
    (11, 'ACCT-3001', 'deposit',    10000000,  10000000),
    (12, 'ACCT-3001', 'deposit',    25000000,  35000000),
    (13, 'ACCT-3001', 'withdrawal', -8000000,  27000000),
    (14, 'ACCT-3001', 'deposit',    50000000,  77000000),
    (15, 'ACCT-3001', 'withdrawal', -15000000, 62000000),
    (16, 'ACCT-4001', 'deposit',    100000000, 100000000),
    (17, 'ACCT-4001', 'deposit',    200000000, 300000000),
    (18, 'ACCT-4001', 'withdrawal', -50000000, 250000000),
    (19, 'ACCT-4001', 'deposit',    400000000, 650000000),
    (20, 'ACCT-4001', 'deposit',    500000000, 1150000000),
    (21, 'ACCT-5001', 'deposit',    800000000, 800000000),
    (22, 'ACCT-5001', 'deposit',    600000000, 1400000000),
    (23, 'ACCT-5001', 'deposit',    400000000, 1800000000),
    (24, 'ACCT-5001', 'deposit',    100000000, 1900000000),
    (25, 'ACCT-5001', 'withdrawal', -50000000, 1850000000),
    (26, 'ACCT-1001', 'deposit',    1800000,   4480000),
    (27, 'ACCT-2001', 'deposit',    4000000,   10700000),
    (28, 'ACCT-3001', 'deposit',    30000000,  92000000),
    (29, 'ACCT-4001', 'deposit',    300000000, 1450000000),
    (30, 'ACCT-5001', 'deposit',    200000000, 2050000000),
]

# ── 10 BIGINT insert rows ──
bigint_rows = [
    (31, 'ACCT-5001', 'deposit',    3000000000,  5250000000),
    (32, 'ACCT-5001', 'deposit',    2500000000,  7750000000),
    (33, 'ACCT-6001', 'deposit',    5000000000,  5000000000),
    (34, 'ACCT-6001', 'deposit',    8000000000,  13000000000),
    (35, 'ACCT-6001', 'withdrawal', -2000000000, 11000000000),
    (36, 'ACCT-7001', 'deposit',    10000000000, 10000000000),
    (37, 'ACCT-7001', 'deposit',    7500000000,  17500000000),
    (38, 'ACCT-7001', 'withdrawal', -3000000000, 14500000000),
    (39, 'ACCT-7001', 'deposit',    12000000000, 26500000000),
    (40, 'ACCT-7001', 'deposit',    9000000000,  35500000000),
]

from collections import defaultdict

# ═══════════════════════════════════════════════════════════════
# A. Pre-UPDATE account summary (30 baseline rows)
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("A. PRE-UPDATE ACCOUNT SUMMARY (30 baseline rows)")
print("=" * 70)

acct_rows = defaultdict(list)
for r in rows:
    acct_rows[r[1]].append(r)

for acct in sorted(acct_rows):
    ar = acct_rows[acct]
    cnt = len(ar)
    max_bal = max(r[4] for r in ar)
    sum_amt = sum(r[3] for r in ar)
    print(f"  {acct}: COUNT={cnt}, MAX(running_balance)={max_bal:>14,}, SUM(amount)={sum_amt:>14,}")

counts = [len(v) for v in acct_rows.values()]
print(f"\n  ASSERT: all accounts have {set(counts)} rows per account => ROW_COUNT per account = {counts[0]}")
print(f"  Total accounts: {len(acct_rows)}")

# ═══════════════════════════════════════════════════════════════
# B. INT boundary % (30 baseline rows, BEFORE update)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("B. INT BOUNDARY % (before UPDATE)")
print("=" * 70)

pcts = []
for acct in sorted(acct_rows):
    ar = acct_rows[acct]
    max_bal = max(r[4] for r in ar)
    pct = round(float(max_bal) / INT_MAX * 100, 2)
    pcts.append((acct, max_bal, pct))

pcts.sort(key=lambda x: -x[2])
for acct, mb, pct in pcts:
    print(f"  {acct}: MAX(running_balance)={mb:>14,}  =>  {pct:>7.2f}% of INT max")

# ═══════════════════════════════════════════════════════════════
# C. After UPDATE: ACCT-5001 deposits get +200M
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("C. AFTER UPDATE (ACCT-5001 deposits += 200,000,000)")
print("=" * 70)

# Apply update to a copy
updated_rows = []
for r in rows:
    rid, acct, tx, amt, bal = r
    if acct == 'ACCT-5001' and tx == 'deposit':
        bal = bal + 200_000_000
    updated_rows.append((rid, acct, tx, amt, bal))

print("\n  ACCT-5001 rows after UPDATE:")
for r in updated_rows:
    if r[1] == 'ACCT-5001':
        overflow_flag = " *** OVERFLOW (> INT_MAX) ***" if r[4] > INT_MAX else ""
        print(f"    id={r[0]:>2}, tx_type={r[2]:<10}, amount={r[3]:>12,}, running_balance={r[4]:>14,}{overflow_flag}")

overflow_count = sum(1 for r in updated_rows if r[4] > INT_MAX)
over_2b_count = sum(1 for r in updated_rows if r[4] > 2_000_000_000)
print(f"\n  Rows with running_balance > INT_MAX ({INT_MAX:,}): {overflow_count}")
print(f"  Rows with running_balance > 2,000,000,000: {over_2b_count}")

# ═══════════════════════════════════════════════════════════════
# D. After INSERT (all 40 rows = updated 30 + bigint 10)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("D. AFTER INSERT (all 40 rows)")
print("=" * 70)

all_rows = updated_rows + list(bigint_rows)

total_count = len(all_rows)
distinct_accts = len(set(r[1] for r in all_rows))
max_running_bal = max(r[4] for r in all_rows)
max_amount = max(r[3] for r in all_rows)

print(f"  Total ROW_COUNT:          {total_count}")
print(f"  COUNT(DISTINCT account_id): {distinct_accts}")
print(f"  MAX(running_balance):     {max_running_bal:,}")
print(f"  MAX(amount):              {max_amount:,}")

# ═══════════════════════════════════════════════════════════════
# E. Account tier analysis (all 40 rows after UPDATE + INSERT)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("E. ACCOUNT TIER ANALYSIS (all 40 rows)")
print("=" * 70)

acct_max = defaultdict(int)
for r in all_rows:
    acct_max[r[1]] = max(acct_max[r[1]], r[4])

def tier(bal):
    if bal > 10_000_000_000:
        return 'Tier 1: Sovereign/Institutional'
    elif bal > 1_000_000_000:
        return 'Tier 2: Large Enterprise'
    elif bal > 10_000_000:
        return 'Tier 3: Mid-Market'
    else:
        return 'Tier 4: Standard'

print("\n  Per-account max balance and tier:")
for acct in sorted(acct_max):
    mb = acct_max[acct]
    t = tier(mb)
    print(f"    {acct}: MAX(running_balance)={mb:>16,}  =>  {t}")

tier_data = defaultdict(lambda: {'count': 0, 'sum_max': 0})
for acct, mb in acct_max.items():
    t = tier(mb)
    tier_data[t]['count'] += 1
    tier_data[t]['sum_max'] += mb

print("\n  Tier summary:")
for t in ['Tier 1: Sovereign/Institutional', 'Tier 2: Large Enterprise', 'Tier 3: Mid-Market', 'Tier 4: Standard']:
    d = tier_data.get(t, {'count': 0, 'sum_max': 0})
    if d['count'] > 0:
        print(f"    {t}: accounts={d['count']}, SUM(max_balances)={d['sum_max']:,}")

# ═══════════════════════════════════════════════════════════════
# F. Deposit vs Withdrawal (all 40 rows)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("F. DEPOSIT vs WITHDRAWAL (all 40 rows)")
print("=" * 70)

dep_count = sum(1 for r in all_rows if r[2] == 'deposit')
dep_sum = sum(r[3] for r in all_rows if r[2] == 'deposit')
wd_count = sum(1 for r in all_rows if r[2] == 'withdrawal')
wd_sum = sum(r[3] for r in all_rows if r[2] == 'withdrawal')

print(f"  Deposits:    COUNT={dep_count}, SUM(amount)={dep_sum:,}")
print(f"  Withdrawals: COUNT={wd_count}, SUM(amount)={wd_sum:,}")

# ═══════════════════════════════════════════════════════════════
# G. VERIFY values
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("G. VERIFICATION VALUES")
print("=" * 70)

# Find specific rows
row_24 = [r for r in all_rows if r[0] == 24][0]
row_30 = [r for r in all_rows if r[0] == 30][0]
row_40 = [r for r in all_rows if r[0] == 40][0]

acct7001_sum = sum(r[3] for r in all_rows if r[1] == 'ACCT-7001')
overflow_all = sum(1 for r in all_rows if r[4] > INT_MAX)

print(f"  Total rows:                              {len(all_rows)}")
print(f"  COUNT(DISTINCT account_id):               {len(set(r[1] for r in all_rows))}")
print(f"  running_balance for id=24 after UPDATE:   {row_24[4]:,}")
print(f"  running_balance for id=30 after UPDATE:   {row_30[4]:,}")
print(f"  running_balance for id=40:                {row_40[4]:,}")
print(f"  MAX(running_balance):                     {max(r[4] for r in all_rows):,}")
print(f"  COUNT WHERE running_balance > INT_MAX:    {overflow_all}")
print(f"  SUM(amount) for ACCT-7001:                {acct7001_sum:,}")

print("\n" + "=" * 70)
print("DONE — All proof values computed.")
print("=" * 70)
