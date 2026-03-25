"""
Precompute ALL proof values for the delta-overflow-detection demo.
Financial transaction monitoring: running totals approaching INT/BIGINT boundaries.
"""

# ── STEP 1: Baseline 30 transactions ──────────────────────────────────────────
baseline_rows = [
    (1,  'ACCT-1001', 'deposit',    500000,    500000,     'Initial deposit',           '2025-01-01'),
    (2,  'ACCT-1001', 'deposit',    750000,    1250000,    'Wire transfer in',          '2025-01-05'),
    (3,  'ACCT-1001', 'withdrawal', -120000,   1130000,    'Operating expenses',        '2025-01-10'),
    (4,  'ACCT-1001', 'deposit',    2000000,   3130000,    'Revenue collection',        '2025-01-15'),
    (5,  'ACCT-1001', 'withdrawal', -450000,   2680000,    'Payroll',                   '2025-01-20'),
    (6,  'ACCT-2001', 'deposit',    1000000,   1000000,    'Seed funding',              '2025-01-01'),
    (7,  'ACCT-2001', 'deposit',    3000000,   4000000,    'Series A tranche',          '2025-01-15'),
    (8,  'ACCT-2001', 'withdrawal', -800000,   3200000,    'Infrastructure spend',      '2025-02-01'),
    (9,  'ACCT-2001', 'deposit',    5000000,   8200000,    'Series A remainder',        '2025-02-15'),
    (10, 'ACCT-2001', 'withdrawal', -1500000,  6700000,    'Hiring costs',              '2025-03-01'),
    (11, 'ACCT-3001', 'deposit',    10000000,  10000000,   'Fund allocation',           '2025-01-01'),
    (12, 'ACCT-3001', 'deposit',    25000000,  35000000,   'Quarterly inflow',          '2025-01-15'),
    (13, 'ACCT-3001', 'withdrawal', -8000000,  27000000,   'Portfolio rebalance',       '2025-02-01'),
    (14, 'ACCT-3001', 'deposit',    50000000,  77000000,   'Large client deposit',      '2025-02-15'),
    (15, 'ACCT-3001', 'withdrawal', -15000000, 62000000,   'Fund distribution',         '2025-03-01'),
    (16, 'ACCT-4001', 'deposit',    100000000, 100000000,  'Treasury allocation',       '2025-01-01'),
    (17, 'ACCT-4001', 'deposit',    200000000, 300000000,  'Bond proceeds',             '2025-01-15'),
    (18, 'ACCT-4001', 'withdrawal', -50000000, 250000000,  'Capital expenditure',       '2025-02-01'),
    (19, 'ACCT-4001', 'deposit',    400000000, 650000000,  'Asset liquidation',         '2025-02-15'),
    (20, 'ACCT-4001', 'deposit',    500000000, 1150000000, 'Merger proceeds',           '2025-03-01'),
    (21, 'ACCT-5001', 'deposit',    800000000, 800000000,  'Sovereign fund transfer',   '2025-01-01'),
    (22, 'ACCT-5001', 'deposit',    600000000, 1400000000, 'Oil revenue',               '2025-01-15'),
    (23, 'ACCT-5001', 'deposit',    500000000, 1900000000, 'Tax collection',            '2025-02-01'),
    (24, 'ACCT-5001', 'deposit',    200000000, 2100000000, 'Bond maturity',             '2025-02-15'),
    (25, 'ACCT-5001', 'withdrawal', -100000000,2000000000, 'Infrastructure project',    '2025-03-01'),
    (26, 'ACCT-1001', 'deposit',    1800000,   4480000,    'Q2 revenue',                '2025-04-01'),
    (27, 'ACCT-2001', 'deposit',    4000000,   10700000,   'Series B funding',          '2025-04-01'),
    (28, 'ACCT-3001', 'deposit',    30000000,  92000000,   'Annual inflow',             '2025-04-01'),
    (29, 'ACCT-4001', 'deposit',    300000000, 1450000000, 'Government grant',          '2025-04-01'),
    (30, 'ACCT-5001', 'deposit',    150000000, 2150000000, 'Trade surplus',             '2025-04-15'),
]

# ── QUERY 1: Account summary (baseline 30 rows, BEFORE step 2) ───────────────
print("=" * 80)
print("QUERY 1: Account Summary (30 baseline rows, before UPDATE)")
print("=" * 80)

from collections import defaultdict

acct_rows = defaultdict(list)
for row in baseline_rows:
    acct_rows[row[1]].append(row)

accounts_sorted = sorted(acct_rows.keys())
print(f"\nAccounts found: {accounts_sorted}")
print(f"ROW_COUNT (distinct accounts): {len(accounts_sorted)}\n")

for acct in accounts_sorted:
    rows = acct_rows[acct]
    count = len(rows)
    max_bal = max(r[4] for r in rows)
    sum_amt = sum(r[3] for r in rows)
    print(f"  {acct}: COUNT={count}, MAX(running_balance)={max_bal:,}, SUM(amount)={sum_amt:,}")

print()

# ── STEP 2: UPDATE — add 40000000 to running_balance for ACCT-5001 deposit rows
print("=" * 80)
print("STEP 2: UPDATE running_balance += 40000000 WHERE account_id='ACCT-5001' AND tx_type='deposit'")
print("=" * 80)

# Apply the update to a working copy
rows_after_update = []
for row in baseline_rows:
    rid, acct, txtype, amt, bal, desc, dt = row
    if acct == 'ACCT-5001' and txtype == 'deposit':
        bal = bal + 40000000
    rows_after_update.append((rid, acct, txtype, amt, bal, desc, dt))

INT_MAX = 2147483647

print("\nACCT-5001 deposit rows after update:")
for row in rows_after_update:
    if row[1] == 'ACCT-5001' and row[2] == 'deposit':
        exceeds = "EXCEEDS INT MAX!" if row[4] > INT_MAX else "safe"
        pct = row[4] / INT_MAX * 100
        print(f"  id={row[0]}: running_balance={row[4]:,}  ({pct:.1f}% of INT_MAX) [{exceeds}]")

print()

# ── QUERY 2: INT boundary proximity (after step 2) ───────────────────────────
print("=" * 80)
print("QUERY 2: INT Boundary Proximity Check (after UPDATE)")
print("=" * 80)

rows_over_2b = [r for r in rows_after_update if r[4] > 2000000000]
print(f"\nRows with running_balance > 2,000,000,000: {len(rows_over_2b)}")
for r in rows_over_2b:
    print(f"  id={r[0]}, account={r[1]}, running_balance={r[4]:,}")

rows_over_intmax = [r for r in rows_after_update if r[4] > INT_MAX]
print(f"\nRows EXCEEDING INT MAX ({INT_MAX:,}): {len(rows_over_intmax)}")
for r in rows_over_intmax:
    print(f"  id={r[0]}, account={r[1]}, running_balance={r[4]:,}")

row_30 = [r for r in rows_after_update if r[0] == 30][0]
print(f"\nSpecifically id=30: running_balance = {row_30[4]:,}")
print(f"  INT_MAX = {INT_MAX:,}")
print(f"  Exceeds INT MAX? {row_30[4] > INT_MAX}")
print(f"  Overflow amount: {row_30[4] - INT_MAX:,}" if row_30[4] > INT_MAX else "  (no overflow)")

print()

# ── STEPS 3 & 4: ALTER COLUMN to BIGINT (no data change, just type widening)
# ── STEP 5: Insert 10 BIGINT-range transactions ──────────────────────────────

bigint_rows = [
    (31, 'ACCT-5001', 'deposit',    3000000000,  5190000000,  'Mega infrastructure bond',   '2025-05-01'),
    (32, 'ACCT-5001', 'deposit',    2500000000,  7690000000,  'Sovereign wealth transfer',  '2025-05-15'),
    (33, 'ACCT-6001', 'deposit',    5000000000,  5000000000,  'Central bank reserve',       '2025-05-01'),
    (34, 'ACCT-6001', 'deposit',    8000000000,  13000000000, 'Foreign exchange reserve',   '2025-05-15'),
    (35, 'ACCT-6001', 'withdrawal', -2000000000, 11000000000, 'Currency stabilization',     '2025-06-01'),
    (36, 'ACCT-7001', 'deposit',    10000000000, 10000000000, 'Pension fund seed',          '2025-05-01'),
    (37, 'ACCT-7001', 'deposit',    7500000000,  17500000000, 'Annual contributions',       '2025-05-15'),
    (38, 'ACCT-7001', 'withdrawal', -3000000000, 14500000000, 'Benefit payments',           '2025-06-01'),
    (39, 'ACCT-7001', 'deposit',    12000000000, 26500000000, 'Investment returns',         '2025-06-15'),
    (40, 'ACCT-7001', 'deposit',    9000000000,  35500000000, 'Rebalance gains',            '2025-07-01'),
]

# Full dataset = updated baseline + bigint rows
all_rows = rows_after_update + bigint_rows

# ── QUERY 3: Post-widening with BIGINT data (all 40 rows) ────────────────────
print("=" * 80)
print("QUERY 3: Post-Widening Summary (all 40 rows)")
print("=" * 80)

total_rows = len(all_rows)
distinct_accounts = len(set(r[1] for r in all_rows))
max_balance = max(r[4] for r in all_rows)
max_amount = max(r[3] for r in all_rows)

print(f"\n  Total ROW_COUNT: {total_rows}")
print(f"  COUNT(DISTINCT account_id): {distinct_accounts}")
print(f"  MAX(running_balance): {max_balance:,}")
print(f"  MAX(amount): {max_amount:,}")
print()

# ── QUERY 4: Account tier analysis (all 40 rows) ─────────────────────────────
print("=" * 80)
print("QUERY 4: Account Tier Analysis (all 40 rows)")
print("=" * 80)

# Get MAX(running_balance) per account
acct_max_bal = defaultdict(int)
for r in all_rows:
    acct_max_bal[r[1]] = max(acct_max_bal[r[1]], r[4])

def get_tier(bal):
    if bal > 10000000000:
        return 'Tier 1: Sovereign/Institutional'
    elif bal > 1000000000:
        return 'Tier 2: Large Enterprise'
    elif bal > 10000000:
        return 'Tier 3: Mid-Market'
    else:
        return 'Tier 4: Standard'

print("\nPer-account MAX(running_balance) and tier:")
for acct in sorted(acct_max_bal.keys()):
    bal = acct_max_bal[acct]
    tier = get_tier(bal)
    print(f"  {acct}: MAX(running_balance)={bal:,} -> {tier}")

# Tier aggregation
tier_data = defaultdict(lambda: {'count': 0, 'sum_max_bal': 0, 'accounts': []})
for acct, bal in acct_max_bal.items():
    tier = get_tier(bal)
    tier_data[tier]['count'] += 1
    tier_data[tier]['sum_max_bal'] += bal
    tier_data[tier]['accounts'].append(acct)

print("\nTier Summary:")
for tier in ['Tier 1: Sovereign/Institutional', 'Tier 2: Large Enterprise', 'Tier 3: Mid-Market', 'Tier 4: Standard']:
    d = tier_data[tier]
    if d['count'] > 0:
        print(f"  {tier}:")
        print(f"    COUNT(accounts): {d['count']}")
        print(f"    SUM(max_balances): {d['sum_max_bal']:,}")
        print(f"    Accounts: {sorted(d['accounts'])}")
    else:
        print(f"  {tier}: (none)")

print()

# ── QUERY 5: Deposit vs Withdrawal analysis (all 40 rows) ────────────────────
print("=" * 80)
print("QUERY 5: Deposit vs Withdrawal Analysis (all 40 rows)")
print("=" * 80)

deposits = [r for r in all_rows if r[2] == 'deposit']
withdrawals = [r for r in all_rows if r[2] == 'withdrawal']

dep_count = len(deposits)
dep_sum = sum(r[3] for r in deposits)
wd_count = len(withdrawals)
wd_sum = sum(r[3] for r in withdrawals)

print(f"\n  Deposits:    COUNT={dep_count}, SUM(amount)={dep_sum:,}")
print(f"  Withdrawals: COUNT={wd_count}, SUM(amount)={wd_sum:,}")
print(f"  Net:         {dep_sum + wd_sum:,}")
print()

# ── QUERY 6: VERIFY section ──────────────────────────────────────────────────
print("=" * 80)
print("QUERY 6: VERIFICATION")
print("=" * 80)

row_30_bal = [r for r in all_rows if r[0] == 30][0][4]
row_40_bal = [r for r in all_rows if r[0] == 40][0][4]
max_bal_overall = max(r[4] for r in all_rows)
count_over_intmax = len([r for r in all_rows if r[4] > INT_MAX])
sum_amt_acct7001 = sum(r[3] for r in all_rows if r[1] == 'ACCT-7001')

print(f"\n  Total rows: {len(all_rows)}")
print(f"  COUNT(DISTINCT account_id): {len(set(r[1] for r in all_rows))}")
print(f"  running_balance for id=30 (after +40M update): {row_30_bal:,}")
print(f"  running_balance for id=40: {row_40_bal:,}")
print(f"    (expected 35,500,000,000: {'MATCH' if row_40_bal == 35500000000 else 'MISMATCH'})")
print(f"  MAX(running_balance) overall: {max_bal_overall:,}")
print(f"  COUNT WHERE running_balance > INT_MAX ({INT_MAX:,}): {count_over_intmax}")
print(f"  SUM(amount) for ACCT-7001: {sum_amt_acct7001:,}")

# List all rows exceeding INT MAX for reference
print(f"\n  All rows with running_balance > INT_MAX:")
for r in all_rows:
    if r[4] > INT_MAX:
        print(f"    id={r[0]}, acct={r[1]}, running_balance={r[4]:,}")

print()
print("=" * 80)
print("COMPUTATION COMPLETE")
print("=" * 80)
