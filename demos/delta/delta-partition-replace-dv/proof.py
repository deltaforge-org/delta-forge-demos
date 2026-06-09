#!/usr/bin/env python3
"""
Proof-of-values script for delta-partition-replace-dv demo.

Real-world scenario: Financial services firm processes monthly transaction
settlements. Late-arriving invoice corrections require replacing January's
entire partition. Old data deleted, corrected data inserted, OPTIMIZE cleans
up DVs from the replaced partition.

Table: monthly_settlements — 60 rows, partitioned by settlement_month
"""

from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict

# =============================================================================
# ORIGINAL 60-ROW DATASET
# =============================================================================
# Columns: id, account_id, settlement_month, transaction_type, amount, currency, counterparty, settled_at

# Transaction types: payment, refund, adjustment, fee
# Currencies: USD, EUR, GBP
# Amounts: 500.00 - 50000.00

original_january = [
    # id, account_id, settlement_month, transaction_type, amount, currency, counterparty, settled_at
    (1,  'ACC-1001', '2024-01', 'payment',    Decimal('12500.00'), 'USD', 'Meridian Capital LLC',       '2024-01-03 09:15:00'),
    (2,  'ACC-1002', '2024-01', 'payment',    Decimal('8750.50'),  'USD', 'Crossbridge Partners',       '2024-01-04 11:30:00'),
    (3,  'ACC-1003', '2024-01', 'refund',     Decimal('2100.00'),  'EUR', 'Nordic Trade Finance',       '2024-01-05 14:22:00'),
    (4,  'ACC-1001', '2024-01', 'fee',        Decimal('875.00'),   'USD', 'Clearstream Services',       '2024-01-07 08:45:00'),
    (5,  'ACC-1004', '2024-01', 'payment',    Decimal('34200.00'), 'GBP', 'Sterling Settlements Ltd',   '2024-01-08 10:00:00'),
    (6,  'ACC-1002', '2024-01', 'adjustment', Decimal('1500.75'),  'USD', 'Meridian Capital LLC',       '2024-01-09 13:10:00'),
    (7,  'ACC-1005', '2024-01', 'payment',    Decimal('19800.00'), 'EUR', 'Deutsche Handelsbank AG',    '2024-01-10 09:30:00'),
    (8,  'ACC-1003', '2024-01', 'payment',    Decimal('6300.25'),  'USD', 'Pacific Rim Holdings',       '2024-01-11 15:45:00'),
    (9,  'ACC-1001', '2024-01', 'refund',     Decimal('3450.00'),  'USD', 'Crossbridge Partners',       '2024-01-14 11:00:00'),
    (10, 'ACC-1004', '2024-01', 'payment',    Decimal('27650.00'), 'GBP', 'London Clearing House',      '2024-01-15 08:20:00'),
    (11, 'ACC-1005', '2024-01', 'fee',        Decimal('1250.00'),  'EUR', 'Euroclear Operations',       '2024-01-16 10:15:00'),
    (12, 'ACC-1002', '2024-01', 'payment',    Decimal('15900.00'), 'USD', 'Apex Financial Group',       '2024-01-17 14:30:00'),
    (13, 'ACC-1003', '2024-01', 'payment',    Decimal('42100.00'), 'USD', 'Meridian Capital LLC',       '2024-01-18 09:00:00'),
    (14, 'ACC-1001', '2024-01', 'adjustment', Decimal('950.50'),   'USD', 'Clearstream Services',       '2024-01-21 11:45:00'),
    (15, 'ACC-1004', '2024-01', 'payment',    Decimal('8200.00'),  'GBP', 'Sterling Settlements Ltd',   '2024-01-22 13:20:00'),
    (16, 'ACC-1005', '2024-01', 'refund',     Decimal('4800.00'),  'EUR', 'Nordic Trade Finance',       '2024-01-23 08:55:00'),
    (17, 'ACC-1002', '2024-01', 'payment',    Decimal('11350.00'), 'USD', 'Pacific Rim Holdings',       '2024-01-24 15:10:00'),
    (18, 'ACC-1003', '2024-01', 'fee',        Decimal('625.00'),   'USD', 'Apex Financial Group',       '2024-01-25 10:30:00'),
    # DUPLICATE ROWS (will be removed in correction):
    # id=19 is a duplicate of id=1 (same counterparty, same amount — double-booked payment)
    (19, 'ACC-1001', '2024-01', 'payment',    Decimal('12500.00'), 'USD', 'Meridian Capital LLC',       '2024-01-28 09:15:00'),
    # id=20 is a duplicate of id=5 (same counterparty, same amount — double-booked GBP payment)
    (20, 'ACC-1004', '2024-01', 'payment',    Decimal('34200.00'), 'GBP', 'Sterling Settlements Ltd',   '2024-01-29 10:00:00'),
]

original_february = [
    (21, 'ACC-1001', '2024-02', 'payment',    Decimal('18200.00'), 'USD', 'Crossbridge Partners',       '2024-02-01 09:00:00'),
    (22, 'ACC-1002', '2024-02', 'payment',    Decimal('9400.75'),  'USD', 'Meridian Capital LLC',       '2024-02-03 11:15:00'),
    (23, 'ACC-1003', '2024-02', 'refund',     Decimal('3200.00'),  'EUR', 'Nordic Trade Finance',       '2024-02-04 14:00:00'),
    (24, 'ACC-1004', '2024-02', 'payment',    Decimal('41500.00'), 'GBP', 'London Clearing House',      '2024-02-05 08:30:00'),
    (25, 'ACC-1005', '2024-02', 'fee',        Decimal('1100.00'),  'EUR', 'Euroclear Operations',       '2024-02-06 10:45:00'),
    (26, 'ACC-1001', '2024-02', 'payment',    Decimal('22750.00'), 'USD', 'Apex Financial Group',       '2024-02-07 13:30:00'),
    (27, 'ACC-1002', '2024-02', 'adjustment', Decimal('2850.50'),  'USD', 'Clearstream Services',       '2024-02-10 09:20:00'),
    (28, 'ACC-1003', '2024-02', 'payment',    Decimal('7600.00'),  'USD', 'Pacific Rim Holdings',       '2024-02-11 15:00:00'),
    (29, 'ACC-1004', '2024-02', 'payment',    Decimal('29300.00'), 'GBP', 'Sterling Settlements Ltd',   '2024-02-12 08:15:00'),
    (30, 'ACC-1005', '2024-02', 'refund',     Decimal('5400.00'),  'EUR', 'Deutsche Handelsbank AG',    '2024-02-13 11:40:00'),
    (31, 'ACC-1001', '2024-02', 'payment',    Decimal('16800.00'), 'USD', 'Meridian Capital LLC',       '2024-02-14 14:20:00'),
    (32, 'ACC-1002', '2024-02', 'fee',        Decimal('750.00'),   'USD', 'Clearstream Services',       '2024-02-17 08:50:00'),
    (33, 'ACC-1003', '2024-02', 'payment',    Decimal('38900.00'), 'USD', 'Crossbridge Partners',       '2024-02-18 10:10:00'),
    (34, 'ACC-1004', '2024-02', 'adjustment', Decimal('1650.25'),  'GBP', 'London Clearing House',      '2024-02-19 13:00:00'),
    (35, 'ACC-1005', '2024-02', 'payment',    Decimal('13200.00'), 'EUR', 'Nordic Trade Finance',       '2024-02-20 09:35:00'),
    (36, 'ACC-1001', '2024-02', 'payment',    Decimal('8950.00'),  'USD', 'Pacific Rim Holdings',       '2024-02-21 15:25:00'),
    (37, 'ACC-1002', '2024-02', 'refund',     Decimal('4100.00'),  'USD', 'Apex Financial Group',       '2024-02-24 11:00:00'),
    (38, 'ACC-1003', '2024-02', 'payment',    Decimal('25600.00'), 'USD', 'Meridian Capital LLC',       '2024-02-25 08:40:00'),
    (39, 'ACC-1004', '2024-02', 'payment',    Decimal('19750.00'), 'GBP', 'Sterling Settlements Ltd',   '2024-02-26 14:50:00'),
    (40, 'ACC-1005', '2024-02', 'payment',    Decimal('6200.00'),  'EUR', 'Deutsche Handelsbank AG',    '2024-02-28 10:05:00'),
]

original_march = [
    (41, 'ACC-1001', '2024-03', 'payment',    Decimal('21300.00'), 'USD', 'Crossbridge Partners',       '2024-03-01 09:10:00'),
    (42, 'ACC-1002', '2024-03', 'payment',    Decimal('14750.50'), 'USD', 'Meridian Capital LLC',       '2024-03-04 11:25:00'),
    (43, 'ACC-1003', '2024-03', 'refund',     Decimal('1800.00'),  'EUR', 'Nordic Trade Finance',       '2024-03-05 14:15:00'),
    (44, 'ACC-1004', '2024-03', 'payment',    Decimal('36400.00'), 'GBP', 'London Clearing House',      '2024-03-06 08:40:00'),
    (45, 'ACC-1005', '2024-03', 'fee',        Decimal('950.00'),   'EUR', 'Euroclear Operations',       '2024-03-07 10:30:00'),
    (46, 'ACC-1001', '2024-03', 'payment',    Decimal('28100.00'), 'USD', 'Apex Financial Group',       '2024-03-10 13:45:00'),
    (47, 'ACC-1002', '2024-03', 'adjustment', Decimal('3200.75'),  'USD', 'Clearstream Services',       '2024-03-11 09:00:00'),
    (48, 'ACC-1003', '2024-03', 'payment',    Decimal('9850.00'),  'USD', 'Pacific Rim Holdings',       '2024-03-12 15:20:00'),
    (49, 'ACC-1004', '2024-03', 'payment',    Decimal('45000.00'), 'GBP', 'Sterling Settlements Ltd',   '2024-03-13 08:05:00'),
    (50, 'ACC-1005', '2024-03', 'refund',     Decimal('6700.00'),  'EUR', 'Deutsche Handelsbank AG',    '2024-03-14 11:50:00'),
    (51, 'ACC-1001', '2024-03', 'payment',    Decimal('17500.00'), 'USD', 'Meridian Capital LLC',       '2024-03-17 14:35:00'),
    (52, 'ACC-1002', '2024-03', 'fee',        Decimal('1025.00'),  'USD', 'Clearstream Services',       '2024-03-18 08:25:00'),
    (53, 'ACC-1003', '2024-03', 'payment',    Decimal('33250.00'), 'USD', 'Crossbridge Partners',       '2024-03-19 10:40:00'),
    (54, 'ACC-1004', '2024-03', 'adjustment', Decimal('2100.50'),  'GBP', 'London Clearing House',      '2024-03-20 13:15:00'),
    (55, 'ACC-1005', '2024-03', 'payment',    Decimal('11600.00'), 'EUR', 'Nordic Trade Finance',       '2024-03-21 09:50:00'),
    (56, 'ACC-1001', '2024-03', 'payment',    Decimal('7800.00'),  'USD', 'Pacific Rim Holdings',       '2024-03-24 15:05:00'),
    (57, 'ACC-1002', '2024-03', 'refund',     Decimal('3500.00'),  'USD', 'Apex Financial Group',       '2024-03-25 11:20:00'),
    (58, 'ACC-1003', '2024-03', 'payment',    Decimal('19200.00'), 'USD', 'Meridian Capital LLC',       '2024-03-26 08:35:00'),
    (59, 'ACC-1004', '2024-03', 'payment',    Decimal('26850.00'), 'GBP', 'Sterling Settlements Ltd',   '2024-03-27 14:00:00'),
    (60, 'ACC-1005', '2024-03', 'payment',    Decimal('5100.00'),  'EUR', 'Deutsche Handelsbank AG',    '2024-03-28 10:15:00'),
]

# =============================================================================
# CORRECTED JANUARY DATA — 18 rows (removed duplicates id=19, id=20)
# Also corrections: id=6 amount changed from 1500.75 to 1475.50 (recalculated adjustment)
#                   id=14 amount changed from 950.50 to 1125.00 (corrected adjustment)
# =============================================================================

corrected_january = [
    (1,  'ACC-1001', '2024-01', 'payment',    Decimal('12500.00'), 'USD', 'Meridian Capital LLC',       '2024-01-03 09:15:00'),
    (2,  'ACC-1002', '2024-01', 'payment',    Decimal('8750.50'),  'USD', 'Crossbridge Partners',       '2024-01-04 11:30:00'),
    (3,  'ACC-1003', '2024-01', 'refund',     Decimal('2100.00'),  'EUR', 'Nordic Trade Finance',       '2024-01-05 14:22:00'),
    (4,  'ACC-1001', '2024-01', 'fee',        Decimal('875.00'),   'USD', 'Clearstream Services',       '2024-01-07 08:45:00'),
    (5,  'ACC-1004', '2024-01', 'payment',    Decimal('34200.00'), 'GBP', 'Sterling Settlements Ltd',   '2024-01-08 10:00:00'),
    (6,  'ACC-1002', '2024-01', 'adjustment', Decimal('1475.50'),  'USD', 'Meridian Capital LLC',       '2024-01-09 13:10:00'),  # corrected: was 1500.75
    (7,  'ACC-1005', '2024-01', 'payment',    Decimal('19800.00'), 'EUR', 'Deutsche Handelsbank AG',    '2024-01-10 09:30:00'),
    (8,  'ACC-1003', '2024-01', 'payment',    Decimal('6300.25'),  'USD', 'Pacific Rim Holdings',       '2024-01-11 15:45:00'),
    (9,  'ACC-1001', '2024-01', 'refund',     Decimal('3450.00'),  'USD', 'Crossbridge Partners',       '2024-01-14 11:00:00'),
    (10, 'ACC-1004', '2024-01', 'payment',    Decimal('27650.00'), 'GBP', 'London Clearing House',      '2024-01-15 08:20:00'),
    (11, 'ACC-1005', '2024-01', 'fee',        Decimal('1250.00'),  'EUR', 'Euroclear Operations',       '2024-01-16 10:15:00'),
    (12, 'ACC-1002', '2024-01', 'payment',    Decimal('15900.00'), 'USD', 'Apex Financial Group',       '2024-01-17 14:30:00'),
    (13, 'ACC-1003', '2024-01', 'payment',    Decimal('42100.00'), 'USD', 'Meridian Capital LLC',       '2024-01-18 09:00:00'),
    (14, 'ACC-1001', '2024-01', 'adjustment', Decimal('1125.00'),  'USD', 'Clearstream Services',       '2024-01-21 11:45:00'),  # corrected: was 950.50
    (15, 'ACC-1004', '2024-01', 'payment',    Decimal('8200.00'),  'GBP', 'Sterling Settlements Ltd',   '2024-01-22 13:20:00'),
    (16, 'ACC-1005', '2024-01', 'refund',     Decimal('4800.00'),  'EUR', 'Nordic Trade Finance',       '2024-01-23 08:55:00'),
    (17, 'ACC-1002', '2024-01', 'payment',    Decimal('11350.00'), 'USD', 'Pacific Rim Holdings',       '2024-01-24 15:10:00'),
    (18, 'ACC-1003', '2024-01', 'fee',        Decimal('625.00'),   'USD', 'Apex Financial Group',       '2024-01-25 10:30:00'),
    # id=19 REMOVED (duplicate of id=1)
    # id=20 REMOVED (duplicate of id=5)
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def sum_amount(rows):
    return sum(r[4] for r in rows)

def count_by_type(rows, txn_type):
    return sum(1 for r in rows if r[3] == txn_type)

def sum_by_type(rows, txn_type):
    return sum(r[4] for r in rows if r[3] == txn_type)

def count_by_currency(rows, currency):
    return sum(1 for r in rows if r[5] == currency)

def sum_by_currency(rows, currency):
    return sum(r[4] for r in rows if r[5] == currency)

def format_sql_values(rows):
    """Generate SQL VALUES clause for a set of rows."""
    lines = []
    for r in rows:
        # (id, 'account_id', 'settlement_month', 'transaction_type', amount, 'currency', 'counterparty', 'settled_at')
        line = f"    ({r[0]:<3} '{r[1]}', '{r[2]}', '{r[3]:<10}', {str(r[4]):>10}, '{r[5]}', '{r[6]:<30}', '{r[7]}')"
        lines.append(line)
    return ',\n'.join(lines)

def format_sql_values_clean(rows):
    """Generate clean SQL VALUES clause."""
    lines = []
    for i, r in enumerate(rows):
        amt = f"{r[4]}"
        line = f"    ({r[0]}, '{r[1]}', '{r[2]}', '{r[3]}', {amt}, '{r[5]}', '{r[6]}', '{r[7]}')"
        lines.append(line)
    return ',\n'.join(lines)


# =============================================================================
# STAGE 0: ORIGINAL BASELINE (60 rows)
# =============================================================================
all_original = original_january + original_february + original_march

print("=" * 80)
print("STAGE 0: ORIGINAL BASELINE — 60 rows total")
print("=" * 80)
print()

jan_total = sum_amount(original_january)
feb_total = sum_amount(original_february)
mar_total = sum_amount(original_march)
grand_total_orig = jan_total + feb_total + mar_total

print(f"  January  count: {len(original_january)}, total_amount: {jan_total}")
print(f"  February count: {len(original_february)}, total_amount: {feb_total}")
print(f"  March    count: {len(original_march)}, total_amount: {mar_total}")
print(f"  Grand total: {grand_total_orig}")
print()

# By transaction type
for t in ['payment', 'refund', 'adjustment', 'fee']:
    cnt = count_by_type(all_original, t)
    amt = sum_by_type(all_original, t)
    print(f"  {t:<12} count={cnt}, total={amt}")
print()

# By currency
for c in ['USD', 'EUR', 'GBP']:
    cnt = count_by_currency(all_original, c)
    amt = sum_by_currency(all_original, c)
    print(f"  {c}: count={cnt}, total={amt}")

print()
print("  EXPLORE baseline ASSERT values:")
print(f"    ASSERT ROW_COUNT = 3  (3 months)")
print(f"    ASSERT VALUE txn_count = {len(original_january)} WHERE settlement_month = '2024-01'")
print(f"    ASSERT VALUE txn_count = {len(original_february)} WHERE settlement_month = '2024-02'")
print(f"    ASSERT VALUE txn_count = {len(original_march)} WHERE settlement_month = '2024-03'")
print(f"    ASSERT VALUE total_amount = {jan_total} WHERE settlement_month = '2024-01'")
print(f"    ASSERT VALUE total_amount = {feb_total} WHERE settlement_month = '2024-02'")
print(f"    ASSERT VALUE total_amount = {mar_total} WHERE settlement_month = '2024-03'")

# =============================================================================
# STAGE 0b: EXPLORE January details
# =============================================================================
print()
print("=" * 80)
print("STAGE 0b: EXPLORE — Original January Details (20 rows)")
print("=" * 80)
print()
print(f"  ASSERT ROW_COUNT = {len(original_january)}")
jan_payments = count_by_type(original_january, 'payment')
jan_refunds = count_by_type(original_january, 'refund')
jan_adjustments = count_by_type(original_january, 'adjustment')
jan_fees = count_by_type(original_january, 'fee')
print(f"  payments={jan_payments}, refunds={jan_refunds}, adjustments={jan_adjustments}, fees={jan_fees}")
print(f"  January total: {jan_total}")

# Identify the duplicates
print(f"\n  DUPLICATES:")
print(f"    id=19: ACC-1001 payment 12500.00 to Meridian Capital LLC (duplicate of id=1)")
print(f"    id=20: ACC-1004 payment 34200.00 to Sterling Settlements Ltd (duplicate of id=5)")
dup_total = Decimal('12500.00') + Decimal('34200.00')
print(f"    Duplicate total: {dup_total}")

# =============================================================================
# STAGE 1: DELETE entire January partition
# =============================================================================
print()
print("=" * 80)
print("STAGE 1: DELETE January partition (20 rows deleted, creates DVs)")
print("=" * 80)
print()
print(f"  ASSERT ROW_COUNT = {len(original_january)}  (rows affected by DELETE)")
print()

remaining_after_delete = original_february + original_march
remaining_total = sum_amount(remaining_after_delete)
print(f"  After DELETE:")
print(f"    Total rows remaining: {len(remaining_after_delete)}")
print(f"    Total amount remaining: {remaining_total}")

# =============================================================================
# STAGE 2: LEARN — Verify January empty, Feb/Mar untouched
# =============================================================================
print()
print("=" * 80)
print("STAGE 2: LEARN — Verify January is empty, Feb/Mar untouched")
print("=" * 80)
print()
print(f"  ASSERT VALUE cnt = 0  (January count)")
print(f"  ASSERT VALUE cnt = {len(remaining_after_delete)}  (total remaining)")
print(f"  ASSERT ROW_COUNT = 2  (only Feb and Mar)")
print(f"  ASSERT VALUE txn_count = {len(original_february)} WHERE settlement_month = '2024-02'")
print(f"  ASSERT VALUE txn_count = {len(original_march)} WHERE settlement_month = '2024-03'")
print(f"  ASSERT VALUE total_amount = {feb_total} WHERE settlement_month = '2024-02'")
print(f"  ASSERT VALUE total_amount = {mar_total} WHERE settlement_month = '2024-03'")

# =============================================================================
# STAGE 3: INSERT corrected January data (18 rows)
# =============================================================================
print()
print("=" * 80)
print("STAGE 3: INSERT corrected January data (18 rows)")
print("=" * 80)
print()
print(f"  ASSERT ROW_COUNT = {len(corrected_january)}  (rows inserted)")

corrected_jan_total = sum_amount(corrected_january)
print(f"  Corrected January total: {corrected_jan_total}")
print(f"  Original January total:  {jan_total}")
print(f"  Difference: {corrected_jan_total - jan_total}")
print(f"    - Removed duplicates: id=19 (12500.00) + id=20 (34200.00) = -{dup_total}")
adj_diff = Decimal('1475.50') - Decimal('1500.75')
adj_diff2 = Decimal('1125.00') - Decimal('950.50')
print(f"    - id=6 adjustment correction: 1500.75 -> 1475.50 = {adj_diff}")
print(f"    - id=14 adjustment correction: 950.50 -> 1125.00 = +{adj_diff2}")
print(f"    Net change: {corrected_jan_total - jan_total}")

# =============================================================================
# STAGE 4: LEARN — Verify corrected January
# =============================================================================
print()
print("=" * 80)
print("STAGE 4: LEARN — Verify corrected January data")
print("=" * 80)
print()

all_after_correction = corrected_january + original_february + original_march
total_after_correction = sum_amount(all_after_correction)

print(f"  Total rows: {len(all_after_correction)}")
print(f"  ASSERT ROW_COUNT = {len(corrected_january)}  (January rows)")
print(f"  January total: {corrected_jan_total}")
print(f"  Grand total: {total_after_correction}")
print()

# Corrected January by type
jan_c_payments = count_by_type(corrected_january, 'payment')
jan_c_refunds = count_by_type(corrected_january, 'refund')
jan_c_adjustments = count_by_type(corrected_january, 'adjustment')
jan_c_fees = count_by_type(corrected_january, 'fee')
jan_c_payment_sum = sum_by_type(corrected_january, 'payment')
jan_c_refund_sum = sum_by_type(corrected_january, 'refund')

print(f"  Corrected January by type:")
print(f"    payments:    count={jan_c_payments}, total={jan_c_payment_sum}")
print(f"    refunds:     count={jan_c_refunds}, total={jan_c_refund_sum}")
print(f"    adjustments: count={jan_c_adjustments}, total={sum_by_type(corrected_january, 'adjustment')}")
print(f"    fees:        count={jan_c_fees}, total={sum_by_type(corrected_january, 'fee')}")
print()

# Verify specific corrections
print(f"  ASSERT VALUE amount = 1475.50 WHERE id = 6  (corrected adjustment)")
print(f"  ASSERT VALUE amount = 1125.00 WHERE id = 14  (corrected adjustment)")
print(f"  id=19 should not exist (removed duplicate)")
print(f"  id=20 should not exist (removed duplicate)")

# =============================================================================
# STAGE 5: OPTIMIZE — cleans up DVs
# =============================================================================
print()
print("=" * 80)
print("STAGE 5: OPTIMIZE — cleans up DVs from partition replacement")
print("=" * 80)
print()
print("  (No data changes, just physical cleanup of deletion vector files)")
print(f"  Total rows unchanged: {len(all_after_correction)}")

# =============================================================================
# STAGE 6: EXPLORE — Final summary per month
# =============================================================================
print()
print("=" * 80)
print("STAGE 6: EXPLORE — Final per-month summary")
print("=" * 80)
print()

final_jan = corrected_jan_total
final_feb = feb_total
final_mar = mar_total
final_grand = final_jan + final_feb + final_mar

print(f"  ASSERT ROW_COUNT = 3")
print(f"  settlement_month='2024-01': txn_count={len(corrected_january)}, total_amount={final_jan}")
print(f"  settlement_month='2024-02': txn_count={len(original_february)}, total_amount={final_feb}")
print(f"  settlement_month='2024-03': txn_count={len(original_march)}, total_amount={final_mar}")
print(f"  Grand total: {final_grand}")

# =============================================================================
# STAGE 7: VERIFY — All final assertions
# =============================================================================
print()
print("=" * 80)
print("STAGE 7: VERIFY — All final assertions")
print("=" * 80)
print()

print(f"  -- total_rows: {len(all_after_correction)} (18 + 20 + 20)")
print(f"  ASSERT VALUE cnt = {len(all_after_correction)}")
print()
print(f"  -- jan_count: 18 corrected rows")
print(f"  ASSERT VALUE cnt = {len(corrected_january)}")
print()
print(f"  -- feb_unchanged")
print(f"  ASSERT VALUE cnt = {len(original_february)}")
print(f"  ASSERT VALUE total_amount = {feb_total}")
print()
print(f"  -- mar_unchanged")
print(f"  ASSERT VALUE cnt = {len(original_march)}")
print(f"  ASSERT VALUE total_amount = {mar_total}")
print()
print(f"  -- jan_corrected_total")
print(f"  ASSERT VALUE total_amount = {corrected_jan_total}")
print()
print(f"  -- duplicates_removed: id=19 and id=20 should not exist")
print(f"  ASSERT VALUE cnt = 0  (count where id IN (19, 20))")
print()
print(f"  -- amount_corrections")
print(f"  ASSERT VALUE amount = 1475.50  (id=6)")
print(f"  ASSERT VALUE amount = 1125.00  (id=14)")
print()
print(f"  -- grand_total")
print(f"  ASSERT VALUE total_amount = {final_grand}")

# =============================================================================
# Additional derived values for queries
# =============================================================================
print()
print("=" * 80)
print("ADDITIONAL DERIVED VALUES")
print("=" * 80)
print()

# Final state by transaction type
print("Final state by transaction_type:")
for t in ['payment', 'refund', 'adjustment', 'fee']:
    cnt = count_by_type(all_after_correction, t)
    amt = sum_by_type(all_after_correction, t)
    print(f"  {t:<12} count={cnt}, total_amount={amt}")

print()
print("Final state by currency:")
for c in ['USD', 'EUR', 'GBP']:
    cnt = count_by_currency(all_after_correction, c)
    amt = sum_by_currency(all_after_correction, c)
    print(f"  {c}: count={cnt}, total_amount={amt}")

# Top counterparties
print()
print("Final state by counterparty (top 5 by amount):")
cp_totals = defaultdict(lambda: [0, Decimal('0')])
for r in all_after_correction:
    cp_totals[r[6]][0] += 1
    cp_totals[r[6]][1] += r[4]
sorted_cp = sorted(cp_totals.items(), key=lambda x: x[1][1], reverse=True)
for cp, (cnt, amt) in sorted_cp[:5]:
    print(f"  {cp:<30} count={cnt}, total={amt}")

# =============================================================================
# PRINT SQL INSERT VALUES
# =============================================================================
print()
print("=" * 80)
print("SQL INSERT VALUES — Original January (20 rows)")
print("=" * 80)
print()
print(format_sql_values_clean(original_january))

print()
print("=" * 80)
print("SQL INSERT VALUES — Original February (20 rows)")
print("=" * 80)
print()
print(format_sql_values_clean(original_february))

print()
print("=" * 80)
print("SQL INSERT VALUES — Original March (20 rows)")
print("=" * 80)
print()
print(format_sql_values_clean(original_march))

print()
print("=" * 80)
print("SQL INSERT VALUES — Corrected January (18 rows)")
print("=" * 80)
print()
print(format_sql_values_clean(corrected_january))

# Final sanity checks
print()
print("=" * 80)
print("SANITY CHECKS")
print("=" * 80)
assert len(original_january) == 20, f"Expected 20 January rows, got {len(original_january)}"
assert len(original_february) == 20, f"Expected 20 February rows, got {len(original_february)}"
assert len(original_march) == 20, f"Expected 20 March rows, got {len(original_march)}"
assert len(corrected_january) == 18, f"Expected 18 corrected January rows, got {len(corrected_january)}"
assert len(all_after_correction) == 58, f"Expected 58 final rows, got {len(all_after_correction)}"
print("All sanity checks passed!")
print()

# Verify no duplicate IDs in corrected set
all_ids = [r[0] for r in all_after_correction]
assert len(all_ids) == len(set(all_ids)), "Duplicate IDs found!"
print("No duplicate IDs in final dataset!")
print()

# Verify amounts are in range 500-50000
for r in all_original + corrected_january:
    assert Decimal('500') <= r[4] <= Decimal('50000'), f"Amount {r[4]} for id={r[0]} out of range"
print("All amounts in range [500, 50000]!")
