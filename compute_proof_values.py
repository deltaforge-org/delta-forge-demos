#!/usr/bin/env python3
"""Precompute ALL proof values for the delta-type-widening IoT fleet demo."""

# ── STEP 1: Baseline data (25 rows) ──
# Columns: id, device_id, region, event_count, bytes_sent, avg_latency, status, reported_date
rows = [
    (1,  'GW-NYC-001', 'us-east',  45000,    1200000,    12.45, 'active', '2025-01-15'),
    (2,  'GW-NYC-002', 'us-east',  62000,    1800000,    15.23, 'active', '2025-01-15'),
    (3,  'GW-NYC-003', 'us-east',  38000,    980000,     8.91,  'active', '2025-01-15'),
    (4,  'GW-LON-001', 'eu-west',  51000,    1500000,    22.67, 'active', '2025-01-15'),
    (5,  'GW-LON-002', 'eu-west',  47000,    1350000,    19.80, 'active', '2025-01-15'),
    (6,  'GW-LON-003', 'eu-west',  33000,    870000,     25.12, 'active', '2025-01-15'),
    (7,  'GW-TKY-001', 'ap-east',  72000,    2100000,    31.55, 'active', '2025-01-15'),
    (8,  'GW-TKY-002', 'ap-east',  58000,    1650000,    28.90, 'active', '2025-01-15'),
    (9,  'GW-TKY-003', 'ap-east',  41000,    1100000,    35.44, 'active', '2025-01-15'),
    (10, 'GW-SYD-001', 'ap-south', 29000,    750000,     42.18, 'active', '2025-01-15'),
    (11, 'GW-SYD-002', 'ap-south', 35000,    920000,     38.75, 'active', '2025-01-15'),
    (12, 'GW-SYD-003', 'ap-south', 22000,    580000,     45.60, 'active', '2025-01-15'),
    (13, 'SNS-TEMP-001','us-east', 150000,   450000,     5.20,  'active', '2025-01-15'),
    (14, 'SNS-TEMP-002','us-east', 142000,   426000,     5.80,  'active', '2025-01-15'),
    (15, 'SNS-TEMP-003','eu-west', 138000,   414000,     6.10,  'active', '2025-01-15'),
    (16, 'SNS-HUM-001', 'eu-west', 125000,   375000,     4.90,  'active', '2025-01-15'),
    (17, 'SNS-HUM-002', 'ap-east', 131000,   393000,     5.50,  'active', '2025-01-15'),
    (18, 'SNS-HUM-003', 'ap-east', 119000,   357000,     6.30,  'active', '2025-01-15'),
    (19, 'CAM-001',  'us-east',    88000,    2400000,    18.20, 'active', '2025-01-15'),
    (20, 'CAM-002',  'eu-west',    95000,    2650000,    20.15, 'active', '2025-01-15'),
    (21, 'CAM-003',  'ap-east',    76000,    2050000,    24.80, 'active', '2025-01-15'),
    (22, 'RTR-001',  'us-east',    210000,   5200000,    3.20,  'active', '2025-01-15'),
    (23, 'RTR-002',  'eu-west',    195000,   4800000,    4.10,  'active', '2025-01-15'),
    (24, 'RTR-003',  'ap-east',    225000,   5500000,    2.90,  'active', '2025-01-15'),
    (25, 'RTR-004',  'ap-south',   180000,   4300000,    3.80,  'active', '2025-01-15'),
]

# Convert to mutable dicts
data = []
for r in rows:
    data.append({
        'id': r[0], 'device_id': r[1], 'region': r[2],
        'event_count': r[3], 'bytes_sent': r[4],
        'avg_latency': r[5], 'status': r[6], 'reported_date': r[7]
    })

# ── STEP 2: UPDATE GW-* devices: multiply by 48000 ──
INT_MAX = 2_147_483_647

print("=" * 80)
print("STEP 2: UPDATE GW-* devices (event_count * 48000, bytes_sent * 48000)")
print("=" * 80)
for d in data:
    if d['device_id'].startswith('GW-'):
        d['event_count'] = d['event_count'] * 48000
        d['bytes_sent'] = d['bytes_sent'] * 48000

print()
print("All GW-* devices after multiplication:")
print(f"{'ID':>3}  {'device_id':<12} {'event_count':>16} {'bytes_sent':>18}  {'ec>INT_MAX?':>11} {'bs>INT_MAX?':>11}")
for d in data:
    if d['device_id'].startswith('GW-'):
        ec_over = d['event_count'] > INT_MAX
        bs_over = d['bytes_sent'] > INT_MAX
        print(f"{d['id']:>3}  {d['device_id']:<12} {d['event_count']:>16,} {d['bytes_sent']:>18,}  {'YES' if ec_over else 'no':>11} {'YES' if bs_over else 'no':>11}")

# ── STEPS 3 & 4: ALTER COLUMN (type widening) — no data change, just schema ──

# ── STEP 5: Insert 10 high-volume BIGINT rows ──
new_rows = [
    (26, 'GW-NYC-004',    'us-east',  3500000000,  85000000000,  11.20, 'active', '2025-07-15'),
    (27, 'GW-LON-004',    'eu-west',  2800000000,  72000000000,  18.90, 'active', '2025-07-15'),
    (28, 'GW-TKY-004',    'ap-east',  4100000000,  95000000000,  27.30, 'active', '2025-07-15'),
    (29, 'GW-SYD-004',    'ap-south', 1900000000,  48000000000,  39.60, 'active', '2025-07-15'),
    (30, 'EDGE-US-001',   'us-east',  8500000000,  210000000000, 7.80,  'active', '2025-07-15'),
    (31, 'EDGE-EU-001',   'eu-west',  7200000000,  180000000000, 9.50,  'active', '2025-07-15'),
    (32, 'EDGE-AP-001',   'ap-east',  9100000000,  230000000000, 6.20,  'active', '2025-07-15'),
    (33, 'EDGE-AP-002',   'ap-south', 6800000000,  165000000000, 11.40, 'active', '2025-07-15'),
    (34, 'CDN-GLOBAL-001','us-east',  15000000000, 500000000000, 2.10,  'active', '2025-07-15'),
    (35, 'CDN-GLOBAL-002','eu-west',  12000000000, 420000000000, 3.40,  'active', '2025-07-15'),
]
for r in new_rows:
    data.append({
        'id': r[0], 'device_id': r[1], 'region': r[2],
        'event_count': r[3], 'bytes_sent': r[4],
        'avg_latency': r[5], 'status': r[6], 'reported_date': r[7]
    })

# ════════════════════════════════════════════════════════════════════════
# QUERY 1: First 10 rows ordered by id
# ════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("QUERY 1: SELECT * ORDER BY id LIMIT 10")
print("=" * 80)
sorted_data = sorted(data, key=lambda x: x['id'])
first10 = sorted_data[:10]
print(f"ROW_COUNT = {len(first10)}")
print()
print(f"{'id':>3}  {'device_id':<12} {'region':<10} {'event_count':>16} {'bytes_sent':>18} {'avg_latency':>12} {'status':<8} {'reported_date'}")
for d in first10:
    print(f"{d['id']:>3}  {d['device_id']:<12} {d['region']:<10} {d['event_count']:>16,} {d['bytes_sent']:>18,} {d['avg_latency']:>12.2f} {d['status']:<8} {d['reported_date']}")

# ════════════════════════════════════════════════════════════════════════
# QUERY 2: Pre-widening INT boundary check (after UPDATE, before step 5 inserts)
# We check the 25 original rows after the *48000 multiplication
# ════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("QUERY 2: Pre-widening INT boundary check (GW-* after *48000)")
print("=" * 80)

# Only the original 25 rows (before step 5 inserts)
original_25 = sorted_data[:25]
gw_rows = [d for d in original_25 if d['device_id'].startswith('GW-')]
gw_overflow = [d for d in gw_rows if d['event_count'] > INT_MAX]

print(f"Total GW-* devices: {len(gw_rows)}")
print(f"GW-* devices with event_count > {INT_MAX:,} (INT_MAX): {len(gw_overflow)}")
print()
print("Overflow devices:")
for d in gw_overflow:
    print(f"  id={d['id']:>2}  {d['device_id']:<12}  event_count={d['event_count']:>16,}  bytes_sent={d['bytes_sent']:>18,}")

print()
print("All GW-* bytes_sent overflow check:")
gw_bs_overflow = [d for d in gw_rows if d['bytes_sent'] > INT_MAX]
print(f"GW-* devices with bytes_sent > INT_MAX: {len(gw_bs_overflow)}")
for d in gw_bs_overflow:
    print(f"  id={d['id']:>2}  {d['device_id']:<12}  bytes_sent={d['bytes_sent']:>18,}")

# ════════════════════════════════════════════════════════════════════════
# QUERY 3: Post-widening verification (all 35 rows)
# ════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("QUERY 3: Post-widening verification (all 35 rows)")
print("=" * 80)

total_rows = len(data)
bigint_rows = [d for d in data if d['event_count'] > INT_MAX]
max_ec = max(d['event_count'] for d in data)
max_bs = max(d['bytes_sent'] for d in data)

print(f"Total ROW_COUNT         = {total_rows}")
print(f"COUNT(event_count > INT_MAX) = {len(bigint_rows)}")
print(f"MAX(event_count)        = {max_ec:,}")
print(f"MAX(bytes_sent)         = {max_bs:,}")

# ════════════════════════════════════════════════════════════════════════
# QUERY 4: Region aggregation (all 35 rows)
# ════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("QUERY 4: Region aggregation (all 35 rows)")
print("=" * 80)

regions = {}
for d in data:
    r = d['region']
    if r not in regions:
        regions[r] = {'count': 0, 'sum_ec': 0, 'sum_bs': 0, 'latencies': []}
    regions[r]['count'] += 1
    regions[r]['sum_ec'] += d['event_count']
    regions[r]['sum_bs'] += d['bytes_sent']
    regions[r]['latencies'].append(d['avg_latency'])

print(f"{'region':<10} {'COUNT':>6} {'SUM(event_count)':>22} {'SUM(bytes_sent)':>24} {'AVG(avg_latency)':>18}")
for r in sorted(regions.keys()):
    info = regions[r]
    avg_lat = round(sum(info['latencies']) / len(info['latencies']), 2)
    print(f"{r:<10} {info['count']:>6} {info['sum_ec']:>22,} {info['sum_bs']:>24,} {avg_lat:>18.2f}")

# ════════════════════════════════════════════════════════════════════════
# QUERY 5: Device type analysis
# ════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("QUERY 5: Device type analysis")
print("=" * 80)

def get_device_type(device_id):
    if device_id.startswith('GW-'):
        return 'Gateway'
    elif device_id.startswith('SNS-'):
        return 'Sensor'
    elif device_id.startswith('CAM-'):
        return 'Camera'
    elif device_id.startswith('RTR-'):
        return 'Router'
    elif device_id.startswith('EDGE-'):
        return 'Edge Node'
    elif device_id.startswith('CDN-'):
        return 'CDN'
    return 'Unknown'

types = {}
for d in data:
    t = get_device_type(d['device_id'])
    if t not in types:
        types[t] = {'count': 0, 'min_ec': float('inf'), 'max_ec': 0}
    types[t]['count'] += 1
    types[t]['min_ec'] = min(types[t]['min_ec'], d['event_count'])
    types[t]['max_ec'] = max(types[t]['max_ec'], d['event_count'])

print(f"{'device_type':<12} {'COUNT':>6} {'MIN(event_count)':>20} {'MAX(event_count)':>20}")
for t in ['Gateway', 'Sensor', 'Camera', 'Router', 'Edge Node', 'CDN']:
    if t in types:
        info = types[t]
        print(f"{t:<12} {info['count']:>6} {info['min_ec']:>20,} {info['max_ec']:>20,}")

# ════════════════════════════════════════════════════════════════════════
# QUERY 6: VERIFY section values
# ════════════════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("QUERY 6: VERIFY section values")
print("=" * 80)

row_id1 = next(d for d in data if d['id'] == 1)
row_id34 = next(d for d in data if d['id'] == 34)
distinct_regions = len(set(d['region'] for d in data))

print(f"Total rows                    = {len(data)}")
print(f"COUNT(DISTINCT region)        = {distinct_regions}")
print(f"event_count for id=1          = {row_id1['event_count']:,}")
print(f"bytes_sent  for id=1          = {row_id1['bytes_sent']:,}")
print(f"event_count for id=34         = {row_id34['event_count']:,}")
print(f"bytes_sent  for id=34         = {row_id34['bytes_sent']:,}")
print(f"MAX(event_count) overall      = {max_ec:,}")
print(f"COUNT WHERE event_count > INT_MAX = {len(bigint_rows)}")

print()
print("=" * 80)
print("SUMMARY OF KEY PROOF VALUES")
print("=" * 80)
print(f"INT_MAX                        = {INT_MAX:,}")
print(f"id=1  event_count (45000*48000)= {row_id1['event_count']:,}")
print(f"id=1  bytes_sent (1200000*48000)= {row_id1['bytes_sent']:,}")
print(f"id=34 event_count              = {row_id34['event_count']:,}")
print(f"id=34 bytes_sent               = {row_id34['bytes_sent']:,}")
print(f"MAX(event_count)               = {max_ec:,}")
print(f"MAX(bytes_sent)                = {max_bs:,}")
print(f"Rows exceeding INT_MAX (ec)    = {len(bigint_rows)}")
print(f"Total rows                     = {len(data)}")
print(f"Distinct regions               = {distinct_regions}")
