"""
Independent proof-value computer for the graph-auto-refresh-csr demo.

Replicates the deterministic data generation performed inline in setup.sql
via `generate_series`, then computes every value asserted in queries.sql.
Running this script must produce the exact numbers used in the ASSERTs —
never use a value in queries.sql that wasn't printed here first.

Scenario: fleet dispatch network. 30 distribution hubs (cities) connected
by 100 directed delivery routes. Teaches AUTO REFRESH CSR by running the
same Cypher queries against two paired graphs over the same tables —
one with `AUTO REFRESH CSR` (live), one without (batched).
"""

# ─── Cities (30) ─────────────────────────────────────────────────────
cities = []
for i in range(1, 31):
    region_idx = (i - 1) // 5      # 0..5 → 6 regions of 5 cities each
    name = f"Hub_{i:02d}"
    region = f"Region_{region_idx}"
    population = 50_000 + (i * 7919) % 450_000
    cities.append((i, name, region, population))

# ─── Routes (100) ────────────────────────────────────────────────────
routes = []
for i in range(1, 101):
    src = ((i - 1) % 30) + 1
    dst = ((i * 7 - 1) % 30) + 1
    if src == dst:
        dst = (dst % 30) + 1
    distance_km = 50 + (i * 11) % 500
    eta_hours   = 2  + (i % 24)
    price_usd   = 100 + (i * 3) % 200
    status      = 'suspended' if (i % 10) == 0 else 'active'
    routes.append((i, src, dst, distance_km, eta_hours, price_usd, status))

# ─── Proof values ────────────────────────────────────────────────────
print("=" * 60)
print("BASELINE (pre-DML)")
print("=" * 60)
print(f"  cities total      : {len(cities)}                (expected 30)")
print(f"  routes total      : {len(routes)}                (expected 100)")
print(f"  suspended routes  : {sum(1 for r in routes if r[6] == 'suspended')}                (expected 10)")
print(f"  active routes     : {sum(1 for r in routes if r[6] == 'active')}                (expected 90)")

target_id = 42
r42 = next(r for r in routes if r[0] == target_id)
print(f"\n  route id={target_id} baseline:")
print(f"    src          : {r42[1]}")
print(f"    dst          : {r42[2]}")
print(f"    distance_km  : {r42[3]}")
print(f"    eta_hours    : {r42[4]}")
print(f"    price_usd    : {r42[5]}      ← used in Q6 stale-batch assert")
print(f"    status       : {r42[6]}")

print()
print("=" * 60)
print("POST-UPDATE (UPDATE routes SET price_usd = 9999 WHERE id = 42)")
print("=" * 60)
print(f"  routes_live  r.id={target_id} price_usd : 9999   ← Q5 live assert (fresh)")
print(f"  routes_batch r.id={target_id} price_usd : {r42[5]}    ← Q6 batch assert (STALE — cache not evicted)")

print()
print("=" * 60)
print("POST-REFRESH (CREATE GRAPHCSR routes_batch)")
print("=" * 60)
print(f"  routes_batch r.id={target_id} price_usd : 9999   ← Q8 batch assert (now fresh)")

print()
print("=" * 60)
print("POST-DELETE (DELETE FROM routes WHERE status = 'suspended')")
print("=" * 60)
remaining = [r for r in routes if r[6] != 'suspended']
remaining_count = len(remaining)
print(f"  routes remaining (SQL) : {remaining_count}    ← Q10 live assert")
print(f"  routes_live  edge count: {remaining_count}    ← Cypher over live")
print(f"  routes_batch edge count: 100   ← Q12 batch assert (STALE — still holds pre-DELETE topology)")
print(f"  after CREATE GRAPHCSR routes_batch → edge count: {remaining_count}  ← Q14 post-refresh")

# Route 42 survives (status = 'active')
assert r42[6] == 'active', "route 42 must be active for the demo"
print(f"\n  route id={target_id} survives DELETE (status='active'), price_usd still 9999 on live")

# ─── Cross-region / regional-mix facts (used in VERIFY) ──────────────
region_counts = {}
for c in cities:
    region_counts[c[2]] = region_counts.get(c[2], 0) + 1
print()
print("=" * 60)
print("REGION DISTRIBUTION (VERIFY)")
print("=" * 60)
for region in sorted(region_counts.keys()):
    print(f"  {region} : {region_counts[region]} hubs")

# ─── Status distribution ────────────────────────────────────────────
suspended_ids = [r[0] for r in routes if r[6] == 'suspended']
print(f"\n  suspended route ids: {suspended_ids}")
