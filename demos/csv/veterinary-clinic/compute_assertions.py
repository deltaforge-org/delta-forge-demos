#!/usr/bin/env python3
"""Compute all assertion values from generated CSV data."""
import csv
import os
from collections import Counter

BASE = os.path.dirname(os.path.abspath(__file__))

def read_csv(path):
    with open(path, "r") as f:
        return list(csv.DictReader(f))

# Read all files
north = read_csv(os.path.join(BASE, "data/branch-north/visits.csv"))
south = read_csv(os.path.join(BASE, "data/branch-south/visits.csv"))
east = read_csv(os.path.join(BASE, "data/branch-east/visits.csv"))
all_rows = north + south + east

print("=" * 70)
print("QUERY 1: Full scan (all_visits table)")
print("=" * 70)
print(f"ROW_COUNT = {len(all_rows)}")
# Spot check: first visit from each branch
n_ids = [r["visit_id"] for r in north]
s_ids = [r["visit_id"] for r in south]
e_ids = [r["visit_id"] for r in east]
print(f"North visit_ids start: {n_ids[0]}, end: {n_ids[-1]}")
print(f"South visit_ids start: {s_ids[0]}, end: {s_ids[-1]}")
print(f"East visit_ids start: {e_ids[0]}, end: {e_ids[-1]}")
# Spot checks
for prefix, data in [("N", north), ("S", south), ("E", east)]:
    r = data[0]
    print(f"  {prefix}-1: pet_name={r['pet_name']}, species={r['species']}, owner={r['owner_name']}")

print()
print("=" * 70)
print("QUERY 2: North branch filter (north_only table)")
print("=" * 70)
print(f"ROW_COUNT = {len(north)}")
r = north[0]
print(f"First row: visit_id={r['visit_id']}, pet_name={r['pet_name']}")

print()
print("=" * 70)
print("QUERY 3: Sampled visits (max_rows=10 per file, 3 files)")
print("=" * 70)
print(f"ROW_COUNT = 30  (10 per file x 3 files)")
# The first 10 rows from each branch
sampled = north[:10] + south[:10] + east[:10]
print(f"Sampled total = {len(sampled)}")

print()
print("=" * 70)
print("QUERY 4: Species breakdown (GROUP BY species)")
print("=" * 70)
species_counts = Counter(r["species"] for r in all_rows)
for sp in sorted(species_counts.keys()):
    print(f"  {sp}: {species_counts[sp]}")
print(f"Total distinct species: {len(species_counts)}")

print()
print("=" * 70)
print("QUERY 5: Treatment cost analysis by species")
print("=" * 70)
species_costs = {}
for r in all_rows:
    sp = r["species"]
    cost = float(r["treatment_cost"])
    if sp not in species_costs:
        species_costs[sp] = []
    species_costs[sp].append(cost)

total_cost_sum = 0
for sp in sorted(species_costs.keys()):
    costs = species_costs[sp]
    avg_c = sum(costs) / len(costs)
    sum_c = sum(costs)
    total_cost_sum += sum_c
    print(f"  {sp}: count={len(costs)}, avg={avg_c:.2f}, sum={sum_c:.2f}")
print(f"Grand total treatment_cost sum: {total_cost_sum:.2f}")

print()
print("=" * 70)
print("QUERY 6: NULL handling")
print("=" * 70)
null_breed = sum(1 for r in all_rows if r["breed"] == "")
null_diag = sum(1 for r in all_rows if r["diagnosis"] == "")
print(f"NULL breed count: {null_breed}")
print(f"NULL diagnosis count: {null_diag}")

print()
print("=" * 70)
print("VERIFY: Grand totals")
print("=" * 70)
total_rows = len(all_rows)
distinct_species = len(species_counts)
sum_treatment = sum(float(r["treatment_cost"]) for r in all_rows)
avg_weight = sum(float(r["weight_kg"]) for r in all_rows) / len(all_rows)
print(f"total_rows = {total_rows}")
print(f"distinct_species = {distinct_species}")
print(f"sum_treatment_cost = {sum_treatment:.2f}")
print(f"avg_weight_kg = {avg_weight:.2f}")

# Additional spot checks for assertions
print()
print("=" * 70)
print("SPOT CHECK VALUES for queries.sql assertions")
print("=" * 70)
# For north_only first/last row
print(f"north[0] visit_id={north[0]['visit_id']}, pet={north[0]['pet_name']}, species={north[0]['species']}")
print(f"north[-1] visit_id={north[-1]['visit_id']}, pet={north[-1]['pet_name']}")

# Vaccinated counts
vacc_true = sum(1 for r in all_rows if r["vaccinated"] == "true")
vacc_false = sum(1 for r in all_rows if r["vaccinated"] == "false")
print(f"vaccinated=true: {vacc_true}, vaccinated=false: {vacc_false}")

# Non-null breed/diagnosis counts (for NULL query)
non_null_breed = sum(1 for r in all_rows if r["breed"] != "")
non_null_diag = sum(1 for r in all_rows if r["diagnosis"] != "")
print(f"non_null_breed = {non_null_breed}")
print(f"non_null_diagnosis = {non_null_diag}")

# Species counts for assertion
print()
print("Species counts for ASSERT:")
for sp in sorted(species_counts.keys()):
    print(f"  species_count_{sp.lower()} = {species_counts[sp]}")

# Per-branch null counts
for prefix, data, name in [("N", north, "north"), ("S", south, "south"), ("E", east, "east")]:
    nb = sum(1 for r in data if r["breed"] == "")
    nd = sum(1 for r in data if r["diagnosis"] == "")
    print(f"  {name}: null_breed={nb}, null_diag={nd}")
