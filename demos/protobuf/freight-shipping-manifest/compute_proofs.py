#!/usr/bin/env python3
"""
Compute all assertion proof values for the freight shipping manifest demo.
This script independently verifies every ASSERT value in queries.sql.
"""

# All shipment data extracted from generate_data.py
# (shipment_id, origin, destination, status, is_express, is_insured, total_cost_cents, packages, tracking_count)

# Package: (package_id, description, weight_kg, (l,w,h), class, requires_sig, declared_value_cents)
# Tracking count per shipment

shipments = [
    # File 1: carrier_alpha — 5 shipments
    {
        "id": "SHIP-A001", "origin": "New York, NY", "dest": "Los Angeles, CA",
        "status": "DELIVERED", "express": True, "insured": True, "cost": 125000,
        "file": "carrier_alpha", "tracking_count": 4,
        "packages": [
            {"id": "PKG-A001-1", "desc": "Electronics", "weight": 2.5, "dims": (40,30,20), "class": "FRAGILE", "sig": True, "value": 89900},
            {"id": "PKG-A001-2", "desc": "Accessories", "weight": 0.8, "dims": (20,15,10), "class": "STANDARD", "sig": False, "value": 15000},
            {"id": "PKG-A001-3", "desc": "Cables", "weight": 0.3, "dims": (15,10,5), "class": "STANDARD", "sig": False, "value": 5000},
        ],
    },
    {
        "id": "SHIP-A002", "origin": "Chicago, IL", "dest": "Miami, FL",
        "status": "IN_TRANSIT", "express": False, "insured": True, "cost": 75000,
        "file": "carrier_alpha", "tracking_count": 3,
        "packages": [
            {"id": "PKG-A002-1", "desc": "Laboratory Samples", "weight": 5.0, "dims": (50,40,30), "class": "HAZMAT", "sig": True, "value": 200000},
            {"id": "PKG-A002-2", "desc": "Lab Equipment", "weight": 3.2, "dims": (35,25,20), "class": "FRAGILE", "sig": True, "value": 180000},
        ],
    },
    {
        "id": "SHIP-A003", "origin": "Seattle, WA", "dest": "Denver, CO",
        "status": "DELIVERED", "express": True, "insured": False, "cost": 45000,
        "file": "carrier_alpha", "tracking_count": 4,
        "packages": [
            {"id": "PKG-A003-1", "desc": "Books", "weight": 4.0, "dims": (30,25,15), "class": "STANDARD", "sig": False, "value": 12000},
        ],
    },
    {
        "id": "SHIP-A004", "origin": "Boston, MA", "dest": "Atlanta, GA",
        "status": "PICKED_UP", "express": False, "insured": False, "cost": 32000,
        "file": "carrier_alpha", "tracking_count": 2,
        "packages": [
            {"id": "PKG-A004-1", "desc": "Frozen Goods", "weight": 8.0, "dims": (45,35,30), "class": "PERISHABLE", "sig": True, "value": 50000},
            {"id": "PKG-A004-2", "desc": "Ice Packs", "weight": 2.0, "dims": (30,20,15), "class": "PERISHABLE", "sig": False, "value": 2000},
        ],
    },
    {
        "id": "SHIP-A005", "origin": "Houston, TX", "dest": "Phoenix, AZ",
        "status": "RETURNED", "express": False, "insured": True, "cost": 58000,
        "file": "carrier_alpha", "tracking_count": 5,
        "packages": [
            {"id": "PKG-A005-1", "desc": "Furniture Part A", "weight": 15.0, "dims": (80,60,40), "class": "FRAGILE", "sig": True, "value": 150000},
            {"id": "PKG-A005-2", "desc": "Furniture Part B", "weight": 12.0, "dims": (70,50,35), "class": "STANDARD", "sig": False, "value": 85000},
        ],
    },
    # File 2: carrier_beta — 4 shipments
    {
        "id": "SHIP-B001", "origin": "San Francisco, CA", "dest": "Portland, OR",
        "status": "DELIVERED", "express": True, "insured": True, "cost": 28000,
        "file": "carrier_beta", "tracking_count": 4,
        "packages": [
            {"id": "PKG-B001-1", "desc": "Wine Collection", "weight": 6.5, "dims": (40,30,35), "class": "FRAGILE", "sig": True, "value": 300000},
        ],
    },
    {
        "id": "SHIP-B002", "origin": "Dallas, TX", "dest": "Nashville, TN",
        "status": "IN_TRANSIT", "express": False, "insured": False, "cost": 42000,
        "file": "carrier_beta", "tracking_count": 3,
        "packages": [
            {"id": "PKG-B002-1", "desc": "Clothing", "weight": 1.5, "dims": (40,30,10), "class": "STANDARD", "sig": False, "value": 25000},
            {"id": "PKG-B002-2", "desc": "Shoes", "weight": 1.2, "dims": (35,25,15), "class": "STANDARD", "sig": False, "value": 18000},
            {"id": "PKG-B002-3", "desc": "Accessories", "weight": 0.4, "dims": (20,15,8), "class": "STANDARD", "sig": False, "value": 8000},
        ],
    },
    {
        "id": "SHIP-B003", "origin": "Philadelphia, PA", "dest": "Washington, DC",
        "status": "CREATED", "express": False, "insured": False, "cost": 15000,
        "file": "carrier_beta", "tracking_count": 1,
        "packages": [
            {"id": "PKG-B003-1", "desc": "Documents", "weight": 0.5, "dims": (35,25,5), "class": "STANDARD", "sig": True, "value": 0},
            {"id": "PKG-B003-2", "desc": "Archive Box", "weight": 3.0, "dims": (40,30,25), "class": "STANDARD", "sig": False, "value": 0},
        ],
    },
    {
        "id": "SHIP-B004", "origin": "Minneapolis, MN", "dest": "Detroit, MI",
        "status": "DELIVERED", "express": True, "insured": True, "cost": 95000,
        "file": "carrier_beta", "tracking_count": 4,
        "packages": [
            {"id": "PKG-B004-1", "desc": "Medical Supplies", "weight": 4.5, "dims": (50,35,25), "class": "PERISHABLE", "sig": True, "value": 450000},
            {"id": "PKG-B004-2", "desc": "Medical Instruments", "weight": 2.0, "dims": (30,20,15), "class": "FRAGILE", "sig": True, "value": 250000},
        ],
    },
    # File 3: carrier_gamma — 3 shipments
    {
        "id": "SHIP-C001", "origin": "Las Vegas, NV", "dest": "Salt Lake City, UT",
        "status": "DELIVERED", "express": False, "insured": True, "cost": 38000,
        "file": "carrier_gamma", "tracking_count": 4,
        "packages": [
            {"id": "PKG-C001-1", "desc": "Industrial Chemicals", "weight": 10.0, "dims": (50,40,35), "class": "HAZMAT", "sig": True, "value": 120000},
            {"id": "PKG-C001-2", "desc": "Safety Equipment", "weight": 3.5, "dims": (40,30,20), "class": "STANDARD", "sig": False, "value": 35000},
        ],
    },
    {
        "id": "SHIP-C002", "origin": "Orlando, FL", "dest": "Charlotte, NC",
        "status": "IN_TRANSIT", "express": True, "insured": False, "cost": 22000,
        "file": "carrier_gamma", "tracking_count": 3,
        "packages": [
            {"id": "PKG-C002-1", "desc": "Fresh Produce", "weight": 7.0, "dims": (45,35,25), "class": "PERISHABLE", "sig": False, "value": 8000},
        ],
    },
    {
        "id": "SHIP-C003", "origin": "Tampa, FL", "dest": "Raleigh, NC",
        "status": "PICKED_UP", "express": False, "insured": True, "cost": 67000,
        "file": "carrier_gamma", "tracking_count": 2,
        "packages": [
            {"id": "PKG-C003-1", "desc": "Art Piece", "weight": 8.0, "dims": (100,70,10), "class": "FRAGILE", "sig": True, "value": 500000},
            {"id": "PKG-C003-2", "desc": "Art Frame", "weight": 5.0, "dims": (90,60,10), "class": "FRAGILE", "sig": True, "value": 75000},
            {"id": "PKG-C003-3", "desc": "Packing Materials", "weight": 2.0, "dims": (60,40,30), "class": "STANDARD", "sig": False, "value": 500},
        ],
    },
]

all_packages = []
for s in shipments:
    for p in s["packages"]:
        p["shipment_id"] = s["id"]
        p["shipment_status"] = s["status"]
        p["express"] = s["express"]
        all_packages.append(p)

print("=" * 70)
print("PROOF VALUES FOR queries.sql")
print("=" * 70)

# Query 1: Shipment overview
print(f"\n--- Query 1: Shipment Overview ---")
print(f"Total shipments: {len(shipments)}")
print(f"SHIP-A001 status: {shipments[0]['status']}")
print(f"SHIP-A001 origin: {shipments[0]['origin']}")
print(f"SHIP-B003 status: {shipments[7]['status']}")

# Query 2: Package inventory (all 24 packages)
print(f"\n--- Query 2: Package Inventory ---")
print(f"Total packages: {len(all_packages)}")
# Check 3-level nesting: PKG-A001-1 dimensions
pkg_a001_1 = all_packages[0]
print(f"PKG-A001-1 length_cm: {pkg_a001_1['dims'][0]}")
print(f"PKG-A001-1 width_cm: {pkg_a001_1['dims'][1]}")
print(f"PKG-A001-1 height_cm: {pkg_a001_1['dims'][2]}")
print(f"PKG-A001-1 weight_kg: {pkg_a001_1['weight']}")

# Query 3: Tracking timeline
total_tracking = sum(s["tracking_count"] for s in shipments)
print(f"\n--- Query 3: Tracking Timeline ---")
print(f"Total tracking events: {total_tracking}")
# SHIP-A005 has most tracking events (5)
a005_tracking = [s for s in shipments if s["id"] == "SHIP-A005"][0]["tracking_count"]
print(f"SHIP-A005 tracking events: {a005_tracking}")

# Query 4: Status distribution
from collections import Counter
status_counts = Counter(s["status"] for s in shipments)
print(f"\n--- Query 4: Status Distribution ---")
for status in sorted(status_counts.keys()):
    print(f"  {status}: {status_counts[status]}")

# Query 5: Package class distribution
class_counts = Counter(p["class"] for p in all_packages)
print(f"\n--- Query 5: Package Class Distribution ---")
for cls in sorted(class_counts.keys()):
    print(f"  {cls}: {class_counts[cls]}")

# Query 6: Bool fields — Express shipment analysis
print(f"\n--- Query 6: Express Analysis (bool fields) ---")
express = [s for s in shipments if s["express"]]
non_express = [s for s in shipments if not s["express"]]
print(f"Express shipments: {len(express)}")
print(f"Non-express shipments: {len(non_express)}")
express_total_cost = sum(s["cost"] for s in express)
non_express_total_cost = sum(s["cost"] for s in non_express)
print(f"Express total cost cents: {express_total_cost}")
print(f"Non-express total cost cents: {non_express_total_cost}")
express_avg = round(express_total_cost / len(express))
non_express_avg = round(non_express_total_cost / len(non_express))
print(f"Express avg cost cents: {express_avg}")
print(f"Non-express avg cost cents: {non_express_avg}")

# Query 7: Bool fields — Signature required analysis
print(f"\n--- Query 7: Signature Required (bool field) ---")
sig_required = [p for p in all_packages if p["sig"]]
no_sig = [p for p in all_packages if not p["sig"]]
print(f"Requires signature: {len(sig_required)}")
print(f"No signature: {len(no_sig)}")

# Query 8: Int64 — High-value packages (declared_value_cents > 100000)
print(f"\n--- Query 8: High-Value Packages (int64 field) ---")
high_value = [p for p in all_packages if p["value"] > 100000]
print(f"High-value packages (>$1000): {len(high_value)}")
total_high_value = sum(p["value"] for p in high_value)
print(f"Total high-value cents: {total_high_value}")
max_value_pkg = max(all_packages, key=lambda p: p["value"])
print(f"Most valuable: {max_value_pkg['id']} = {max_value_pkg['value']}")

# Query 9: Float — Weight analysis
print(f"\n--- Query 9: Weight Analysis (float field) ---")
total_weight = sum(p["weight"] for p in all_packages)
print(f"Total weight kg: {total_weight}")
heaviest = max(all_packages, key=lambda p: p["weight"])
lightest = min(all_packages, key=lambda p: p["weight"])
print(f"Heaviest: {heaviest['id']} = {heaviest['weight']} kg")
print(f"Lightest: {lightest['id']} = {lightest['weight']} kg")
avg_weight = round(total_weight / len(all_packages), 1)
print(f"Average weight: {avg_weight} kg")

# Query 10: 3-level nesting — Volume calculation
print(f"\n--- Query 10: Volume Analysis (3-level nesting) ---")
for p in all_packages:
    p["volume"] = p["dims"][0] * p["dims"][1] * p["dims"][2]
largest_vol = max(all_packages, key=lambda p: p["volume"])
smallest_vol = min(all_packages, key=lambda p: p["volume"])
print(f"Largest volume: {largest_vol['id']} = {largest_vol['volume']} cm³ ({largest_vol['dims']})")
print(f"Smallest volume: {smallest_vol['id']} = {smallest_vol['volume']} cm³ ({smallest_vol['dims']})")
total_volume = sum(p["volume"] for p in all_packages)
print(f"Total volume: {total_volume} cm³")

# Query 11: Carrier file distribution
print(f"\n--- Query 11: Carrier File Distribution ---")
file_counts = Counter(s["file"] for s in shipments)
for f in sorted(file_counts.keys()):
    print(f"  {f}: {file_counts[f]} shipments")

# Packages per carrier
file_pkg_counts = Counter(p["shipment_id"][:6] for p in all_packages)  # A/B/C
alpha_pkgs = sum(1 for p in all_packages if p["shipment_id"].startswith("SHIP-A"))
beta_pkgs = sum(1 for p in all_packages if p["shipment_id"].startswith("SHIP-B"))
gamma_pkgs = sum(1 for p in all_packages if p["shipment_id"].startswith("SHIP-C"))
print(f"  carrier_alpha packages: {alpha_pkgs}")
print(f"  carrier_beta packages: {beta_pkgs}")
print(f"  carrier_gamma packages: {gamma_pkgs}")

# Query 12: Insured analysis
print(f"\n--- Query 12: Insured Shipments ---")
insured = [s for s in shipments if s["insured"]]
print(f"Insured shipments: {len(insured)}")
insured_total = sum(s["cost"] for s in insured)
print(f"Insured total cost: {insured_total}")

# Query 13: Packages per shipment
print(f"\n--- Query 13: Packages per Shipment ---")
for s in shipments:
    print(f"  {s['id']}: {len(s['packages'])} packages")
max_pkgs = max(shipments, key=lambda s: len(s["packages"]))
print(f"Most packages: {max_pkgs['id']} with {len(max_pkgs['packages'])}")

# VERIFY checks
print(f"\n--- VERIFY: Grand Totals ---")
print(f"shipment_count_12: {len(shipments)}")
print(f"package_rows_24: {len(all_packages)}")
print(f"tracking_rows_39: {total_tracking}")
print(f"five_statuses: {len(status_counts)}")
print(f"four_package_classes: {len(class_counts)}")
print(f"five_express: {len(express)}")
print(f"twelve_require_sig: {len(sig_required)}")
print(f"three_source_files: {len(file_counts)}")

# Additional computed values
print(f"\n--- Additional Computed Values ---")
print(f"Total cost all shipments: {sum(s['cost'] for s in shipments)}")
print(f"Total declared value all packages: {sum(p['value'] for p in all_packages)}")
print(f"Zero-value packages: {sum(1 for p in all_packages if p['value'] == 0)}")

# Tracking per shipment for the most/least
max_track = max(shipments, key=lambda s: s["tracking_count"])
min_track = min(shipments, key=lambda s: s["tracking_count"])
print(f"Most tracking: {max_track['id']} with {max_track['tracking_count']}")
print(f"Least tracking: {min_track['id']} with {min_track['tracking_count']}")
