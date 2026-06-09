#!/usr/bin/env python3
"""Generate veterinary clinic CSV data for 3 branches."""
import csv
import os
import random

random.seed(42)

BASE = os.path.dirname(os.path.abspath(__file__))

# Species with realistic weight ranges and common breeds
SPECIES_DATA = {
    "Dog": {
        "weight_range": (5.0, 45.0),
        "breeds": ["Labrador", "Beagle", "Poodle", "Bulldog", "Golden Retriever", "German Shepherd", "Dachshund", None],
    },
    "Cat": {
        "weight_range": (3.0, 7.0),
        "breeds": ["Persian", "Siamese", "Maine Coon", "British Shorthair", "Ragdoll", None],
    },
    "Rabbit": {
        "weight_range": (1.0, 3.0),
        "breeds": ["Holland Lop", "Rex", "Flemish Giant", "Mini Lop", None],
    },
    "Bird": {
        "weight_range": (0.1, 1.5),
        "breeds": ["Budgerigar", "Cockatiel", "Canary", None],
    },
    "Hamster": {
        "weight_range": (0.03, 0.15),
        "breeds": ["Syrian", "Dwarf", None],
    },
}

DIAGNOSES = [
    "Annual checkup", "Ear infection", "Skin allergy", "Dental cleaning",
    "Vaccination update", "Sprain", "Upper respiratory infection",
    "Parasite treatment", "Eye infection", "Digestive issues",
    "Fracture", "Arthritis", "Urinary infection", None,
]

OWNER_NAMES = [
    "Sarah Johnson", "Michael Chen", "Emily Rodriguez", "David Kim",
    "Jessica Martinez", "Robert Taylor", "Amanda Wilson", "James Brown",
    "Laura Davis", "Thomas Anderson", "Maria Garcia", "William Lee",
    "Jennifer White", "Christopher Moore", "Stephanie Clark",
    "Daniel Harris", "Rachel Lewis", "Andrew Walker", "Michelle Hall",
    "Kevin Young",
]

PET_NAMES = [
    "Buddy", "Luna", "Max", "Bella", "Charlie", "Daisy", "Rocky", "Molly",
    "Duke", "Sadie", "Bear", "Maggie", "Tucker", "Sophie", "Jack",
    "Chloe", "Oliver", "Lily", "Zeus", "Penny", "Milo", "Rosie",
    "Leo", "Ruby", "Finn", "Willow", "Oscar", "Coco", "Ginger", "Simba",
]

COST_RANGES = {
    "Annual checkup": (50, 100),
    "Ear infection": (75, 150),
    "Skin allergy": (100, 250),
    "Dental cleaning": (150, 350),
    "Vaccination update": (25, 75),
    "Sprain": (100, 300),
    "Upper respiratory infection": (75, 200),
    "Parasite treatment": (50, 125),
    "Eye infection": (75, 175),
    "Digestive issues": (100, 250),
    "Fracture": (250, 500),
    "Arthritis": (100, 200),
    "Urinary infection": (100, 250),
}

def generate_branch(prefix, branch_dir, branch_name):
    rows = []
    # Decide which rows get NULL breed (3-4 per file) and NULL diagnosis (1-2 per file)
    null_breed_indices = random.sample(range(25), random.randint(3, 4))
    null_diag_indices = random.sample(range(25), random.randint(1, 2))

    for i in range(25):
        visit_id = f"{prefix}-{i+1}"
        species = random.choice(list(SPECIES_DATA.keys()))
        sdata = SPECIES_DATA[species]

        if i in null_breed_indices:
            breed = ""
        else:
            breeds_no_none = [b for b in sdata["breeds"] if b is not None]
            breed = random.choice(breeds_no_none)

        pet_name = random.choice(PET_NAMES)
        owner_name = random.choice(OWNER_NAMES)
        day = random.randint(1, 31)
        if day > 28:
            day = random.randint(1, 28)  # keep January safe
        visit_date = f"2025-01-{day:02d}"

        if i in null_diag_indices:
            diagnosis = ""
            cost = round(random.uniform(50, 150), 2)
            # Use round numbers
            cost = round(cost / 5) * 5
            cost = float(f"{cost:.2f}")
        else:
            diag_options = [d for d in DIAGNOSES if d is not None]
            diagnosis = random.choice(diag_options)
            lo, hi = COST_RANGES[diagnosis]
            cost = round(random.uniform(lo, hi) / 5) * 5
            cost = float(f"{cost:.2f}")

        wlo, whi = sdata["weight_range"]
        weight = round(random.uniform(wlo, whi), 2)

        vaccinated = random.choice(["true", "false"])

        rows.append({
            "visit_id": visit_id,
            "pet_name": pet_name,
            "species": species,
            "breed": breed,
            "owner_name": owner_name,
            "visit_date": visit_date,
            "diagnosis": diagnosis,
            "treatment_cost": f"{cost:.2f}",
            "weight_kg": f"{weight:.2f}",
            "vaccinated": vaccinated,
        })

    filepath = os.path.join(BASE, "data", branch_dir, "visits.csv")
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "visit_id", "pet_name", "species", "breed", "owner_name",
            "visit_date", "diagnosis", "treatment_cost", "weight_kg", "vaccinated"
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {filepath} with {len(rows)} rows")
    return rows

all_rows = []
all_rows.extend(generate_branch("N", "branch-north", "North"))
all_rows.extend(generate_branch("S", "branch-south", "South"))
all_rows.extend(generate_branch("E", "branch-east", "East"))
print(f"\nTotal rows: {len(all_rows)}")
