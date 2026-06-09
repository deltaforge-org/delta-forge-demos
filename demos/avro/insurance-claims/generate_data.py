#!/usr/bin/env python3
"""Generate Avro data files for the Insurance Claims Processing demo."""

import os
import random
import fastavro

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

schema_v1 = {
    "type": "record",
    "name": "InsuranceClaim",
    "namespace": "com.deltaforge.demo",
    "fields": [
        {"name": "claim_id", "type": "string"},
        {"name": "policy_number", "type": "string"},
        {"name": "claimant_name", "type": "string"},
        {"name": "claim_type", "type": "string"},
        {"name": "incident_date", "type": "string"},
        {"name": "filed_date", "type": "string"},
        {"name": "amount_claimed", "type": "double"},
        {"name": "amount_approved", "type": "double"},
        {"name": "status", "type": "string"},
        {"name": "description", "type": ["null", "string"], "default": None},
    ],
}

schema_v2 = {
    "type": "record",
    "name": "InsuranceClaim",
    "namespace": "com.deltaforge.demo",
    "fields": [
        {"name": "claim_id", "type": "string"},
        {"name": "policy_number", "type": "string"},
        {"name": "claimant_name", "type": "string"},
        {"name": "claim_type", "type": "string"},
        {"name": "incident_date", "type": "string"},
        {"name": "filed_date", "type": "string"},
        {"name": "amount_claimed", "type": "double"},
        {"name": "amount_approved", "type": "double"},
        {"name": "status", "type": "string"},
        {"name": "description", "type": ["null", "string"], "default": None},
        {"name": "adjuster_name", "type": ["null", "string"], "default": None},
        {"name": "settlement_date", "type": ["null", "string"], "default": None},
    ],
}

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Dorothy", "Andrew", "Kimberly", "Paul", "Emily", "Joshua", "Donna",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
]

ADJUSTER_NAMES = [
    "Alice Chen", "Bob Marshall", "Carol Rivera", "Dan Kowalski", "Eva Petrov",
    "Frank DeLuca", "Grace Okafor", "Hank Torres", "Irene Walsh", "Jake Moreno",
]

AUTO_TYPES = ["Collision", "Comprehensive", "Liability", "Theft"]
HOME_TYPES = ["Property Damage", "Water Damage", "Fire", "Wind"]
STATUSES = ["Approved", "Denied", "Pending", "Under Review"]

# Amount ranges by claim type
AMOUNT_RANGES = {
    "Collision": (2000, 45000),
    "Comprehensive": (500, 15000),
    "Liability": (5000, 75000),
    "Theft": (1000, 30000),
    "Property Damage": (3000, 50000),
    "Water Damage": (2000, 35000),
    "Fire": (10000, 75000),
    "Wind": (1500, 25000),
}

DESCRIPTIONS_AUTO = [
    "Rear-end collision at intersection",
    "Side-swipe on highway during lane change",
    "Hail damage to windshield and roof",
    "Vehicle stolen from parking garage",
    "Multi-vehicle pile-up during fog",
    "Deer collision on rural road",
    "Parking lot fender bender",
    "Vandalism damage to exterior panels",
    "Flooded engine from flash flooding",
    "Tree branch fell on parked vehicle",
    "T-bone collision at stop sign",
    "Hit-and-run in shopping center lot",
    "Catalytic converter theft",
    "Uninsured motorist collision",
    "Rollover accident on icy bridge",
]

DESCRIPTIONS_HOME = [
    "Burst pipe caused basement flooding",
    "Kitchen fire from unattended stove",
    "Wind damage to roof shingles",
    "Fallen tree damaged front porch",
    "Sewer backup into finished basement",
    "Lightning strike caused electrical fire",
    "Tornado damaged detached garage",
    "Frozen pipes burst in attic",
    "Vandalism to exterior windows",
    "Smoke damage throughout first floor",
    "Hurricane wind removed siding panels",
    "Mold discovered behind bathroom walls",
    "Foundation crack from soil settling",
    "Ice dam caused ceiling leak",
    "Chimney fire spread to attic",
]


def random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def random_date(year, month_start, month_end):
    month = random.randint(month_start, month_end)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"


def filed_after(incident_date):
    """Return a filing date 1-14 days after the incident."""
    parts = incident_date.split("-")
    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
    d += random.randint(1, 14)
    if d > 28:
        d -= 28
        m += 1
        if m > 12:
            m = 1
            y += 1
    return f"{y}-{m:02d}-{d:02d}"


def settlement_after(filed_date):
    """Return a settlement date 14-60 days after filing."""
    parts = filed_date.split("-")
    y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
    d += random.randint(14, 28)
    while d > 28:
        d -= 28
        m += 1
        if m > 12:
            m = 1
            y += 1
    return f"{y}-{m:02d}-{d:02d}"


def generate_records(prefix, start_num, count, claim_types, descriptions, null_desc_indices, policy_start):
    records = []
    # Pre-select some shared policy numbers for repeat claimants
    policy_pool = [f"POL-{policy_start + i}" for i in range(count)]
    # Make ~5 policies shared (repeat claimants)
    for i in range(5):
        idx = random.randint(5, count - 1)
        policy_pool[idx] = policy_pool[random.randint(0, 4)]

    for i in range(count):
        claim_type = claim_types[i % len(claim_types)]
        lo, hi = AMOUNT_RANGES[claim_type]
        amount_claimed = round(random.uniform(lo, hi), 2)
        status = STATUSES[i % len(STATUSES)]

        if status == "Denied":
            amount_approved = 0.0
        elif status == "Pending" or status == "Under Review":
            amount_approved = 0.0
        elif random.random() < 0.3:
            # Partial approval
            amount_approved = round(amount_claimed * random.uniform(0.4, 0.9), 2)
        else:
            amount_approved = amount_claimed

        incident_date = random_date(2025, 1, 10)
        f_date = filed_after(incident_date)

        desc = None if i in null_desc_indices else random.choice(descriptions)

        record = {
            "claim_id": f"{prefix}{start_num + i:04d}",
            "policy_number": policy_pool[i],
            "claimant_name": random_name(),
            "claim_type": claim_type,
            "incident_date": incident_date,
            "filed_date": f_date,
            "amount_claimed": amount_claimed,
            "amount_approved": amount_approved,
            "status": status,
            "description": desc,
        }
        records.append(record)
    return records


def generate_v2_records(prefix, start_num, count, claim_types, descriptions, null_desc_indices, policy_start):
    records = generate_records(prefix, start_num, count, claim_types, descriptions, null_desc_indices, policy_start)
    for rec in records:
        status = rec["status"]
        rec["adjuster_name"] = random.choice(ADJUSTER_NAMES)
        if status in ("Pending", "Under Review"):
            rec["settlement_date"] = None
        else:
            rec["settlement_date"] = settlement_after(rec["filed_date"])
    return records


# Null description indices (5 per file)
null_indices_auto_v1 = {2, 7, 13, 19, 25}
null_indices_home_v1 = {1, 8, 14, 22, 28}
null_indices_auto_v2 = {3, 9, 16, 21, 27}

# Generate records
auto_v1_records = generate_records("CLM-A", 1, 30, AUTO_TYPES, DESCRIPTIONS_AUTO, null_indices_auto_v1, 100001)
home_v1_records = generate_records("CLM-H", 1, 30, HOME_TYPES, DESCRIPTIONS_HOME, null_indices_home_v1, 100031)
auto_v2_records = generate_v2_records("CLM-A", 2001, 30, AUTO_TYPES, DESCRIPTIONS_AUTO, null_indices_auto_v2, 100041)

# Write files
parsed_v1 = fastavro.parse_schema(schema_v1)
parsed_v2 = fastavro.parse_schema(schema_v2)

with open(os.path.join(OUTPUT_DIR, "claims_auto_v1.avro"), "wb") as f:
    fastavro.writer(f, parsed_v1, auto_v1_records, codec="null")

with open(os.path.join(OUTPUT_DIR, "claims_home_v1.avro"), "wb") as f:
    fastavro.writer(f, parsed_v1, home_v1_records, codec="deflate")

with open(os.path.join(OUTPUT_DIR, "claims_auto_v2.avro"), "wb") as f:
    fastavro.writer(f, parsed_v2, auto_v2_records, codec="deflate")

print("Avro files generated successfully.")
for fname in ["claims_auto_v1.avro", "claims_home_v1.avro", "claims_auto_v2.avro"]:
    path = os.path.join(OUTPUT_DIR, fname)
    print(f"  {fname}: {os.path.getsize(path)} bytes")
