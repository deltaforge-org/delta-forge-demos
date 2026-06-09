#!/usr/bin/env python3
"""
Deterministic data generator for the Concurrent CDR Ingestion demo.

Scenario: a mobile carrier runs 10 regional Mobile Switching Centers (MSCs).
Each MSC exports a daily Call Detail Record (CDR) file with an identical
schema. The nightly mediation job loads all 10 files concurrently into one
central rated-CDR Delta table.

This script writes 10 CSV files (region_01.csv .. region_10.csv) into ./data.
Every value is produced from a fixed RNG seed so the data is reproducible and
the demo's ASSERT proof values are stable across regenerations.

Run:  python3 generate_data.py
"""

import csv
import os
import random

SEED = 20260601
random.seed(SEED)

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")

# region_id -> (region_name, row_count). Counts deliberately vary per region so
# per-feed integrity can be proven (each file's rows must all land, undamaged).
REGIONS = [
    (1,  "North-Coast",  2150),
    (2,  "Capital-Metro", 1820),
    (3,  "Lakeside",     2470),
    (4,  "Highland",     1560),
    (5,  "River-Valley", 2030),
    (6,  "Eastport",     2310),
    (7,  "Sunbelt",      1690),
    (8,  "Pinewood",     2120),
    (9,  "Goldfield",    1940),
    (10, "Bayshore",     2260),
]

# Call type mix (weights) and network mix (weights).
CALL_TYPES = ["VOICE", "SMS", "DATA"]
CALL_TYPE_WEIGHTS = [50, 30, 20]

NETWORKS = ["3G", "4G", "5G"]
NETWORK_WEIGHTS = [15, 55, 30]

# VOICE per-minute rate by network generation (carrier price book).
VOICE_RATE = {"3G": 0.12, "4G": 0.10, "5G": 0.08}
SMS_FLAT_CHARGE = 0.05          # flat per-message charge
DATA_RATE_PER_MB = 0.01         # 1 cent per megabyte

HEADER = [
    "cdr_id",
    "region_id",
    "region_name",
    "caller_number",
    "callee_number",
    "call_start",
    "duration_seconds",
    "call_type",
    "bytes_transferred",
    "network_type",
    "rate_per_minute",
    "charge_amount",
]


def phone() -> str:
    """A clearly-string phone number that CSV inference will not read as a number."""
    return "+1-{:03d}-{:03d}-{:04d}".format(
        random.randint(200, 989),
        random.randint(200, 999),
        random.randint(0, 9999),
    )


def call_start() -> str:
    """ISO-ish timestamp within a single operating day."""
    return "2026-06-01 {:02d}:{:02d}:{:02d}".format(
        random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)
    )


def make_row(region_id: int, region_name: str, seq: int) -> dict:
    cdr_id = "R{:02d}-{:07d}".format(region_id, seq)
    call_type = random.choices(CALL_TYPES, weights=CALL_TYPE_WEIGHTS, k=1)[0]
    network = random.choices(NETWORKS, weights=NETWORK_WEIGHTS, k=1)[0]
    caller = phone()

    if call_type == "DATA":
        # Data session: no callee party, no per-minute rate; billed on volume.
        callee = ""  # NULL in the warehouse
        duration = random.randint(30, 7200)
        nbytes = random.randint(100_000, 500_000_000)
        rate = 0.0
        charge = round(nbytes / 1_000_000.0 * DATA_RATE_PER_MB, 2)
        bytes_field = str(nbytes)
    elif call_type == "SMS":
        callee = phone()
        duration = 0
        rate = 0.0
        charge = SMS_FLAT_CHARGE
        bytes_field = ""  # NULL: messages do not transfer billable data volume
    else:  # VOICE
        callee = phone()
        # ~5% of voice calls are dropped (zero duration, zero charge).
        duration = 0 if random.random() < 0.05 else random.randint(15, 3600)
        rate = VOICE_RATE[network]
        charge = round(duration / 60.0 * rate, 2)
        bytes_field = ""  # NULL: voice calls do not transfer billable data volume

    return {
        "cdr_id": cdr_id,
        "region_id": region_id,
        "region_name": region_name,
        "caller_number": caller,
        "callee_number": callee,
        "call_start": call_start(),
        "duration_seconds": duration,
        "call_type": call_type,
        "bytes_transferred": bytes_field,
        "network_type": network,
        "rate_per_minute": rate,
        "charge_amount": charge,
    }


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    grand_total = 0
    for region_id, region_name, count in REGIONS:
        path = os.path.join(DATA_DIR, "region_{:02d}.csv".format(region_id))
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=HEADER)
            writer.writeheader()
            for seq in range(1, count + 1):
                writer.writerow(make_row(region_id, region_name, seq))
        grand_total += count
        size = os.path.getsize(path)
        print("wrote {} rows -> region_{:02d}.csv ({} bytes)".format(count, region_id, size))
    print("TOTAL rows across 10 files: {}".format(grand_total))


if __name__ == "__main__":
    main()
