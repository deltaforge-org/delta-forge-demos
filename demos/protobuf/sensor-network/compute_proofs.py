#!/usr/bin/env python3
"""Compute expected values for ASSERT statements in queries.sql."""

# Sensor data: (sensor_id, sensor_type, location, status, num_readings, file)
sensors = [
    # Factory Floor A (8 sensors)
    ("TEMP-A001", "temperature", "Line-A", "active", 4, "factory_floor_a.pb"),
    ("TEMP-A002", "temperature", "Line-A", "active", 4, "factory_floor_a.pb"),
    ("TEMP-A003", "temperature", "Line-A", "maintenance", 4, "factory_floor_a.pb"),
    ("HUM-A001",  "humidity",    "Line-A", "active", 4, "factory_floor_a.pb"),
    ("HUM-A002",  "humidity",    "Line-A", "active", 4, "factory_floor_a.pb"),
    ("HUM-A003",  "humidity",    "Line-A", "active", 5, "factory_floor_a.pb"),
    ("VIB-A001",  "vibration",   "Line-A", "active", 4, "factory_floor_a.pb"),
    ("VIB-A002",  "vibration",   "Line-A", "active", 4, "factory_floor_a.pb"),
    # Factory Floor B (7 sensors)
    ("TEMP-B001", "temperature", "Line-B", "active", 4, "factory_floor_b.pb"),
    ("TEMP-B002", "temperature", "Line-B", "offline", 4, "factory_floor_b.pb"),
    ("HUM-B001",  "humidity",    "Line-B", "active", 4, "factory_floor_b.pb"),
    ("HUM-B002",  "humidity",    "Line-B", "active", 5, "factory_floor_b.pb"),
    ("VIB-B001",  "vibration",   "Line-B", "active", 4, "factory_floor_b.pb"),
    ("VIB-B002",  "vibration",   "Line-B", "maintenance", 4, "factory_floor_b.pb"),
    ("VIB-B003",  "vibration",   "Line-B", "active", 4, "factory_floor_b.pb"),
    # Warehouse (5 sensors)
    ("TEMP-W001", "temperature", "Warehouse", "active", 4, "warehouse.pb"),
    ("TEMP-W002", "temperature", "Warehouse", "active", 4, "warehouse.pb"),
    ("HUM-W001",  "humidity",    "Warehouse", "active", 4, "warehouse.pb"),
    ("HUM-W002",  "humidity",    "Warehouse", "maintenance", 4, "warehouse.pb"),
    ("VIB-W001",  "vibration",   "Warehouse", "active", 4, "warehouse.pb"),
]

print("=== Sensor Summary (sensor_summary table) ===")
print(f"Total sensors (ROW_COUNT): {len(sensors)}")

print("\n=== Exploded Readings (sensor_readings table) ===")
total_readings = sum(s[4] for s in sensors)
print(f"Total readings (ROW_COUNT): {total_readings}")

print("\n=== Query 3: Sensor Type Breakdown ===")
from collections import Counter, defaultdict
type_counts = Counter(s[1] for s in sensors)
for t, c in sorted(type_counts.items()):
    print(f"  {t}: {c} sensors")

# Readings per type
type_readings = defaultdict(int)
for s in sensors:
    type_readings[s[1]] += s[4]
for t in sorted(type_readings):
    print(f"  {t}: {type_readings[t]} readings")

print("\n=== Query 4: Location Analysis ===")
loc_sensors = Counter(s[2] for s in sensors)
loc_readings = defaultdict(int)
for s in sensors:
    loc_readings[s[2]] += s[4]
for loc in sorted(loc_sensors):
    print(f"  {loc}: {loc_sensors[loc]} sensors, {loc_readings[loc]} readings")

print("\n=== Query 5: Status Check ===")
status_counts = Counter(s[3] for s in sensors)
for st, c in sorted(status_counts.items()):
    print(f"  {st}: {c}")

print("\n=== Query 6: File Source ===")
file_counts = Counter(s[5] for s in sensors)
for f, c in sorted(file_counts.items()):
    print(f"  {f}: {c} sensors")

print("\n=== Reading values for avg computation ===")
# Temperature readings
temp_values = [
    22.5, 23.1, 22.8, 23.4,  # TEMP-A001
    21.7, 22.0, 22.3, 21.9,  # TEMP-A002
    19.2, 18.5, 18.0, 18.3,  # TEMP-A003
    24.6, 25.0, 24.8, 25.2,  # TEMP-B001
    31.2, 30.8, 32.0, 31.5,  # TEMP-B002
    18.3, 18.1, 18.5, 18.2,  # TEMP-W001
    19.7, 20.0, 19.5, 20.2,  # TEMP-W002
]
humidity_values = [
    45.2, 46.8, 44.5, 47.1,  # HUM-A001
    52.3, 51.0, 53.7, 52.8,  # HUM-A002
    48.9, 49.2, 50.1, 48.4, 49.8,  # HUM-A003
    38.4, 39.1, 37.8, 38.7,  # HUM-B001
    61.5, 62.3, 60.8, 63.0, 61.2,  # HUM-B002
    55.0, 56.2, 54.8, 55.5,  # HUM-W001
    72.1, 73.5, 71.8, 72.9,  # HUM-W002
]
vibration_values = [
    2.3, 2.5, 2.1, 2.4,  # VIB-A001
    4.7, 5.2, 4.9, 5.1,  # VIB-A002
    1.2, 1.5, 1.3, 1.4,  # VIB-B001
    7.8, 8.2, 8.5, 7.9,  # VIB-B002
    3.4, 3.6, 3.2, 3.5,  # VIB-B003
    0.5, 0.3, 0.4, 0.6,  # VIB-W001
]

print(f"  temperature: count={len(temp_values)}, avg={sum(temp_values)/len(temp_values):.4f}")
print(f"  humidity:    count={len(humidity_values)}, avg={sum(humidity_values)/len(humidity_values):.4f}")
print(f"  vibration:   count={len(vibration_values)}, avg={sum(vibration_values)/len(vibration_values):.4f}")
print(f"  Total readings: {len(temp_values) + len(humidity_values) + len(vibration_values)}")
