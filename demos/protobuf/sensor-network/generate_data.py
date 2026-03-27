#!/usr/bin/env python3
"""
Generate sample protobuf binary data files for the IoT sensor network demo.
Uses raw protobuf wire encoding (no external dependencies needed).

Wire format reference:
  - Varint (type 0): int32, int64, bool, enum
  - 64-bit (type 1): double, fixed64
  - Length-delimited (type 2): string, bytes, embedded messages
  - Field key = (field_number << 3) | wire_type
"""
import struct
import os

# -- Protobuf wire encoding primitives ------------------------------------

def encode_varint(value):
    """Encode an unsigned integer as a protobuf varint."""
    if value < 0:
        value = value + (1 << 64)
    parts = []
    while value > 0x7F:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    parts.append(value & 0x7F)
    return bytes(parts)

def encode_signed_varint(value):
    """Encode a signed int32/int64 as a varint."""
    if value >= 0:
        return encode_varint(value)
    return encode_varint(value + (1 << 64))

def encode_field_key(field_number, wire_type):
    """Encode a field key (tag)."""
    return encode_varint((field_number << 3) | wire_type)

def encode_string(field_number, value):
    """Encode a string field (length-delimited, wire type 2)."""
    data = value.encode('utf-8')
    return encode_field_key(field_number, 2) + encode_varint(len(data)) + data

def encode_varint_field(field_number, value):
    """Encode a varint field (wire type 0)."""
    return encode_field_key(field_number, 0) + encode_signed_varint(value)

def encode_embedded(field_number, message_bytes):
    """Encode an embedded message (length-delimited, wire type 2)."""
    return encode_field_key(field_number, 2) + encode_varint(len(message_bytes)) + message_bytes

def encode_int64_field(field_number, value):
    """Encode an int64 varint field."""
    return encode_field_key(field_number, 0) + encode_varint(value)

def encode_double_field(field_number, value):
    """Encode a double field (64-bit, wire type 1)."""
    return encode_field_key(field_number, 1) + struct.pack('<d', value)

# -- Message encoders -----------------------------------------------------

def encode_timestamp(seconds, nanos=0):
    """Encode a google.protobuf.Timestamp message."""
    msg = b''
    if seconds != 0:
        msg += encode_int64_field(1, seconds)
    if nanos != 0:
        msg += encode_varint_field(2, nanos)
    return msg

def encode_sensor_reading(value, recorded_at_seconds, unit):
    """Encode a SensorReading message.
    Fields: value(1)=double, recorded_at(2)=Timestamp, unit(3)=string
    """
    msg = encode_double_field(1, value)
    ts_msg = encode_timestamp(recorded_at_seconds)
    msg += encode_embedded(2, ts_msg)
    msg += encode_string(3, unit)
    return msg

def encode_sensor(sensor_id, sensor_type, location, status, readings, installed_at_seconds):
    """Encode a Sensor message.
    Fields: sensor_id(1), sensor_type(2), location(3), status(4),
            readings(5)=repeated SensorReading, installed_at(6)=Timestamp
    """
    msg = encode_string(1, sensor_id)
    msg += encode_string(2, sensor_type)
    msg += encode_string(3, location)
    msg += encode_string(4, status)
    for reading in readings:
        reading_msg = encode_sensor_reading(*reading)
        msg += encode_embedded(5, reading_msg)
    ts_msg = encode_timestamp(installed_at_seconds)
    msg += encode_embedded(6, ts_msg)
    return msg

def encode_sensor_network(sensors, facility_name):
    """Encode a SensorNetwork message.
    Fields: sensors(1)=repeated Sensor, facility_name(2)=string
    """
    msg = b''
    for sensor_bytes in sensors:
        msg += encode_embedded(1, sensor_bytes)
    msg += encode_string(2, facility_name)
    return msg

# -- Timestamps (Unix epoch seconds) --------------------------------------

# Installation dates
TS_INSTALL_2023_01 = 1672531200   # 2023-01-01 00:00:00 UTC
TS_INSTALL_2023_03 = 1677628800   # 2023-03-01 00:00:00 UTC
TS_INSTALL_2023_06 = 1685577600   # 2023-06-01 00:00:00 UTC
TS_INSTALL_2023_09 = 1693526400   # 2023-09-01 00:00:00 UTC
TS_INSTALL_2024_01 = 1704067200   # 2024-01-01 00:00:00 UTC
TS_INSTALL_2024_04 = 1711929600   # 2024-04-01 00:00:00 UTC

# Reading timestamps (spread across 2025-03-15)
TS_R01 = 1742025600   # 2025-03-15 08:00:00 UTC
TS_R02 = 1742029200   # 2025-03-15 09:00:00 UTC
TS_R03 = 1742032800   # 2025-03-15 10:00:00 UTC
TS_R04 = 1742036400   # 2025-03-15 11:00:00 UTC
TS_R05 = 1742040000   # 2025-03-15 12:00:00 UTC

# -- Data generation -------------------------------------------------------

def generate():
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    os.makedirs(data_dir, exist_ok=True)

    total_sensors = 0
    total_readings = 0

    # == File 1: Factory Floor A — 8 sensors ================================
    floor_a_sensors = [
        # Temperature sensors (3)
        encode_sensor("TEMP-A001", "temperature", "Line-A", "active", [
            (22.5, TS_R01, "celsius"),
            (23.1, TS_R02, "celsius"),
            (22.8, TS_R03, "celsius"),
            (23.4, TS_R04, "celsius"),
        ], TS_INSTALL_2023_01),
        encode_sensor("TEMP-A002", "temperature", "Line-A", "active", [
            (21.7, TS_R01, "celsius"),
            (22.0, TS_R02, "celsius"),
            (22.3, TS_R03, "celsius"),
            (21.9, TS_R04, "celsius"),
        ], TS_INSTALL_2023_01),
        encode_sensor("TEMP-A003", "temperature", "Line-A", "maintenance", [
            (19.2, TS_R01, "celsius"),
            (18.5, TS_R02, "celsius"),
            (18.0, TS_R03, "celsius"),
            (18.3, TS_R04, "celsius"),
        ], TS_INSTALL_2023_03),
        # Humidity sensors (3)
        encode_sensor("HUM-A001", "humidity", "Line-A", "active", [
            (45.2, TS_R01, "percent"),
            (46.8, TS_R02, "percent"),
            (44.5, TS_R03, "percent"),
            (47.1, TS_R04, "percent"),
        ], TS_INSTALL_2023_01),
        encode_sensor("HUM-A002", "humidity", "Line-A", "active", [
            (52.3, TS_R01, "percent"),
            (51.0, TS_R02, "percent"),
            (53.7, TS_R03, "percent"),
            (52.8, TS_R04, "percent"),
        ], TS_INSTALL_2023_06),
        encode_sensor("HUM-A003", "humidity", "Line-A", "active", [
            (48.9, TS_R01, "percent"),
            (49.2, TS_R02, "percent"),
            (50.1, TS_R03, "percent"),
            (48.4, TS_R04, "percent"),
            (49.8, TS_R05, "percent"),
        ], TS_INSTALL_2024_01),
        # Vibration sensors (2)
        encode_sensor("VIB-A001", "vibration", "Line-A", "active", [
            (2.3, TS_R01, "mm_per_s"),
            (2.5, TS_R02, "mm_per_s"),
            (2.1, TS_R03, "mm_per_s"),
            (2.4, TS_R04, "mm_per_s"),
        ], TS_INSTALL_2023_03),
        encode_sensor("VIB-A002", "vibration", "Line-A", "active", [
            (4.7, TS_R01, "mm_per_s"),
            (5.2, TS_R02, "mm_per_s"),
            (4.9, TS_R03, "mm_per_s"),
            (5.1, TS_R04, "mm_per_s"),
        ], TS_INSTALL_2023_09),
    ]
    floor_a_reading_counts = [4, 4, 4, 4, 4, 5, 4, 4]
    total_sensors += 8
    total_readings += sum(floor_a_reading_counts)

    floor_a_msg = encode_sensor_network(floor_a_sensors, "Factory Floor A")
    with open(os.path.join(data_dir, 'factory_floor_a.pb'), 'wb') as f:
        f.write(encode_varint(len(floor_a_msg)) + floor_a_msg)

    # == File 2: Factory Floor B — 7 sensors ================================
    floor_b_sensors = [
        # Temperature sensors (2)
        encode_sensor("TEMP-B001", "temperature", "Line-B", "active", [
            (24.6, TS_R01, "celsius"),
            (25.0, TS_R02, "celsius"),
            (24.8, TS_R03, "celsius"),
            (25.2, TS_R04, "celsius"),
        ], TS_INSTALL_2023_01),
        encode_sensor("TEMP-B002", "temperature", "Line-B", "offline", [
            (31.2, TS_R01, "celsius"),
            (30.8, TS_R02, "celsius"),
            (32.0, TS_R03, "celsius"),
            (31.5, TS_R04, "celsius"),
        ], TS_INSTALL_2023_06),
        # Humidity sensors (2)
        encode_sensor("HUM-B001", "humidity", "Line-B", "active", [
            (38.4, TS_R01, "percent"),
            (39.1, TS_R02, "percent"),
            (37.8, TS_R03, "percent"),
            (38.7, TS_R04, "percent"),
        ], TS_INSTALL_2023_03),
        encode_sensor("HUM-B002", "humidity", "Line-B", "active", [
            (61.5, TS_R01, "percent"),
            (62.3, TS_R02, "percent"),
            (60.8, TS_R03, "percent"),
            (63.0, TS_R04, "percent"),
            (61.2, TS_R05, "percent"),
        ], TS_INSTALL_2023_09),
        # Vibration sensors (3)
        encode_sensor("VIB-B001", "vibration", "Line-B", "active", [
            (1.2, TS_R01, "mm_per_s"),
            (1.5, TS_R02, "mm_per_s"),
            (1.3, TS_R03, "mm_per_s"),
            (1.4, TS_R04, "mm_per_s"),
        ], TS_INSTALL_2023_06),
        encode_sensor("VIB-B002", "vibration", "Line-B", "maintenance", [
            (7.8, TS_R01, "mm_per_s"),
            (8.2, TS_R02, "mm_per_s"),
            (8.5, TS_R03, "mm_per_s"),
            (7.9, TS_R04, "mm_per_s"),
        ], TS_INSTALL_2024_01),
        encode_sensor("VIB-B003", "vibration", "Line-B", "active", [
            (3.4, TS_R01, "mm_per_s"),
            (3.6, TS_R02, "mm_per_s"),
            (3.2, TS_R03, "mm_per_s"),
            (3.5, TS_R04, "mm_per_s"),
        ], TS_INSTALL_2024_04),
    ]
    floor_b_reading_counts = [4, 4, 4, 5, 4, 4, 4]
    total_sensors += 7
    total_readings += sum(floor_b_reading_counts)

    floor_b_msg = encode_sensor_network(floor_b_sensors, "Factory Floor B")
    with open(os.path.join(data_dir, 'factory_floor_b.pb'), 'wb') as f:
        f.write(encode_varint(len(floor_b_msg)) + floor_b_msg)

    # == File 3: Warehouse — 5 sensors ======================================
    warehouse_sensors = [
        # Temperature sensors (2)
        encode_sensor("TEMP-W001", "temperature", "Warehouse", "active", [
            (18.3, TS_R01, "celsius"),
            (18.1, TS_R02, "celsius"),
            (18.5, TS_R03, "celsius"),
            (18.2, TS_R04, "celsius"),
        ], TS_INSTALL_2023_09),
        encode_sensor("TEMP-W002", "temperature", "Warehouse", "active", [
            (19.7, TS_R01, "celsius"),
            (20.0, TS_R02, "celsius"),
            (19.5, TS_R03, "celsius"),
            (20.2, TS_R04, "celsius"),
        ], TS_INSTALL_2024_01),
        # Humidity sensors (2)
        encode_sensor("HUM-W001", "humidity", "Warehouse", "active", [
            (55.0, TS_R01, "percent"),
            (56.2, TS_R02, "percent"),
            (54.8, TS_R03, "percent"),
            (55.5, TS_R04, "percent"),
        ], TS_INSTALL_2023_09),
        encode_sensor("HUM-W002", "humidity", "Warehouse", "maintenance", [
            (72.1, TS_R01, "percent"),
            (73.5, TS_R02, "percent"),
            (71.8, TS_R03, "percent"),
            (72.9, TS_R04, "percent"),
        ], TS_INSTALL_2024_04),
        # Vibration sensor (1)
        encode_sensor("VIB-W001", "vibration", "Warehouse", "active", [
            (0.5, TS_R01, "mm_per_s"),
            (0.3, TS_R02, "mm_per_s"),
            (0.4, TS_R03, "mm_per_s"),
            (0.6, TS_R04, "mm_per_s"),
        ], TS_INSTALL_2024_01),
    ]
    warehouse_reading_counts = [4, 4, 4, 4, 4]
    total_sensors += 5
    total_readings += sum(warehouse_reading_counts)

    warehouse_msg = encode_sensor_network(warehouse_sensors, "Warehouse")
    with open(os.path.join(data_dir, 'warehouse.pb'), 'wb') as f:
        f.write(encode_varint(len(warehouse_msg)) + warehouse_msg)

    # -- Print summary ---------------------------------------------------
    print(f"Total sensors:  {total_sensors}")
    print(f"Total readings: {total_readings}")
    print()
    for fname in sorted(os.listdir(data_dir)):
        if fname.endswith('.pb'):
            fpath = os.path.join(data_dir, fname)
            size = os.path.getsize(fpath)
            print(f"  {fname}: {size} bytes")

if __name__ == '__main__':
    generate()
    print("\nDone -- protobuf data files generated.")
