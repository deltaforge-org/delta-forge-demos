#!/usr/bin/env python3
"""
Generate sample protobuf binary data files for the addressbook demo.
Uses raw protobuf wire encoding (no external dependencies needed).

Wire format reference:
  - Varint (type 0): int32, int64, bool, enum
  - Length-delimited (type 2): string, bytes, embedded messages
  - Field key = (field_number << 3) | wire_type
"""
import struct
import os
import time

# ── Protobuf wire encoding primitives ────────────────────────────────

def encode_varint(value):
    """Encode an unsigned integer as a protobuf varint."""
    if value < 0:
        # Signed integers use two's complement for 64-bit
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

# ── Message encoders ─────────────────────────────────────────────────

# PhoneType enum values
MOBILE = 0
HOME = 1
WORK = 2

def encode_timestamp(seconds, nanos=0):
    """Encode a google.protobuf.Timestamp message."""
    msg = b''
    if seconds != 0:
        msg += encode_int64_field(1, seconds)
    if nanos != 0:
        msg += encode_varint_field(2, nanos)
    return msg

def encode_phone_number(number, phone_type):
    """Encode a Person.PhoneNumber message."""
    msg = encode_string(1, number)
    if phone_type != 0:  # MOBILE is default (0), only encode non-default
        msg += encode_varint_field(2, phone_type)
    return msg

def encode_person(name, person_id, email, phones, last_updated_seconds=0):
    """Encode a Person message."""
    msg = encode_string(1, name)
    msg += encode_varint_field(2, person_id)
    if email:
        msg += encode_string(3, email)
    for number, phone_type in phones:
        phone_msg = encode_phone_number(number, phone_type)
        msg += encode_embedded(4, phone_msg)
    if last_updated_seconds > 0:
        ts_msg = encode_timestamp(last_updated_seconds)
        msg += encode_embedded(5, ts_msg)
    return msg

def encode_address_book(people):
    """Encode an AddressBook message containing multiple Person entries."""
    msg = b''
    for person_bytes in people:
        msg += encode_embedded(1, person_bytes)
    return msg


# ── Data generation ──────────────────────────────────────────────────

# Timestamps (Unix epoch seconds)
# 2024-01-15 09:30:00 UTC
TS_2024_01_15 = 1705308600
# 2024-03-22 14:15:00 UTC
TS_2024_03_22 = 1711116900
# 2024-06-10 11:00:00 UTC
TS_2024_06_10 = 1718013600
# 2024-08-05 16:45:00 UTC
TS_2024_08_05 = 1722876300
# 2024-09-18 08:20:00 UTC
TS_2024_09_18 = 1726647600
# 2024-11-01 13:00:00 UTC
TS_2024_11_01 = 1730466000
# 2024-12-20 10:30:00 UTC
TS_2024_12_20 = 1734691800
# 2025-01-08 15:45:00 UTC
TS_2025_01_08 = 1736351100
# 2025-02-14 09:00:00 UTC
TS_2025_02_14 = 1739523600
# 2025-03-01 12:00:00 UTC
TS_2025_03_01 = 1740826800

def generate():
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    os.makedirs(data_dir, exist_ok=True)

    # ── File 1: Engineering team contacts (5 people) ─────────────────
    engineering = [
        encode_person(
            "Alice Chen", 1001, "alice.chen@example.com",
            [("+1-555-0101", MOBILE), ("+1-555-0102", WORK)],
            TS_2024_01_15
        ),
        encode_person(
            "Bob Martinez", 1002, "bob.martinez@example.com",
            [("+1-555-0201", MOBILE), ("+1-555-0202", HOME), ("+1-555-0203", WORK)],
            TS_2024_03_22
        ),
        encode_person(
            "Carol Nakamura", 1003, "carol.nakamura@example.com",
            [("+81-90-1234-5678", MOBILE)],
            TS_2024_06_10
        ),
        encode_person(
            "David Okonkwo", 1004, "david.okonkwo@example.com",
            [("+44-7700-900100", MOBILE), ("+44-20-7946-0958", WORK)],
            TS_2024_08_05
        ),
        encode_person(
            "Elena Volkov", 1005, "elena.volkov@example.com",
            [("+7-495-123-4567", WORK)],
            TS_2024_09_18
        ),
    ]
    with open(os.path.join(data_dir, '01_engineering_team.pb'), 'wb') as f:
        f.write(encode_address_book(engineering))

    # ── File 2: Sales team contacts (5 people) ───────────────────────
    sales = [
        encode_person(
            "Frank Johnson", 2001, "frank.johnson@example.com",
            [("+1-555-0301", MOBILE), ("+1-555-0302", WORK)],
            TS_2024_01_15
        ),
        encode_person(
            "Grace Kim", 2002, "grace.kim@example.com",
            [("+82-10-9876-5432", MOBILE)],
            TS_2024_03_22
        ),
        encode_person(
            "Hassan Ali", 2003, "hassan.ali@example.com",
            [("+971-50-123-4567", MOBILE), ("+971-4-567-8900", WORK)],
            TS_2024_06_10
        ),
        encode_person(
            "Ingrid Svensson", 2004, "ingrid.svensson@example.com",
            [("+46-70-123-4567", MOBILE), ("+46-8-123-4567", HOME), ("+46-8-987-6543", WORK)],
            TS_2024_11_01
        ),
        encode_person(
            "James Wilson", 2005, "james.wilson@example.com",
            [("+1-555-0401", MOBILE)],
            TS_2024_12_20
        ),
    ]
    with open(os.path.join(data_dir, '02_sales_team.pb'), 'wb') as f:
        f.write(encode_address_book(sales))

    # ── File 3: Executive contacts (3 people, some with no phone) ────
    # This tests sparse data — not everyone has all fields populated
    executives = [
        encode_person(
            "Katherine Park", 3001, "katherine.park@example.com",
            [("+1-555-0501", MOBILE), ("+1-555-0502", HOME), ("+1-555-0503", WORK)],
            TS_2025_01_08
        ),
        encode_person(
            "Luis Hernandez", 3002, "luis.hernandez@example.com",
            [],  # No phone numbers — tests empty repeated field
            TS_2025_02_14
        ),
        encode_person(
            "Maria Schmidt", 3003, "",  # No email — tests empty/missing string
            [("+49-151-1234-5678", MOBILE)],
            TS_2025_03_01
        ),
    ]
    with open(os.path.join(data_dir, '03_executives.pb'), 'wb') as f:
        f.write(encode_address_book(executives))

    # Print file sizes for demo.toml
    for fname in sorted(os.listdir(data_dir)):
        if fname.endswith('.pb'):
            fpath = os.path.join(data_dir, fname)
            size = os.path.getsize(fpath)
            print(f"  {fname}: {size} bytes")

if __name__ == '__main__':
    generate()
    print("Done — protobuf data files generated.")
