#!/usr/bin/env python3
"""
Generate sample protobuf binary data files for the freight shipping manifest demo.
Uses raw protobuf wire encoding (no external dependencies needed).

Wire format reference:
  - Varint (type 0): int32, int64, bool, enum
  - 64-bit (type 1): double, fixed64
  - 32-bit (type 5): float, fixed32
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
    """Encode a varint field (wire type 0). Used for int32, int64, bool, enum."""
    return encode_field_key(field_number, 0) + encode_signed_varint(value)

def encode_bool_field(field_number, value):
    """Encode a bool field (wire type 0, varint 0 or 1)."""
    return encode_field_key(field_number, 0) + encode_varint(1 if value else 0)

def encode_embedded(field_number, message_bytes):
    """Encode an embedded message (length-delimited, wire type 2)."""
    return encode_field_key(field_number, 2) + encode_varint(len(message_bytes)) + message_bytes

def encode_int64_field(field_number, value):
    """Encode an int64 varint field."""
    return encode_field_key(field_number, 0) + encode_varint(value)

def encode_float_field(field_number, value):
    """Encode a float field (32-bit, wire type 5)."""
    return encode_field_key(field_number, 5) + struct.pack('<f', value)

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

def encode_dimensions(length_cm, width_cm, height_cm):
    """Encode a Dimensions message.
    Fields: length_cm(1)=float, width_cm(2)=float, height_cm(3)=float
    """
    msg = encode_float_field(1, length_cm)
    msg += encode_float_field(2, width_cm)
    msg += encode_float_field(3, height_cm)
    return msg

def encode_package(package_id, description, weight_kg, dims, package_class, requires_signature, declared_value_cents):
    """Encode a Package message.
    Fields: package_id(1)=string, description(2)=string, weight_kg(3)=float,
            dimensions(4)=Dimensions, package_class(5)=enum, requires_signature(6)=bool,
            declared_value_cents(7)=int64
    """
    msg = encode_string(1, package_id)
    msg += encode_string(2, description)
    msg += encode_float_field(3, weight_kg)
    dims_msg = encode_dimensions(*dims)
    msg += encode_embedded(4, dims_msg)
    # Always encode all fields (don't skip proto3 defaults) so reader gets non-NULL values
    msg += encode_varint_field(5, package_class)
    msg += encode_bool_field(6, requires_signature)
    msg += encode_int64_field(7, declared_value_cents)
    return msg

def encode_tracking_event(event_time_seconds, location, description):
    """Encode a TrackingEvent message.
    Fields: event_time(1)=Timestamp, location(2)=string, description(3)=string
    """
    ts_msg = encode_timestamp(event_time_seconds)
    msg = encode_embedded(1, ts_msg)
    msg += encode_string(2, location)
    msg += encode_string(3, description)
    return msg

def encode_shipment(shipment_id, origin, destination, status, is_express, is_insured,
                    total_cost_cents, packages, tracking_events, created_at_seconds):
    """Encode a Shipment message.
    Fields: shipment_id(1), origin(2), destination(3), status(4)=enum,
            is_express(5)=bool, is_insured(6)=bool, total_cost_cents(7)=int64,
            packages(8)=repeated Package, tracking(9)=repeated TrackingEvent,
            created_at(10)=Timestamp
    """
    msg = encode_string(1, shipment_id)
    msg += encode_string(2, origin)
    msg += encode_string(3, destination)
    # Always encode all fields (don't skip proto3 defaults) so reader gets non-NULL values
    msg += encode_varint_field(4, status)
    msg += encode_bool_field(5, is_express)
    msg += encode_bool_field(6, is_insured)
    msg += encode_int64_field(7, total_cost_cents)
    for pkg in packages:
        pkg_msg = encode_package(*pkg)
        msg += encode_embedded(8, pkg_msg)
    for evt in tracking_events:
        evt_msg = encode_tracking_event(*evt)
        msg += encode_embedded(9, evt_msg)
    ts_msg = encode_timestamp(created_at_seconds)
    msg += encode_embedded(10, ts_msg)
    return msg

def encode_shipping_manifest(shipments, carrier_name, manifest_date):
    """Encode a ShippingManifest message.
    Fields: shipments(1)=repeated Shipment, carrier_name(2)=string, manifest_date(3)=string
    """
    msg = b''
    for shipment_bytes in shipments:
        msg += encode_embedded(1, shipment_bytes)
    msg += encode_string(2, carrier_name)
    msg += encode_string(3, manifest_date)
    return msg

# -- Enum constants --------------------------------------------------------

# ShipmentStatus (0 = UNSPECIFIED, never used)
CREATED = 1
PICKED_UP = 2
IN_TRANSIT = 3
DELIVERED = 4
RETURNED = 5

# PackageClass (0 = UNSPECIFIED, never used)
STANDARD = 1
FRAGILE = 2
HAZMAT = 3
PERISHABLE = 4

# -- Timestamps (Unix epoch seconds) --------------------------------------

# Shipment creation dates (spread across 2025 Q1)
TS_JAN15 = 1736899200   # 2025-01-15 00:00:00 UTC
TS_JAN22 = 1737504000   # 2025-01-22 00:00:00 UTC
TS_FEB03 = 1738540800   # 2025-02-03 00:00:00 UTC
TS_FEB10 = 1739145600   # 2025-02-10 00:00:00 UTC
TS_FEB18 = 1739836800   # 2025-02-18 00:00:00 UTC
TS_MAR01 = 1740787200   # 2025-03-01 00:00:00 UTC
TS_MAR05 = 1741132800   # 2025-03-05 00:00:00 UTC
TS_MAR10 = 1741564800   # 2025-03-10 00:00:00 UTC
TS_MAR12 = 1741737600   # 2025-03-12 00:00:00 UTC
TS_MAR15 = 1741996800   # 2025-03-15 00:00:00 UTC
TS_MAR18 = 1742256000   # 2025-03-18 00:00:00 UTC
TS_MAR20 = 1742428800   # 2025-03-20 00:00:00 UTC

# Tracking event offsets (hours after creation)
H1  = 3600
H4  = 14400
H8  = 28800
H24 = 86400
H48 = 172800
H72 = 259200
H96 = 345600
H120 = 432000

# -- Data generation -------------------------------------------------------

def generate():
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    os.makedirs(data_dir, exist_ok=True)

    total_shipments = 0
    total_packages = 0
    total_tracking = 0

    # ======================================================================
    # File 1: carrier_alpha.pb — 5 shipments, 10 packages, 18 tracking events
    # ======================================================================
    alpha_shipments = [
        # SHIP-A001: NY→LA, DELIVERED, express, insured, 3 packages, 4 tracking
        encode_shipment(
            "SHIP-A001", "New York, NY", "Los Angeles, CA", DELIVERED, True, True, 125000,
            [
                ("PKG-A001-1", "Electronics", 2.5, (40.0, 30.0, 20.0), FRAGILE, True, 89900),
                ("PKG-A001-2", "Accessories", 0.8, (20.0, 15.0, 10.0), STANDARD, False, 15000),
                ("PKG-A001-3", "Cables", 0.3, (15.0, 10.0, 5.0), STANDARD, False, 5000),
            ],
            [
                (TS_JAN15,       "New York, NY",    "Order created"),
                (TS_JAN15 + H4,  "New York, NY",    "Picked up from sender"),
                (TS_JAN15 + H48, "Denver, CO",      "In transit - hub transfer"),
                (TS_JAN15 + H96, "Los Angeles, CA", "Delivered - signed by recipient"),
            ],
            TS_JAN15
        ),
        # SHIP-A002: Chicago→Miami, IN_TRANSIT, not express, insured, 2 packages, 3 tracking
        encode_shipment(
            "SHIP-A002", "Chicago, IL", "Miami, FL", IN_TRANSIT, False, True, 75000,
            [
                ("PKG-A002-1", "Laboratory Samples", 5.0, (50.0, 40.0, 30.0), HAZMAT, True, 200000),
                ("PKG-A002-2", "Lab Equipment", 3.2, (35.0, 25.0, 20.0), FRAGILE, True, 180000),
            ],
            [
                (TS_FEB03,       "Chicago, IL",    "Order created"),
                (TS_FEB03 + H4,  "Chicago, IL",    "Picked up from sender"),
                (TS_FEB03 + H48, "Nashville, TN",  "In transit - hub transfer"),
            ],
            TS_FEB03
        ),
        # SHIP-A003: Seattle→Denver, DELIVERED, express, not insured, 1 package, 4 tracking
        encode_shipment(
            "SHIP-A003", "Seattle, WA", "Denver, CO", DELIVERED, True, False, 45000,
            [
                ("PKG-A003-1", "Books", 4.0, (30.0, 25.0, 15.0), STANDARD, False, 12000),
            ],
            [
                (TS_FEB10,       "Seattle, WA",  "Order created"),
                (TS_FEB10 + H1,  "Seattle, WA",  "Picked up from sender"),
                (TS_FEB10 + H24, "Boise, ID",    "In transit - hub transfer"),
                (TS_FEB10 + H48, "Denver, CO",   "Delivered - left at door"),
            ],
            TS_FEB10
        ),
        # SHIP-A004: Boston→Atlanta, PICKED_UP, not express, not insured, 2 packages, 2 tracking
        encode_shipment(
            "SHIP-A004", "Boston, MA", "Atlanta, GA", PICKED_UP, False, False, 32000,
            [
                ("PKG-A004-1", "Frozen Goods", 8.0, (45.0, 35.0, 30.0), PERISHABLE, True, 50000),
                ("PKG-A004-2", "Ice Packs", 2.0, (30.0, 20.0, 15.0), PERISHABLE, False, 2000),
            ],
            [
                (TS_MAR18,      "Boston, MA", "Order created"),
                (TS_MAR18 + H4, "Boston, MA", "Picked up from sender"),
            ],
            TS_MAR18
        ),
        # SHIP-A005: Houston→Phoenix, RETURNED, not express, insured, 2 packages, 5 tracking
        encode_shipment(
            "SHIP-A005", "Houston, TX", "Phoenix, AZ", RETURNED, False, True, 58000,
            [
                ("PKG-A005-1", "Furniture Part A", 15.0, (80.0, 60.0, 40.0), FRAGILE, True, 150000),
                ("PKG-A005-2", "Furniture Part B", 12.0, (70.0, 50.0, 35.0), STANDARD, False, 85000),
            ],
            [
                (TS_JAN22,        "Houston, TX",  "Order created"),
                (TS_JAN22 + H4,   "Houston, TX",  "Picked up from sender"),
                (TS_JAN22 + H48,  "El Paso, TX",  "In transit - hub transfer"),
                (TS_JAN22 + H96,  "Phoenix, AZ",  "Delivery attempted - recipient unavailable"),
                (TS_JAN22 + H120, "Houston, TX",  "Returned to sender"),
            ],
            TS_JAN22
        ),
    ]
    total_shipments += 5
    total_packages += 10  # 3+2+1+2+2
    total_tracking += 18  # 4+3+4+2+5

    alpha_msg = encode_shipping_manifest(alpha_shipments, "Alpha Express Logistics", "2025-03-25")
    with open(os.path.join(data_dir, 'carrier_alpha.pb'), 'wb') as f:
        f.write(encode_varint(len(alpha_msg)) + alpha_msg)

    # ======================================================================
    # File 2: carrier_beta.pb — 4 shipments, 8 packages, 12 tracking events
    # ======================================================================
    beta_shipments = [
        # SHIP-B001: SF→Portland, DELIVERED, express, insured, 1 package, 4 tracking
        encode_shipment(
            "SHIP-B001", "San Francisco, CA", "Portland, OR", DELIVERED, True, True, 28000,
            [
                ("PKG-B001-1", "Wine Collection", 6.5, (40.0, 30.0, 35.0), FRAGILE, True, 300000),
            ],
            [
                (TS_MAR01,       "San Francisco, CA", "Order created"),
                (TS_MAR01 + H1,  "San Francisco, CA", "Picked up from sender"),
                (TS_MAR01 + H24, "Medford, OR",       "In transit - hub transfer"),
                (TS_MAR01 + H48, "Portland, OR",      "Delivered - signed by recipient"),
            ],
            TS_MAR01
        ),
        # SHIP-B002: Dallas→Nashville, IN_TRANSIT, not express, not insured, 3 packages, 3 tracking
        encode_shipment(
            "SHIP-B002", "Dallas, TX", "Nashville, TN", IN_TRANSIT, False, False, 42000,
            [
                ("PKG-B002-1", "Clothing", 1.5, (40.0, 30.0, 10.0), STANDARD, False, 25000),
                ("PKG-B002-2", "Shoes", 1.2, (35.0, 25.0, 15.0), STANDARD, False, 18000),
                ("PKG-B002-3", "Accessories", 0.4, (20.0, 15.0, 8.0), STANDARD, False, 8000),
            ],
            [
                (TS_MAR10,       "Dallas, TX",       "Order created"),
                (TS_MAR10 + H4,  "Dallas, TX",       "Picked up from sender"),
                (TS_MAR10 + H48, "Little Rock, AR",  "In transit - hub transfer"),
            ],
            TS_MAR10
        ),
        # SHIP-B003: Philly→DC, CREATED, not express, not insured, 2 packages, 1 tracking
        encode_shipment(
            "SHIP-B003", "Philadelphia, PA", "Washington, DC", CREATED, False, False, 15000,
            [
                ("PKG-B003-1", "Documents", 0.5, (35.0, 25.0, 5.0), STANDARD, True, 0),
                ("PKG-B003-2", "Archive Box", 3.0, (40.0, 30.0, 25.0), STANDARD, False, 0),
            ],
            [
                (TS_MAR20, "Philadelphia, PA", "Order created"),
            ],
            TS_MAR20
        ),
        # SHIP-B004: Minneapolis→Detroit, DELIVERED, express, insured, 2 packages, 4 tracking
        encode_shipment(
            "SHIP-B004", "Minneapolis, MN", "Detroit, MI", DELIVERED, True, True, 95000,
            [
                ("PKG-B004-1", "Medical Supplies", 4.5, (50.0, 35.0, 25.0), PERISHABLE, True, 450000),
                ("PKG-B004-2", "Medical Instruments", 2.0, (30.0, 20.0, 15.0), FRAGILE, True, 250000),
            ],
            [
                (TS_FEB18,       "Minneapolis, MN", "Order created"),
                (TS_FEB18 + H1,  "Minneapolis, MN", "Picked up from sender"),
                (TS_FEB18 + H24, "Milwaukee, WI",   "In transit - hub transfer"),
                (TS_FEB18 + H48, "Detroit, MI",     "Delivered - signed by recipient"),
            ],
            TS_FEB18
        ),
    ]
    total_shipments += 4
    total_packages += 8  # 1+3+2+2
    total_tracking += 12  # 4+3+1+4

    beta_msg = encode_shipping_manifest(beta_shipments, "Beta Freight Services", "2025-03-25")
    with open(os.path.join(data_dir, 'carrier_beta.pb'), 'wb') as f:
        f.write(encode_varint(len(beta_msg)) + beta_msg)

    # ======================================================================
    # File 3: carrier_gamma.pb — 3 shipments, 6 packages, 9 tracking events
    # ======================================================================
    gamma_shipments = [
        # SHIP-C001: Vegas→SLC, DELIVERED, not express, insured, 2 packages, 4 tracking
        encode_shipment(
            "SHIP-C001", "Las Vegas, NV", "Salt Lake City, UT", DELIVERED, False, True, 38000,
            [
                ("PKG-C001-1", "Industrial Chemicals", 10.0, (50.0, 40.0, 35.0), HAZMAT, True, 120000),
                ("PKG-C001-2", "Safety Equipment", 3.5, (40.0, 30.0, 20.0), STANDARD, False, 35000),
            ],
            [
                (TS_MAR05,       "Las Vegas, NV",      "Order created"),
                (TS_MAR05 + H4,  "Las Vegas, NV",      "Picked up from sender"),
                (TS_MAR05 + H24, "St. George, UT",     "In transit - hub transfer"),
                (TS_MAR05 + H72, "Salt Lake City, UT", "Delivered - signed by recipient"),
            ],
            TS_MAR05
        ),
        # SHIP-C002: Orlando→Charlotte, IN_TRANSIT, express, not insured, 1 package, 3 tracking
        encode_shipment(
            "SHIP-C002", "Orlando, FL", "Charlotte, NC", IN_TRANSIT, True, False, 22000,
            [
                ("PKG-C002-1", "Fresh Produce", 7.0, (45.0, 35.0, 25.0), PERISHABLE, False, 8000),
            ],
            [
                (TS_MAR15,       "Orlando, FL",      "Order created"),
                (TS_MAR15 + H4,  "Orlando, FL",      "Picked up from sender"),
                (TS_MAR15 + H24, "Jacksonville, FL", "In transit - hub transfer"),
            ],
            TS_MAR15
        ),
        # SHIP-C003: Tampa→Raleigh, PICKED_UP, not express, insured, 3 packages, 2 tracking
        encode_shipment(
            "SHIP-C003", "Tampa, FL", "Raleigh, NC", PICKED_UP, False, True, 67000,
            [
                ("PKG-C003-1", "Art Piece", 8.0, (100.0, 70.0, 10.0), FRAGILE, True, 500000),
                ("PKG-C003-2", "Art Frame", 5.0, (90.0, 60.0, 10.0), FRAGILE, True, 75000),
                ("PKG-C003-3", "Packing Materials", 2.0, (60.0, 40.0, 30.0), STANDARD, False, 500),
            ],
            [
                (TS_MAR12,      "Tampa, FL", "Order created"),
                (TS_MAR12 + H4, "Tampa, FL", "Picked up from sender"),
            ],
            TS_MAR12
        ),
    ]
    total_shipments += 3
    total_packages += 6  # 2+1+3
    total_tracking += 9  # 4+3+2

    gamma_msg = encode_shipping_manifest(gamma_shipments, "Gamma Global Shipping", "2025-03-25")
    with open(os.path.join(data_dir, 'carrier_gamma.pb'), 'wb') as f:
        f.write(encode_varint(len(gamma_msg)) + gamma_msg)

    # -- Print summary ---------------------------------------------------
    print(f"Total shipments: {total_shipments}")
    print(f"Total packages:  {total_packages}")
    print(f"Total tracking:  {total_tracking}")
    print()
    for fname in sorted(os.listdir(data_dir)):
        if fname.endswith('.pb'):
            fpath = os.path.join(data_dir, fname)
            size = os.path.getsize(fpath)
            print(f"  {fname}: {size} bytes")

if __name__ == '__main__':
    generate()
    print("\nDone -- protobuf data files generated.")
