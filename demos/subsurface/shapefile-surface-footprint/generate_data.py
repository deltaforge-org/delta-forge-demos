"""Write the ESRI shapefiles this demo reads.

Scenario: an onshore operator has to prove that every well pad it has built
sits inside a tract it actually leases. The land department holds the lease
tracts as polygons and the drilling department holds the pads as points, and
until both are in the same place nobody can answer the question.

Two deliveries: the pads on one day, the lease tracts the next.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

# What a shapefile actually is

Three files that have to travel together. The `.shp` holds the geometry as
big-endian record headers wrapped around little-endian coordinates, which is
the detail that catches naive readers: the file mixes byte orders inside one
record. The `.dbf` is a dBase III table holding one attribute row per shape,
in file order, with no key joining them. The `.prj` states the coordinate
system in well-known text.

Losing the `.dbf` leaves geometry with no attributes; losing the `.prj` leaves
coordinates with no meaning. This generator writes all three for both layers.
"""
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

SHP_FILE_CODE = 9994
SHAPE_POINT = 1
SHAPE_POLYGON = 5

# NAD83 / UTM zone 13N, which is what the Delaware Basin is surveyed in.
PRJ = (
    'PROJCS["NAD_1983_UTM_Zone_13N",GEOGCS["GCS_North_American_1983",'
    'DATUM["D_North_American_1983",SPHEROID["GRS_1980",6378137.0,298.257222101]],'
    'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],'
    'PROJECTION["Transverse_Mercator"],PARAMETER["False_Easting",500000.0],'
    'PARAMETER["Central_Meridian",-105.0],PARAMETER["Scale_Factor",0.9996],'
    'PARAMETER["Latitude_Of_Origin",0.0],UNIT["Meter",1.0]]'
)

# Lease tracts: (tract id, lessor, expiry year, east, north, width, height)
# The rectangle is the leased area in UTM metres.
TRACTS = [
    ('TR-4401', 'Redbluff Ranch LLC',  2031, 512000.0, 3540000.0, 3200.0, 2400.0),
    ('TR-4402', 'Redbluff Ranch LLC',  2031, 515200.0, 3540000.0, 2800.0, 2400.0),
    ('TR-4407', 'Pecos Land Trust',    2029, 512000.0, 3542400.0, 3000.0, 2000.0),
    ('TR-4412', 'State of New Mexico', 2034, 518000.0, 3542400.0, 2600.0, 2000.0),
]

# Well pads: (pad id, operator, spud year, status, east, north)
# Two of these are deliberately outside every tract, which is the finding the
# compliance query exists to produce.
PADS = [
    ('PAD-01', 'DeltaForge Onshore', 2023, 'PRODUCING', 513100.0, 3540900.0),
    ('PAD-02', 'DeltaForge Onshore', 2023, 'PRODUCING', 514400.0, 3541500.0),
    ('PAD-03', 'DeltaForge Onshore', 2024, 'DRILLING',  516800.0, 3541200.0),
    ('PAD-04', 'DeltaForge Onshore', 2024, 'PRODUCING', 513500.0, 3543100.0),
    ('PAD-05', 'DeltaForge Onshore', 2025, 'PERMITTED', 519200.0, 3543400.0),
    ('PAD-06', 'DeltaForge Onshore', 2025, 'PERMITTED', 522500.0, 3544800.0),
    ('PAD-07', 'DeltaForge Onshore', 2022, 'PLUGGED',   511200.0, 3539100.0),
    ('PAD-08', 'DeltaForge Onshore', 2024, 'PRODUCING', 514900.0, 3543000.0),
]


def dbf(fields, rows):
    """A dBase III table. `fields` is (name, type char, length, decimals)."""
    header_length = 32 + 32 * len(fields) + 1
    record_length = 1 + sum(f[2] for f in fields)

    header = bytearray(32)
    header[0] = 0x03
    header[1:4] = bytes([126, 8, 24])          # last updated 2026-08-24
    struct.pack_into('<I', header, 4, len(rows))
    struct.pack_into('<H', header, 8, header_length)
    struct.pack_into('<H', header, 10, record_length)

    out = bytearray(header)
    for name, kind, length, decimals in fields:
        descriptor = bytearray(32)
        descriptor[0:11] = name.ljust(11, '\0').encode('ascii')[:11]
        descriptor[11] = ord(kind)
        descriptor[16] = length
        descriptor[17] = decimals
        out += descriptor
    out += b'\x0D'

    for row in rows:
        out += b' '                             # not deleted
        for (name, kind, length, decimals), value in zip(fields, row):
            if kind == 'N':
                text = ('%*.*f' % (length, decimals, value)) if decimals \
                    else ('%*d' % (length, value))
                out += text[:length].rjust(length).encode('ascii')
            else:
                out += str(value)[:length].ljust(length).encode('ascii')
    out += b'\x1A'
    return bytes(out)


def shp_header(shape_type, bounds, length_words):
    """The 100-byte header. File code and length are BIG endian, everything
    from byte 24 on is LITTLE endian, which is the format's own inconsistency
    rather than ours."""
    head = bytearray(100)
    struct.pack_into('>i', head, 0, SHP_FILE_CODE)
    struct.pack_into('>i', head, 24, length_words)
    struct.pack_into('<i', head, 28, 1000)      # version
    struct.pack_into('<i', head, 32, shape_type)
    struct.pack_into('<d', head, 36, bounds[0])
    struct.pack_into('<d', head, 44, bounds[1])
    struct.pack_into('<d', head, 52, bounds[2])
    struct.pack_into('<d', head, 60, bounds[3])
    return head


def point_shapefile(points):
    body = bytearray()
    for index, (east, north) in enumerate(points):
        content = struct.pack('<i', SHAPE_POINT) + struct.pack('<dd', east, north)
        # Record header: number and content length in 16-bit words, big endian.
        body += struct.pack('>ii', index + 1, len(content) // 2) + content
    bounds = (min(p[0] for p in points), min(p[1] for p in points),
              max(p[0] for p in points), max(p[1] for p in points))
    head = shp_header(SHAPE_POINT, bounds, (100 + len(body)) // 2)
    return bytes(head) + bytes(body)


def polygon_shapefile(rings):
    body = bytearray()
    all_x, all_y = [], []
    for index, ring in enumerate(rings):
        xs = [p[0] for p in ring]
        ys = [p[1] for p in ring]
        all_x += xs
        all_y += ys
        content = bytearray()
        content += struct.pack('<i', SHAPE_POLYGON)
        content += struct.pack('<dddd', min(xs), min(ys), max(xs), max(ys))
        content += struct.pack('<ii', 1, len(ring))     # one part
        content += struct.pack('<i', 0)                 # part starts at 0
        for x, y in ring:
            content += struct.pack('<dd', x, y)
        body += struct.pack('>ii', index + 1, len(content) // 2) + bytes(content)
    bounds = (min(all_x), min(all_y), max(all_x), max(all_y))
    head = shp_header(SHAPE_POLYGON, bounds, (100 + len(body)) // 2)
    return bytes(head) + bytes(body)


def tract_ring(east, north, width, height):
    """A closed ring, clockwise, which is what a shapefile outer ring is."""
    return [
        (east, north),
        (east, north + height),
        (east + width, north + height),
        (east + width, north),
        (east, north),
    ]


def write(name, payload):
    with open(os.path.join(DATA, name), 'wb') as handle:
        handle.write(payload)
    print('  %-40s %7d bytes' % (name, len(payload)))
    return len(payload)


def main():
    os.makedirs(DATA, exist_ok=True)
    total = 0

    # ── Well pads, delivered 11 March ────────────────────────────────────
    pad_points = [(p[4], p[5]) for p in PADS]
    total += write('2026-03-11_well_pads.shp', point_shapefile(pad_points))
    total += write('2026-03-11_well_pads.dbf', dbf(
        [('PAD_ID', 'C', 10, 0), ('OPERATOR', 'C', 20, 0),
         ('SPUD_YEAR', 'N', 6, 0), ('STATUS', 'C', 10, 0),
         ('EASTING', 'N', 12, 1), ('NORTHING', 'N', 12, 1)],
        [(p[0], p[1], p[2], p[3], p[4], p[5]) for p in PADS]))
    total += write('2026-03-11_well_pads.prj', PRJ.encode('ascii'))

    # ── Lease tracts, delivered 12 March ─────────────────────────────────
    rings = [tract_ring(t[3], t[4], t[5], t[6]) for t in TRACTS]
    total += write('2026-03-12_lease_tracts.shp', polygon_shapefile(rings))
    total += write('2026-03-12_lease_tracts.dbf', dbf(
        [('TRACT_ID', 'C', 10, 0), ('LESSOR', 'C', 22, 0),
         ('EXPIRY', 'N', 6, 0),
         ('MIN_EAST', 'N', 12, 1), ('MIN_NORTH', 'N', 12, 1),
         ('MAX_EAST', 'N', 12, 1), ('MAX_NORTH', 'N', 12, 1),
         ('ACRES', 'N', 10, 1)],
        [(t[0], t[1], t[2], t[3], t[4], t[3] + t[5], t[4] + t[6],
          round(t[5] * t[6] / 4046.8564224, 1)) for t in TRACTS]))
    total += write('2026-03-12_lease_tracts.prj', PRJ.encode('ascii'))

    print()
    inside = 0
    for pad in PADS:
        for t in TRACTS:
            if (t[3] <= pad[4] <= t[3] + t[5]
                    and t[4] <= pad[5] <= t[4] + t[6]):
                inside += 1
                break
    print('  %d pads, %d tracts, %d pads inside a leased tract, %d outside'
          % (len(PADS), len(TRACTS), inside, len(PADS) - inside))
    print('  bytes on disk %d' % total)
    return 0


if __name__ == '__main__':
    sys.exit(main())
