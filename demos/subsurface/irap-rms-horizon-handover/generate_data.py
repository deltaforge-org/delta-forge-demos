"""Write the Irap surfaces this demo reads.

Scenario: a geomodeller finishes a structural model in RMS and hands the
horizons to the subsurface team. RMS writes surfaces as Irap, and a handover
routinely carries the same surface twice: the binary `.gri` the project stores
and a classic ASCII export made for a tool that only reads text. The team
loads both, checks they agree, and computes gross rock volume from the top and
base pair.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

# The format, and the four things that go wrong in it

An Irap surface is eleven header numbers (the origin, the node spacing, the
counts and a rotation) followed by the grid values. Both containers hold the
same eleven numbers and the same values, and everything that can go wrong goes
wrong silently:

  - The value block runs X FASTEST, one whole row of constant Y at a time.
    This is the reverse of ZMAP+. Read column-major, an Irap grid transposes:
    the node count matches, every value is present, every value is in the
    wrong place.
  - The ASCII header is NOT in the binary header's field order. ASCII writes
    nrow, xinc, yinc, then the origins; binary writes nrow, then the origins
    and extents, then xinc and yinc. A reader that shares one field table
    between the two lands the whole map at the coordinate (200, 200).
  - The undefined sentinel is a THRESHOLD and it differs per container: at or
    above 1e30 in the binary form, at or above 9999900.0 in the ASCII one.
    Compared against the wrong one, a blank node reads as a real depth of
    nearly ten million metres.
  - Row zero is the SOUTH edge. ZMAP+ counts from the north, so the same map
    in the two formats agrees on x and y and disagrees on row.

The grid is 60 columns by 45 rows rather than square, and rotated 24 degrees,
so none of those mistakes can succeed quietly: a transposed read runs off the
end, and an ignored rotation puts every node on a different coordinate.

Cross-checked against equinor/xtgeo 4.25.1, which reads these files back to
the geometry and values written here.
"""
import math
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

COLUMNS = 60          # ncol, along the grid X axis
ROWS = 45             # nrow, along the grid Y axis
X_ORIGIN = 458000.0
Y_ORIGIN = 6785000.0
X_INCREMENT = 200.0
Y_INCREMENT = 200.0
ROTATION = 24.0       # counter-clockwise from east, in degrees

UNDEFINED_BINARY = 1.0e30
UNDEFINED_ASCII = 9999900.0

IDENTIFIER = -996
LF = chr(10)


def surface(datum, relief):
    """The surface as a flat list, X fastest, blank outside the mapped ellipse.

    Values are quantised to a quarter of a metre so the float32 the binary
    container stores and the decimal the ASCII container writes are the same
    number. That is what lets the demo assert the two containers agree
    exactly rather than approximately.
    """
    values = []
    mapped = 0
    for row in range(ROWS):
        for column in range(COLUMNS):
            u = (column / (COLUMNS - 1) - 0.47) / 0.42
            v = (row / (ROWS - 1) - 0.51) / 0.44
            radius = math.hypot(u, v)
            if radius > 1.0:
                values.append(None)
            else:
                depth = datum - relief * math.cos(radius * math.pi / 2)
                values.append(round(depth * 4.0) / 4.0)
                mapped += 1
    return values, mapped


def irap_binary(values):
    """Irap binary: three Fortran header records, then one record per row.

    One record per grid row is what xtgeo's own writer emits, and writing it
    that way keeps the fixture a file a real tool would produce rather than
    our own dialect of the container.
    """
    body = bytearray()
    body += struct.pack('>i', 32)
    body += struct.pack('>ii', IDENTIFIER, ROWS)
    body += struct.pack(
        '>ffff',
        X_ORIGIN,
        X_ORIGIN + (COLUMNS - 1) * X_INCREMENT,
        Y_ORIGIN,
        Y_ORIGIN + (ROWS - 1) * Y_INCREMENT,
    )
    body += struct.pack('>ff', X_INCREMENT, Y_INCREMENT)
    body += struct.pack('>i', 32)

    body += struct.pack('>i', 16)
    body += struct.pack('>i', COLUMNS)
    body += struct.pack('>fff', ROTATION, X_ORIGIN, Y_ORIGIN)
    body += struct.pack('>i', 16)

    body += struct.pack('>i', 28)
    body += struct.pack('>7i', 0, 0, 0, 0, 0, 0, 0)
    body += struct.pack('>i', 28)

    stream = [UNDEFINED_BINARY if v is None else v for v in values]
    for start in range(0, len(stream), COLUMNS):
        chunk = stream[start:start + COLUMNS]
        payload = struct.pack('>%df' % len(chunk), *chunk)
        body += struct.pack('>i', len(payload)) + payload + struct.pack('>i', len(payload))
    return bytes(body)


def irap_ascii(values):
    """Irap classic ASCII: nineteen header numbers, then six values a line.

    The header field order here is the ASCII one, which is deliberately not
    the binary one. That difference is the whole reason this demo ships the
    same surface in both containers.
    """
    lines = [
        '%d %d %.4f %.4f' % (IDENTIFIER, ROWS, X_INCREMENT, Y_INCREMENT),
        '%.4f %.4f %.4f %.4f' % (
            X_ORIGIN,
            X_ORIGIN + (COLUMNS - 1) * X_INCREMENT,
            Y_ORIGIN,
            Y_ORIGIN + (ROWS - 1) * Y_INCREMENT,
        ),
        '%d %.4f %.4f %.4f' % (COLUMNS, ROTATION, X_ORIGIN, Y_ORIGIN),
        '0 0 0 0 0 0 0',
    ]
    stream = [UNDEFINED_ASCII if v is None else v for v in values]
    for start in range(0, len(stream), 6):
        lines.append(' '.join('%.4f' % v for v in stream[start:start + 6]))
    return (LF.join(lines) + LF).encode('ascii')


def write(name, payload):
    path = os.path.join(DATA, name)
    with open(path, 'wb') as handle:
        handle.write(payload)
    print('  %-42s %8d bytes' % (name, len(payload)))


def main():
    os.makedirs(DATA, exist_ok=True)
    top, top_mapped = surface(2380.0, 165.0)
    base, base_mapped = surface(2560.0, 118.0)

    print('Irap surfaces for irap-rms-horizon-handover')
    write('2026-04-08_top_hugin.gri', irap_binary(top))
    write('2026-04-08_top_hugin.irap', irap_ascii(top))
    write('2026-04-09_base_hugin.gri', irap_binary(base))

    nodes = COLUMNS * ROWS
    print()
    print('  grid              %d columns by %d rows = %d nodes' % (COLUMNS, ROWS, nodes))
    print('  rotation          %.1f degrees counter-clockwise from east' % ROTATION)
    print('  mapped nodes      top %d, base %d' % (top_mapped, base_mapped))
    print('  blank nodes       top %d, base %d' % (nodes - top_mapped, nodes - base_mapped))
    return 0


if __name__ == '__main__':
    sys.exit(main())
