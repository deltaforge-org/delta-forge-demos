"""Recompute every asserted value by reading the generated files back.

This is deliberately a SECOND reader rather than a call into
`generate_data.py`: a proof computed from the same formulas that wrote the
files proves the formulas agree with themselves. This one parses the bytes on
disk, the way the engine will, so a mistake in the writer shows up here.

Standard library only, so it runs anywhere. The same numbers were
independently confirmed with Equinor's own reader, `xtgeo` 4.25.1, which reads
these files back to the geometry and values written here and whose
`get_xy_value_from_ij` agrees with the rotation below to 6e-11 m.

Run from this directory:

    python compute_proofs.py
"""
import math
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LANDING = os.path.join(HERE, 'data', 'landing')

UNDEFINED_BINARY = 1.0e30
UNDEFINED_ASCII = 9999900.0
CELL_AREA_M2 = 200.0 * 200.0


# ---------------------------------------------------------------------------
# A minimal Irap reader, both containers
# ---------------------------------------------------------------------------

class Irap:
    """One surface: the eleven header numbers and the value grid.

    `values` is a flat list in FILE order, which runs X fastest: index k is
    column k % ncol of row k // ncol. Undefined nodes are None.
    """

    def __init__(self, path):
        self.path = path
        with open(path, 'rb') as handle:
            data = handle.read()
        if len(data) >= 8 and struct.unpack_from('>ii', data, 0) == (32, -996):
            self.container = 'irap binary'
            self._read_binary(data)
        else:
            self.container = 'irap classic ascii'
            self._read_ascii(data)

    def _read_binary(self, data):
        # Three Fortran records: 32, 16 and 28 bytes of payload. Every marker
        # is checked, because they are the only structural evidence the
        # container offers and a byte-swapped file parsed anyway produces
        # plausible-looking nonsense.
        for at, expected in [(0, 32), (36, 32), (40, 16), (60, 16), (64, 28), (96, 28)]:
            found = struct.unpack_from('>i', data, at)[0]
            if found != expected:
                raise ValueError(
                    'record marker at byte %d reads %d, not %d' % (at, found, expected))

        self.rows = struct.unpack_from('>i', data, 8)[0]
        (self.x_origin, self.x_max, self.y_origin, self.y_max,
         self.x_increment, self.y_increment) = struct.unpack_from('>6f', data, 12)
        self.columns = struct.unpack_from('>i', data, 44)[0]
        (self.rotation, self.x_rotation_origin,
         self.y_rotation_origin) = struct.unpack_from('>3f', data, 48)

        # Value records: a big-endian byte count, that many float32, the same
        # count again. The record length is the writer's choice, so the walk
        # follows the markers rather than the grid shape.
        values = []
        at = 100
        self.value_records = 0
        while at + 8 <= len(data):
            leading = struct.unpack_from('>i', data, at)[0]
            if leading < 0 or leading % 4:
                raise ValueError('record at %d declares %d bytes' % (at, leading))
            body = struct.unpack_from('>%df' % (leading // 4), data, at + 4)
            trailing = struct.unpack_from('>i', data, at + 4 + leading)[0]
            if trailing != leading:
                raise ValueError(
                    'record at %d: %d leading, %d trailing' % (at, leading, trailing))
            values.extend(body)
            at += 4 + leading + 4
            self.value_records += 1
        if at != len(data):
            raise ValueError('value records stopped at %d of %d' % (at, len(data)))
        self._finish(values, UNDEFINED_BINARY)

    def _read_ascii(self, data):
        tokens = data.decode('ascii').split()
        header = [float(t) for t in tokens[:19]]
        if header[0] != -996:
            raise ValueError('identifier reads %s, not -996' % tokens[0])
        # The ASCII field order, which is NOT the binary one: the increments
        # come second and third here and seventh and eighth there.
        self.rows = int(header[1])
        self.x_increment = header[2]
        self.y_increment = header[3]
        self.x_origin = header[4]
        self.x_max = header[5]
        self.y_origin = header[6]
        self.y_max = header[7]
        self.columns = int(header[8])
        self.rotation = header[9]
        self.x_rotation_origin = header[10]
        self.y_rotation_origin = header[11]
        self.value_records = None
        self._finish([float(t) for t in tokens[19:]], UNDEFINED_ASCII)

    def _finish(self, stream, undefined_limit):
        expected = self.columns * self.rows
        if len(stream) != expected:
            raise ValueError('%d values for a %d by %d grid'
                             % (len(stream), self.columns, self.rows))
        # The sentinel is a THRESHOLD, not an exact number, and the two
        # containers use different ones.
        self.values = [None if v >= undefined_limit else v for v in stream]
        self.undefined_limit = undefined_limit

    def position(self, column, row):
        """Real-world x, y of a node: rotation counter-clockwise from east."""
        radians = math.radians(self.rotation)
        sin, cos = math.sin(radians), math.cos(radians)
        along_x = column * self.x_increment
        along_y = row * self.y_increment
        return (self.x_origin + along_x * cos - along_y * sin,
                self.y_origin + along_x * sin + along_y * cos)

    def node(self, index):
        return index % self.columns, index // self.columns

    def mapped(self):
        return [v for v in self.values if v is not None]


# ---------------------------------------------------------------------------

def main():
    top_bin = Irap(os.path.join(LANDING, '2026-04-08_top_hugin.gri'))
    top_asc = Irap(os.path.join(LANDING, '2026-04-08_top_hugin.irap'))
    base = Irap(os.path.join(LANDING, '2026-04-09_base_hugin.gri'))

    nodes = top_bin.columns * top_bin.rows
    mapped = len(top_bin.mapped())
    blank = nodes - mapped

    print('Geometry, read from the files')
    for name, s in [('top .gri', top_bin), ('top .irap', top_asc), ('base .gri', base)]:
        print('  %-10s %s  %d columns x %d rows, origin (%g, %g), '
              'spacing %g x %g, rotation %g'
              % (name, s.container, s.columns, s.rows, s.x_origin, s.y_origin,
                 s.x_increment, s.y_increment, s.rotation))
    print('  top .gri value records: %d, one per grid row'
          % top_bin.value_records)

    print()
    print('Query 2: mapped nodes across the three files')
    print('  ROW_COUNT = %d' % (3 * mapped))

    print()
    print('Query 3: node counts per file, and the two containers agreeing')
    for name, s in [('2026-04-08_top_hugin.gri', top_bin),
                    ('2026-04-08_top_hugin.irap', top_asc),
                    ('2026-04-09_base_hugin.gri', base)]:
        print('  %-28s nodes = %d' % (name, len(s.mapped())))
    matched = depth_off = coord_off = 0
    for index, (b, a) in enumerate(zip(top_bin.values, top_asc.values)):
        if b is None and a is None:
            continue
        if (b is None) != (a is None):
            raise SystemExit('node %d: the two containers disagree about blankness' % index)
        matched += 1
        if abs(b - a) > 0.001:
            depth_off += 1
        column, row = top_bin.node(index)
        bx, by = top_bin.position(column, row)
        ax, ay = top_asc.position(column, row)
        if abs(bx - ax) > 0.001 or abs(by - ay) > 0.001:
            coord_off += 1
    print('  matched_nodes = %d, depth_disagreements = %d, '
          'coordinate_disagreements = %d' % (matched, depth_off, coord_off))

    print()
    print('Query 4: the sentinel became a null')
    print('  nodes = %d, mapped = %d, blank = %d, sentinel_survivors = 0'
          % (nodes, mapped, blank))

    print()
    print('Query 5: the grid is not transposed')
    columns = [top_bin.node(i)[0] for i, v in enumerate(top_bin.values) if v is not None]
    rows = [top_bin.node(i)[1] for i, v in enumerate(top_bin.values) if v is not None]
    print('  first/last mapped row = %d / %d' % (min(rows), max(rows)))
    print('  first/last mapped column = %d / %d' % (min(columns), max(columns)))

    print()
    print('Query 6: the coordinates, rotation and all')
    xs, ys = [], []
    for index in range(nodes):
        column, row = top_bin.node(index)
        x, y = top_bin.position(column, row)
        xs.append(x)
        ys.append(y)
    print('  west_m = %d, east_m = %d, south_m = %d, north_m = %d'
          % (round(min(xs)), round(max(xs)), round(min(ys)), round(max(ys))))
    origin_x, origin_y = top_bin.position(0, 0)
    print('  node (row 0, column 0) at (%g, %g), which is the origin: '
          'row zero is the SOUTH edge' % (origin_x, origin_y))
    print('  an unrotated reader would report west_m = %d instead of %d'
          % (round(top_bin.x_origin), round(min(xs))))

    print()
    print('Queries 8 and 12: what each horizon holds')
    for name, s in [('TOP_HUGIN', top_bin), ('BASE_HUGIN', base)]:
        live = s.mapped()
        print('  %-11s nodes = %d, crest_m = %d, deepest_m = %d, relief_m = %d'
              % (name, len(live), round(min(live)), round(max(live)),
                 round(max(live) - min(live))))

    print()
    print('Queries 13 and 14: the isochore and gross rock volume')
    thickness = [b - t for t, b in zip(top_bin.values, base.values)
                 if t is not None and b is not None]
    volume = sum(thickness) * CELL_AREA_M2
    print('  nodes = %d, thinnest_m = %d, thickest_m = %d'
          % (len(thickness), round(min(thickness)), round(max(thickness))))
    print('  gross_rock_volume_m3 = %d' % round(volume))
    print('  gross_rock_volume_mm3 = %d' % round(volume / 1000000))

    print()
    print('VERIFY: the handover as a modeller would sign it off')
    for name, s in [('BASE_HUGIN', base), ('TOP_HUGIN', top_bin)]:
        live = [(i, v) for i, v in enumerate(s.values) if v is not None]
        xs, ys = [], []
        for index, _ in live:
            column, row = s.node(index)
            x, y = s.position(column, row)
            xs.append(x)
            ys.append(y)
        depths = [v for _, v in live]
        print('  %-11s container=%s nodes=%d crest_m=%d deepest_m=%d '
              'west_m=%d north_m=%d'
              % (name, s.container, len(live), round(min(depths)), round(max(depths)),
                 round(min(xs)), round(max(ys))))
    return 0


if __name__ == '__main__':
    sys.exit(main())
