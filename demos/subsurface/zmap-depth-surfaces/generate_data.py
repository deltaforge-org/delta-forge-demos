"""Write the ZMAP+ depth grids this demo reads.

Scenario: a geophysicist depth-converts a seismic interpretation and hands the
result to the mapping team as ZMAP+ grids. The team loads them to compute
gross rock volume, which is the number a prospect is sized on, and to compare
one depth-conversion iteration against the next.

Three grids over two days: the first pass at the top reservoir, then a revised
top after the velocity model was corrected, and the base reservoir surface
that the pair of them needs to become a volume.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

# The format, and the two things that go wrong in it

A ZMAP+ file is a small `@`-delimited header block followed by a stream of
numbers. Two details decide whether a reader gets it right:

  - Values run COLUMN BY COLUMN, not row by row. A reader that assumes
    row-major produces a grid of exactly the right size where every node
    holds a neighbour's value, which no row count will catch.
  - Nodes outside the mapped polygon carry a null sentinel, here 1e30. Read
    as a number it is not merely wrong, it is 1e30, and one of them in an
    average destroys the answer.

The grid is deliberately 45 rows by 60 columns so those two are not
interchangeable: a transposed read would run off the end rather than quietly
succeeding.
"""
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

ROWS = 45
COLUMNS = 60

# Chosen so the node spacing is exactly 200 m in both directions:
# (471800 - 460000) / 59 = 200, (6548800 - 6540000) / 44 = 200.
X_MIN, X_MAX = 460000.0, 471800.0
Y_MIN, Y_MAX = 6540000.0, 6548800.0
NODE_SPACING = 200.0
CELL_AREA_M2 = NODE_SPACING * NODE_SPACING

NULL_VALUE = 1.0e30

# The mapped polygon is an ellipse centred on the structure. Nodes outside it
# were never interpreted and carry the null sentinel.
CENTRE_COLUMN = 0.46
CENTRE_ROW = 0.50
RADIUS_COLUMN = 0.40
RADIUS_ROW = 0.44


def inside(row, column):
    u = (column / (COLUMNS - 1) - CENTRE_COLUMN) / RADIUS_COLUMN
    v = (row / (ROWS - 1) - CENTRE_ROW) / RADIUS_ROW
    return math.hypot(u, v) <= 1.0


def top_depth(row, column, velocity_correction):
    """A dome. Depth is shallowest at the crest and falls away from it.

    `velocity_correction` is the change between depth-conversion iterations:
    the first pass used a velocity that was slightly too fast, so the second
    puts the same structure deeper.
    """
    u = (column / (COLUMNS - 1) - CENTRE_COLUMN) / RADIUS_COLUMN
    v = (row / (ROWS - 1) - CENTRE_ROW) / RADIUS_ROW
    relief = 1.0 - min(1.0, math.hypot(u, v) ** 2)
    return 2585.0 - 145.0 * relief + velocity_correction


def gross_thickness(row, column):
    """Reservoir thickness, thickest at the crest and thinning to the flanks."""
    u = (column / (COLUMNS - 1) - CENTRE_COLUMN) / RADIUS_COLUMN
    v = (row / (ROWS - 1) - CENTRE_ROW) / RADIUS_ROW
    relief = 1.0 - min(1.0, math.hypot(u, v) ** 2)
    return 12.0 + 46.0 * relief


def grid(name, value_at):
    """A ZMAP+ grid. Values are written column by column, five to a line."""
    lines = []
    lines.append('!')
    lines.append('! %s' % name)
    lines.append('! Written by generate_data.py for the DeltaForge ZMAP+ demo')
    lines.append('!')
    lines.append('@%s, GRID, 5' % name)
    # Directive line one: field width, null value, null text, decimals, start.
    lines.append('   20, %g, , 7, 1' % NULL_VALUE)
    # Directive line two: rows, columns, then the grid's own extent.
    lines.append('   %d, %d, %.1f, %.1f, %.1f, %.1f'
                 % (ROWS, COLUMNS, X_MIN, X_MAX, Y_MIN, Y_MAX))
    lines.append('   0.0, 0.0, 0.0')
    lines.append('@')

    values = []
    for column in range(COLUMNS):
        for row in range(ROWS):
            values.append(value_at(row, column))

    for start in range(0, len(values), 5):
        chunk = values[start:start + 5]
        lines.append(''.join('%20.7E' % v for v in chunk))
    return '\n'.join(lines) + '\n', values


def surface(value_at):
    def at(row, column):
        if not inside(row, column):
            return NULL_VALUE
        return value_at(row, column)
    return at


def main():
    os.makedirs(DATA, exist_ok=True)
    total = 0
    summary = []

    grids = [
        ('2026-03-11', 'TOP_HUGIN_V1',
         surface(lambda r, c: top_depth(r, c, 0.0))),
        ('2026-03-12', 'TOP_HUGIN_V2',
         surface(lambda r, c: top_depth(r, c, 18.0))),
        ('2026-03-12', 'BASE_HUGIN',
         surface(lambda r, c: top_depth(r, c, 18.0) + gross_thickness(r, c))),
    ]

    for day, name, value_at in grids:
        text, values = grid(name, value_at)
        file_name = '%s_%s.zmap' % (day, name.lower())
        with open(os.path.join(DATA, file_name), 'w',
                  encoding='utf-8', newline='\n') as handle:
            handle.write(text)
        total += len(text.encode('utf-8'))
        live = [v for v in values if v != NULL_VALUE]
        summary.append((file_name, len(values), len(live), min(live), max(live)))
        print('  %-38s %7d bytes  %d nodes, %d live, %d blank, depth %.1f to %.1f'
              % (file_name, len(text.encode('utf-8')), len(values), len(live),
                 len(values) - len(live), min(live), max(live)))

    print()
    print('  grid %d rows x %d columns = %d nodes, %.0f m spacing, %.0f m2 per node'
          % (ROWS, COLUMNS, ROWS * COLUMNS, NODE_SPACING, CELL_AREA_M2))
    print('  bytes on disk %d' % total)
    return 0


if __name__ == '__main__':
    sys.exit(main())
