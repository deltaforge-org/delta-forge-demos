"""Generate the geomodel export audit dataset.

Four files describing ONE reservoir model, delivered the way a real handover
arrives: the RMS model itself, the ASCII copy the modeller sent for review, the
GRDECL export the simulation team was asked to run, and a depth surface.

    2026-04-02  reservoir_model.roff      binary ROFF, the model RMS holds
    2026-04-02  reservoir_model.roffasc   the same model, ASCII, for review
    2026-04-03  grid_export.grdecl        the GRDECL export of that model
    2026-04-04  top_reservoir.roffasc     a depth surface, which has no grid

The whole point of the pairing is that the two grid formats number their cells
in OPPOSITE directions. ROFF stores a per-cell array in C order over
(nX, nY, nZ), so K moves fastest and I slowest; ECLIPSE, and therefore GRDECL,
moves I fastest and K slowest. The same cell has a different ordinal in each
file, so an audit that joins the export to the original on the cell ordinal
lines up the wrong cells and reports a model that does not match itself.

Everything here is synthetic and every value is a deterministic function of
(i, j, k). There is no random number generator, so regenerating produces byte
identical files and every asserted value can be recomputed from these formulas
rather than read back out of the engine.
"""
import math
import os
import struct

HERE = os.path.dirname(os.path.abspath(__file__))
LANDING = os.path.join(HERE, 'data', 'landing')

NI, NJ, NK = 20, 15, 8
CELLS = NI * NJ * NK

# RMS writes these where it has no value. They are the writer's convention and
# not part of either format, which is exactly why the export cannot carry them
# as anything better than a number.
UNDEF_FLOAT = -999.0
UNDEF_BYTE = 255

CODE_NAMES = ['shale', 'sand', 'silt']
CODE_VALUES = [0, 1, 2]

# Three zones, top to bottom, in the file's own K direction.
SUBGRID_LAYERS = [3, 3, 2]
ZONE_NAMES = ['Upper Tarbert', 'Lower Tarbert', 'Ness']


def roff_index(i, j, k):
    """Ordinal of a cell in ROFF storage order: K fastest, I slowest."""
    return (i * NJ + j) * NK + k


def eclipse_index(i, j, k):
    """Ordinal of a cell in ECLIPSE storage order: I fastest, K slowest."""
    return (k * NJ + j) * NI + i


def is_active(i, j, k):
    """The model outline, plus one layer excluded from the simulation.

    Layer 5 is a shale break the modeller took out of the flow model. It is
    still in the grid, which is what makes "how many cells does this model
    cover" and "how many does it solve" different questions.
    """
    if k == 5:
        return 0
    cx, cy = (NI - 1) / 2.0, (NJ - 1) / 2.0
    inside = ((i - cx) / 9.6) ** 2 + ((j - cy) / 7.1) ** 2 <= 1.0
    return 1 if inside else 0


def poro_undefined(i, j, k):
    """Three columns around the injector where porosity was never populated."""
    return i in (9, 10, 11) and j == 7


def facies_undefined(i, j, k):
    """The bottom layer's western edge, outside the facies model."""
    return k == 7 and i < 3


def poro(i, j, k):
    if poro_undefined(i, j, k):
        return UNDEF_FLOAT
    base = 0.28 - 0.018 * k
    lateral = 0.010 * math.sin(i / 3.0) + 0.008 * math.cos(j / 2.5)
    return round(min(0.34, max(0.02, base + lateral)), 4)


def permx(i, j, k):
    if poro_undefined(i, j, k):
        return UNDEF_FLOAT
    p = poro(i, j, k)
    return round(min(9999.0, max(0.1, 2500.0 * p ** 3 / (1 - p) ** 2)), 2)


def ntg(i, j, k):
    return round(min(1.0, max(0.3, 0.95 - 0.03 * k + 0.02 * math.sin(j / 2.0))), 4)


def facies(i, j, k):
    if facies_undefined(i, j, k):
        return UNDEF_BYTE
    if k <= 2:
        return 1 if (i + j) % 4 != 0 else 2
    if k <= 5:
        return 2 if (i * j) % 3 != 0 else 0
    return 0 if (i + 2 * j) % 5 != 0 else 1


def f32(value):
    """The value as it survives a four-byte float, which is what ROFF holds."""
    return struct.unpack('<f', struct.pack('<f', value))[0]


def text_float(value):
    """Six significant digits, which round-trips every value here.

    The ASCII and the binary form of the model have to decode to the SAME
    four-byte float or the test that asserts they read identically is testing
    nothing. Six significant digits is enough for that, and `check_round_trip`
    proves it for every value rather than assuming it.
    """
    return '%.6g' % value


def check_round_trip():
    """No value may differ between the two forms of the file."""
    for i in range(NI):
        for j in range(NJ):
            for k in range(NK):
                for value in (poro(i, j, k), permx(i, j, k), ntg(i, j, k)):
                    written = float(text_float(value))
                    if f32(written) != f32(value):
                        raise AssertionError(
                            'cell (%d,%d,%d): %r does not survive %s'
                            % (i, j, k, value, text_float(value))
                        )


# ---------------------------------------------------------------------------
# ROFF
# ---------------------------------------------------------------------------

def roff_binary():
    out = bytearray()

    def token(text):
        out.extend(text.encode('ascii'))
        out.append(0)

    def integer(value):
        out.extend(struct.pack('<i', value))

    def real(value):
        out.extend(struct.pack('<f', value))

    token('roff-bin')
    token('#ROFF file#')
    token('#Creator: DeltaForge demo, synthetic model, not real field data#')

    token('tag')
    token('filedata')
    token('int')
    token('byteswaptest')
    integer(1)
    token('char')
    token('filetype')
    token('parameter')
    token('char')
    token('creationDate')
    token('02/04/2026 09:14:03')
    token('endtag')

    token('tag')
    token('version')
    for key, value in (('major', 2), ('minor', 0)):
        token('int')
        token(key)
        integer(value)
    token('endtag')

    token('tag')
    token('dimensions')
    for key, value in (('nX', NI), ('nY', NJ), ('nZ', NK)):
        token('int')
        token(key)
        integer(value)
    token('endtag')

    token('tag')
    token('subgrids')
    token('array')
    token('int')
    token('nLayers')
    integer(len(SUBGRID_LAYERS))
    for layers in SUBGRID_LAYERS:
        integer(layers)
    token('endtag')

    token('tag')
    token('active')
    token('array')
    token('bool')
    token('data')
    integer(CELLS)
    out.extend(bytes(is_active(i, j, k)
                     for i in range(NI) for j in range(NJ) for k in range(NK)))
    token('endtag')

    for name, fn in (('PORO', poro), ('PERMX', permx), ('NTG', ntg)):
        token('tag')
        token('parameter')
        token('char')
        token('name')
        token(name)
        token('array')
        token('float')
        token('data')
        integer(CELLS)
        for i in range(NI):
            for j in range(NJ):
                for k in range(NK):
                    real(fn(i, j, k))
        token('endtag')

    token('tag')
    token('parameter')
    token('char')
    token('name')
    token('FACIES')
    token('array')
    token('char')
    token('codeNames')
    integer(len(CODE_NAMES))
    for name in CODE_NAMES:
        token(name)
    token('array')
    token('int')
    token('codeValues')
    integer(len(CODE_VALUES))
    for code in CODE_VALUES:
        integer(code)
    token('array')
    token('byte')
    token('data')
    integer(CELLS)
    out.extend(bytes(facies(i, j, k)
                     for i in range(NI) for j in range(NJ) for k in range(NK)))
    token('endtag')

    token('tag')
    token('eof')
    token('endtag')
    return bytes(out)


def wrapped(values, per_line=12):
    lines = []
    for at in range(0, len(values), per_line):
        lines.append('  ' + ' '.join(values[at:at + per_line]))
    return lines


def roff_ascii():
    cells = [(i, j, k) for i in range(NI) for j in range(NJ) for k in range(NK)]
    lines = [
        'roff-asc',
        '#ROFF file#',
        '#Creator: DeltaForge demo, synthetic model, not real field data#',
        'tag filedata',
        'int byteswaptest 1',
        'char filetype  "parameter"',
        'char creationDate  "02/04/2026 09:14:03"',
        'endtag',
        'tag version',
        'int major 2',
        'int minor 0',
        'endtag',
        'tag dimensions',
        'int nX %d' % NI,
        'int nY %d' % NJ,
        'int nZ %d' % NK,
        'endtag',
        'tag subgrids',
        'array int nLayers %d' % len(SUBGRID_LAYERS),
        '  ' + ' '.join(str(n) for n in SUBGRID_LAYERS),
        'endtag',
        'tag active',
        'array bool data %d' % CELLS,
    ]
    lines += wrapped([str(is_active(*c)) for c in cells], 40)
    lines.append('endtag')

    for name, fn in (('PORO', poro), ('PERMX', permx), ('NTG', ntg)):
        lines += [
            'tag parameter',
            'char name "%s"' % name,
            'array float data %d' % CELLS,
        ]
        lines += wrapped([text_float(fn(*c)) for c in cells])
        lines.append('endtag')

    lines += [
        'tag parameter',
        'char name "FACIES"',
        'array char codeNames %d' % len(CODE_NAMES),
        '  ' + ' '.join('"%s"' % n for n in CODE_NAMES),
        'array int codeValues %d' % len(CODE_VALUES),
        '  ' + ' '.join(str(c) for c in CODE_VALUES),
        'array byte data %d' % CELLS,
    ]
    lines += wrapped([str(facies(*c)) for c in cells], 40)
    lines += ['endtag', 'tag eof', 'endtag']
    return ('\n'.join(lines) + '\n').encode('ascii')


def roff_surface():
    """A depth surface, which carries no dimensions tag and so has no cells."""
    sx, sy = 20, 15
    values = []
    for row in range(sy):
        for col in range(sx):
            values.append(round(2140.0 + 3.5 * col + 1.8 * row
                                - 12.0 * math.sin(col / 4.0), 2))
    lines = [
        'roff-asc',
        '#ROFF file#',
        '#Creator: DeltaForge demo, synthetic surface, not real field data#',
        'tag filedata',
        'int byteswaptest 1',
        'char filetype  "surface"',
        'char creationDate  "04/04/2026 16:02:55"',
        'endtag',
        'tag version',
        'int major 2',
        'int minor 0',
        'endtag',
        'tag surface',
        'int nx %d' % sx,
        'int ny %d' % sy,
        'float xori 458000.0',
        'float yori 6785000.0',
        'float xinc 50.0',
        'float yinc 50.0',
        'float rot 0.0',
        'array float values %d' % len(values),
    ]
    lines += wrapped([text_float(v) for v in values], 10)
    lines += ['endtag', 'tag eof', 'endtag']
    # filedata 3 keys, version 2, surface 7 scalars + one value per node.
    long_form_rows = 3 + 2 + 7 + len(values)
    return ('\n'.join(lines) + '\n').encode('ascii'), len(values), long_form_rows


# ---------------------------------------------------------------------------
# GRDECL, the export
# ---------------------------------------------------------------------------

def grdecl_export():
    """The same model, written the way a simulator wants it.

    Values run in ECLIPSE order, I fastest, which is the reverse of the order
    the ROFF file holds them in. The export is FAITHFUL: cell (i, j, k) carries
    the same numbers in both files. What it cannot carry is the facies code
    NAMES, because GRDECL has no place to put them, and it has no null, so the
    cells RMS left undefined arrive as the marker -999 rather than as nothing.
    """
    def block(keyword, fn, fmt):
        values = [fmt % fn(i, j, k)
                  for k in range(NK) for j in range(NJ) for i in range(NI)]
        out = [keyword]
        out += wrapped(values, 12)
        out.append('  /')
        out.append('')
        return out

    lines = [
        '-- DeltaForge demo: GRDECL export of the RMS model, 2026-04-03',
        '-- Synthetic. Written by the modelling team from reservoir_model.roff.',
        '',
        'SPECGRID',
        '  %d %d %d 1 F /' % (NI, NJ, NK),
        '',
    ]
    lines += block('ACTNUM', is_active, '%d')
    lines += block('PORO', poro, '%.6g')
    lines += block('PERMX', permx, '%.6g')
    lines += block('NTG', ntg, '%.6g')
    lines += block('FACIES', facies, '%d')
    return ('\n'.join(lines) + '\n').encode('ascii')


def main():
    check_round_trip()
    os.makedirs(LANDING, exist_ok=True)

    written = []

    def emit(name, data):
        path = os.path.join(LANDING, name)
        with open(path, 'wb') as handle:
            handle.write(data)
        written.append((name, len(data)))
        print('  %-42s %9d bytes' % (name, len(data)))

    emit('2026-04-02_reservoir_model.roff', roff_binary())
    emit('2026-04-02_reservoir_model.roffasc', roff_ascii())
    surface, nodes, long_form_rows = roff_surface()
    emit('2026-04-03_grid_export.grdecl', grdecl_export())
    emit('2026-04-04_top_reservoir.roffasc', surface)

    print()
    print('grid            %d x %d x %d = %d cells' % (NI, NJ, NK, CELLS))
    print('surface nodes   %d, long form rows %d' % (nodes, long_form_rows))
    print('total bytes     %d' % sum(size for _, size in written))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
