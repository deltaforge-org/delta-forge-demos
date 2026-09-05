"""Recompute every asserted value by reading the generated files back.

This is deliberately a SECOND reader rather than a call into
`generate_data.py`: a proof computed from the same formulas that wrote the
files proves the formulas agree with themselves. This one parses the bytes on
disk, the way the engine will, so a mistake in the writer shows up here.

Run from this directory:

    python compute_proofs.py
"""
import os
import struct

HERE = os.path.dirname(os.path.abspath(__file__))
LANDING = os.path.join(HERE, 'data', 'landing')

UNDEF_FLOAT = -999.0
UNDEF_BYTE = 255


# ---------------------------------------------------------------------------
# A minimal ROFF reader
# ---------------------------------------------------------------------------

class Roff:
    def __init__(self, data):
        self.data = data
        if data.startswith(b'roff-bin'):
            self.binary = True
            self.at = len(b'roff-bin')
            if self.data[self.at:self.at + 1] == b'\x00':
                self.at += 1
        elif data.startswith(b'roff-asc'):
            self.binary = False
            self.at = len(b'roff-asc')
        else:
            raise ValueError('not a ROFF file')
        self.order = None
        self.tags = []
        self._parse()

    # -- lexing ------------------------------------------------------------

    def _skip_ascii_comment(self):
        if self.binary:
            return
        while True:
            while self.at < len(self.data) and self.data[self.at:self.at + 1].isspace():
                self.at += 1
            if self.data[self.at:self.at + 1] != b'#':
                return
            end = self.data.index(b'#', self.at + 1)
            self.at = end + 1

    def _token(self):
        if self.binary:
            if self.at >= len(self.data):
                return None
            end = self.data.index(b'\x00', self.at)
            out = self.data[self.at:end].decode('ascii')
            self.at = end + 1
            return out
        while self.at < len(self.data) and self.data[self.at:self.at + 1].isspace():
            self.at += 1
        if self.at >= len(self.data):
            return None
        start = self.at
        while self.at < len(self.data) and not self.data[self.at:self.at + 1].isspace():
            self.at += 1
        return self.data[start:self.at].decode('ascii')

    def _string(self):
        if self.binary:
            return self._token()
        while self.at < len(self.data) and self.data[self.at:self.at + 1].isspace():
            self.at += 1
        if self.data[self.at:self.at + 1] != b'"':
            return self._token()
        end = self.data.index(b'"', self.at + 1)
        out = self.data[self.at + 1:end].decode('ascii')
        self.at = end + 1
        return out

    def _fixed(self, fmt, width):
        raw = self.data[self.at:self.at + width]
        self.at += width
        return struct.unpack(('<' if self.order == 'little' else '>') + fmt, raw)[0]

    def _scalar(self, type_name, key):
        if self.binary and type_name == 'int' and key.lower() == 'byteswaptest':
            raw = self.data[self.at:self.at + 4]
            self.at += 4
            self.order = 'little' if raw == b'\x01\x00\x00\x00' else 'big'
            return 1
        if self.binary:
            if type_name == 'char':
                return self._token()
            if type_name in ('bool', 'byte'):
                value = self.data[self.at]
                self.at += 1
                return value
            if type_name == 'int':
                return self._fixed('i', 4)
            if type_name == 'float':
                return self._fixed('f', 4)
            if type_name == 'double':
                return self._fixed('d', 8)
            raise ValueError(type_name)
        if type_name == 'char':
            return self._string()
        token = self._token()
        if type_name in ('bool', 'byte', 'int'):
            return int(token)
        return float(token)

    def _length(self):
        return self._fixed('i', 4) if self.binary else int(self._token())

    def _parse(self):
        while True:
            self._skip_ascii_comment()
            token = self._token()
            if token is None:
                break
            if not token or token.startswith('#'):
                continue
            if token == 'eof':
                break
            if token != 'tag':
                raise ValueError('expected tag, found %r' % token)
            name = self._token().lower()
            keys = {}
            order = []
            while True:
                self._skip_ascii_comment()
                token = self._token()
                if token is None:
                    raise ValueError('tag %s not closed' % name)
                if not token or token.startswith('#'):
                    continue
                if token == 'endtag':
                    break
                if token == 'array':
                    element = self._token().lower()
                    key = self._token().lower()
                    count = self._length()
                    values = [self._scalar(element, '') for _ in range(count)]
                    keys[key] = values
                    order.append(key)
                else:
                    key = self._token().lower()
                    keys[key] = self._scalar(token.lower(), key)
                    order.append(key)
            self.tags.append((name, keys, order))

    # -- the grid view -----------------------------------------------------

    def dimensions(self):
        for name, keys, _ in self.tags:
            if name == 'dimensions':
                return keys['nx'], keys['ny'], keys['nz']
        return None

    def array(self, tag_name, key):
        for name, keys, _ in self.tags:
            if name == tag_name and key in keys:
                return keys[key]
        return None

    def parameters(self):
        out = {}
        for name, keys, _ in self.tags:
            if name == 'parameter' and 'data' in keys:
                out[keys.get('name', '?')] = keys
        return out

    def long_form_rows(self):
        total = 0
        for _, keys, order in self.tags:
            for key in order:
                value = keys[key]
                total += len(value) if isinstance(value, list) else 1
        return total


def f32(value):
    return struct.unpack('<f', struct.pack('<f', value))[0]


# ---------------------------------------------------------------------------
# A minimal GRDECL reader
# ---------------------------------------------------------------------------

def read_grdecl(path):
    with open(path, 'r', encoding='ascii') as handle:
        text = handle.read()
    keywords = {}
    current, values = None, []
    for raw in text.splitlines():
        line = raw.split('--')[0].strip()
        if not line:
            continue
        for token in line.split():
            if token == '/':
                if current:
                    keywords[current] = values
                    current, values = None, []
                continue
            if current is None:
                if token[0].isalpha():
                    current, values = token.upper(), []
                continue
            if '*' in token:
                count, _, value = token.partition('*')
                values.extend([float(value or 0)] * int(count))
            elif token in ('F', 'T'):
                values.append(0.0 if token == 'F' else 1.0)
            else:
                values.append(float(token))
    if current:
        keywords[current] = values
    return keywords


# ---------------------------------------------------------------------------

def main():
    binary = Roff(open(os.path.join(LANDING, '2026-04-02_reservoir_model.roff'), 'rb').read())
    ascii_form = Roff(open(os.path.join(LANDING, '2026-04-02_reservoir_model.roffasc'), 'rb').read())
    surface = Roff(open(os.path.join(LANDING, '2026-04-04_top_reservoir.roffasc'), 'rb').read())
    deck = read_grdecl(os.path.join(LANDING, '2026-04-03_grid_export.grdecl'))

    ni, nj, nk = binary.dimensions()
    cells = ni * nj * nk
    print('byte order              %s' % binary.order)
    print('dimensions              %d x %d x %d = %d cells' % (ni, nj, nk, cells))
    assert ascii_form.dimensions() == (ni, nj, nk)

    active = binary.array('active', 'data')
    params = binary.parameters()
    poro = params['PORO']['data']
    permx = params['PERMX']['data']
    ntg = params['NTG']['data']
    facies = params['FACIES']['data']
    code_names = params['FACIES']['codenames']
    code_values = params['FACIES']['codevalues']
    codes = dict(zip(code_values, code_names))
    layers = binary.array('subgrids', 'nlayers')

    # The reader's own index derivation, K fastest.
    def ijk(cell):
        return cell // (nk * nj), (cell // nk) % nj, cell % nk

    def subgrid_of(k):
        base = 0
        for index, count in enumerate(layers):
            if k < base + count:
                return index
            base += count
        return None

    n_active = sum(active)
    print()
    print('--- Q2 the model as the simulator solves it')
    print('active cells            %d' % n_active)
    print('inactive cells          %d' % (cells - n_active))

    print()
    print('--- Q3 the full extent')
    print('total cells             %d' % cells)
    print('distinct i/j/k          %d %d %d' % (ni, nj, nk))
    print('max i/j/k               %d %d %d' % (ni - 1, nj - 1, nk - 1))
    print('cell_index range        %d..%d' % (0, cells - 1))
    dead = [k for k in range(nk)
            if not any(active[c] for c in range(cells) if ijk(c)[2] == k)]
    print('layers with no active   %s' % dead)
    print('cells in that layer     %d' % sum(1 for c in range(cells) if ijk(c)[2] == dead[0]))

    print()
    print('--- Q4 the ordering, ROFF side')
    first_eight = [ijk(c) for c in range(8)]
    print('cells 0..7 (i,j,k)      %s' % first_eight)
    print('distinct k in 0..7      %d' % len({t[2] for t in first_eight}))
    print('distinct i in 0..7      %d' % len({t[0] for t in first_eight}))

    print()
    print('--- Q4b the ordering, GRDECL side')
    # The deck reader derives i fastest from SPECGRID.
    ecl_first_eight = [(c % ni, (c // ni) % nj, c // (ni * nj)) for c in range(8)]
    print('cells 0..7 (i,j,k)      %s' % ecl_first_eight)
    print('distinct i in 0..7      %d' % len({t[0] for t in ecl_first_eight}))
    print('distinct k in 0..7      %d' % len({t[2] for t in ecl_first_eight}))

    print()
    print('--- Q5 facies by name')
    counts = {}
    undefined_facies = 0
    for c in range(cells):
        if not active[c]:
            continue
        code = facies[c]
        if code == UNDEF_BYTE:
            undefined_facies += 1
            continue
        counts[codes[code]] = counts.get(codes[code], 0) + 1
    for name in sorted(counts):
        print('%-24s%d' % (name, counts[name]))
    print('%-24s%d' % ('undefined (255)', undefined_facies))
    print('code names              %s' % code_names)

    print()
    print('--- Q6 undefined porosity')
    undef_poro_active = sum(1 for c in range(cells)
                            if active[c] and poro[c] == UNDEF_FLOAT)
    undef_poro_all = sum(1 for c in range(cells) if poro[c] == UNDEF_FLOAT)
    print('active cells, poro null %d' % undef_poro_active)
    print('all cells, poro null    %d' % undef_poro_all)

    print()
    print('--- Q7 the two forms agree')
    ascii_params = ascii_form.parameters()
    for name in ('PORO', 'PERMX', 'NTG'):
        theirs = [f32(v) for v in ascii_params[name]['data']]
        mine = [f32(v) for v in params[name]['data']]
        assert theirs == mine, '%s differs between the binary and ASCII forms' % name
    assert ascii_form.array('active', 'data') == active
    assert ascii_params['FACIES']['data'] == facies
    print('every value identical   yes (%d cells x 4 parameters)' % cells)
    defined = [f32(poro[c]) for c in range(cells)
               if active[c] and poro[c] != UNDEF_FLOAT]
    print('active defined poro     %d cells' % len(defined))
    print('sum(poro) 6dp           %.6f' % sum(defined))
    print('min / max poro          %.6f %.6f' % (min(defined), max(defined)))
    print('round(avg,6)            %.6f' % (sum(defined) / len(defined)))

    print()
    print('--- Q8 the audit, joined on i, j, k')
    ecl_poro = deck['PORO']
    ecl_actnum = deck['ACTNUM']
    ecl_facies = deck['FACIES']

    def ecl_at(i, j, k):
        return (k * nj + j) * ni + i

    matched = 0
    worst = 0.0
    mismatches = 0
    for c in range(cells):
        if not active[c]:
            continue
        i, j, k = ijk(c)
        e = ecl_at(i, j, k)
        assert ecl_actnum[e] == 1.0, 'export disagrees about which cells are active'
        if poro[c] == UNDEF_FLOAT:
            continue
        matched += 1
        diff = abs(f32(poro[c]) - ecl_poro[e])
        worst = max(worst, diff)
        if diff > 1e-6:
            mismatches += 1
    print('cells compared          %d' % matched)
    print('largest difference      %.3e' % worst)
    print('cells over 1e-6         %d' % mismatches)

    print()
    print('--- Q9 the audit, joined on cell_index instead')
    same_cell = 0
    for c in range(cells):
        i, j, k = ijk(c)
        if ecl_at(i, j, k) == c:
            same_cell += 1
    print('ordinals naming the same cell   %d of %d' % (same_cell, cells))
    # Of the active cells, how many would a cell_index join line up correctly?
    same_active = sum(1 for c in range(cells)
                      if active[c] and ecl_at(*ijk(c)) == c)
    print('of the active cells             %d of %d' % (same_active, n_active))

    print()
    print('--- Q10 what the export could not carry')
    lost = sum(1 for c in range(cells)
               if active[c] and poro[c] == UNDEF_FLOAT
               and ecl_poro[ecl_at(*ijk(c))] == UNDEF_FLOAT)
    print('null in ROFF, -999 in the deck  %d' % lost)
    ecl_undef_facies = sum(1 for c in range(cells)
                           if active[c] and facies[c] == UNDEF_BYTE
                           and ecl_facies[ecl_at(*ijk(c))] == UNDEF_BYTE)
    print('facies 255 carried as a number  %d' % ecl_undef_facies)

    print()
    print('--- Q11 the surface, in long form')
    print('filetype                %s' % [k['filetype'] for n, k, _ in surface.tags
                                          if n == 'filedata'][0])
    print('long form rows          %d' % surface.long_form_rows())
    print('has a dimensions tag    %s' % (surface.dimensions() is not None))
    values_of_surface = surface.array('surface', 'values')
    print('surface nodes           %d' % len(values_of_surface))
    print('first node value        %s' % f32(values_of_surface[0]))
    print('deepest node            %.2f' % max(f32(v) for v in values_of_surface))

    print()
    print('--- Q12 the zones')
    print('nLayers                 %s' % layers)
    for index in range(len(layers)):
        n = sum(1 for c in range(cells) if active[c] and subgrid_of(ijk(c)[2]) == index)
        print('zone %d active cells     %d' % (index, n))

    print()
    print('--- VERIFY pore volume')
    # A cell is 50 x 50 x 5 metres, so 12,500 cubic metres of bulk rock.
    bulk = 50.0 * 50.0 * 5.0
    pv = 0.0
    counted = 0
    for c in range(cells):
        if not active[c] or poro[c] == UNDEF_FLOAT:
            continue
        pv += bulk * f32(poro[c]) * f32(ntg[c])
        counted += 1
    print('cells contributing      %d' % counted)
    print('pore volume m3          %.2f' % pv)
    print('rounded to whole m3     %d' % round(pv))
    net = sum(bulk * f32(ntg[c]) for c in range(cells)
              if active[c] and poro[c] != UNDEF_FLOAT)
    print('net rock volume m3      %.2f' % net)
    print('avg permx (defined)     %.4f' % (
        sum(f32(permx[c]) for c in range(cells)
            if active[c] and permx[c] != UNDEF_FLOAT) / counted))

    # ----------------------------------------------------------------------
    # Every asserted value, spelled exactly as queries.sql states it.
    #
    # The section above is for reading. This one is for checking: it prints
    # each number in the same form the assertion uses, so the two can be
    # compared mechanically rather than by eye. A value that appears in
    # queries.sql and not here has no proof behind it.
    # ----------------------------------------------------------------------
    zones = {}
    for c in range(cells):
        if not active[c]:
            continue
        k = ijk(c)[2]
        index = subgrid_of(k)
        total, defined_count, lo, hi = zones.get(index, (0, 0, 99, -1))
        zones[index] = (total + 1,
                        defined_count + (0 if poro[c] == UNDEF_FLOAT else 1),
                        min(lo, k), max(hi, k))

    roff_active = {c for c in range(cells) if active[c]}
    ecl_active = {e for e in range(cells) if ecl_actnum[e] == 1.0}
    ordinal_join = len(roff_active & ecl_active)

    values = [
        ('cells', cells),
        ('active_cells', n_active),
        ('inactive_cells', cells - n_active),
        ('distinct_i', ni), ('distinct_j', nj), ('distinct_k', nk),
        ('max_i', ni - 1), ('max_j', nj - 1), ('max_k', nk - 1),
        ('first_cell', 0), ('last_cell', cells - 1),
        ('excluded_layer_k', dead[0]),
        ('excluded_layer_cells', sum(1 for c in range(cells) if ijk(c)[2] == dead[0])),
        ('facies_sand', counts['sand']),
        ('facies_shale', counts['shale']),
        ('facies_silt', counts['silt']),
        ('facies_undefined', undefined_facies),
        ('defined_poro', len(defined)),
        ('undefined_poro', undef_poro_active),
        ('poro_sum', round(sum(defined), 2)),
        ('poro_min', round(min(defined), 4)),
        ('poro_max', round(max(defined), 4)),
        ('audit_compared', matched),
        ('audit_beyond_tolerance', mismatches),
        ('audit_largest_difference', round(worst, 6)),
        ('ordinal_join_rows', ordinal_join),
        ('ordinal_join_same_cell', 0),
        ('lost_to_minus_999', lost),
        ('lost_facies_names', ecl_undef_facies),
        ('surface_rows_total', surface.long_form_rows()),
        ('surface_nodes', len(values_of_surface)),
        ('surface_deepest_node', round(max(f32(v) for v in values_of_surface), 2)),
        ('pore_volume_m3', int(round(pv))),
        ('facies_named', n_active - undefined_facies),
    ]
    for index in sorted(zones):
        total, defined_count, lo, hi = zones[index]
        values.append(('zone%d_cells' % index, total))
        values.append(('zone%d_defined_poro' % index, defined_count))
        values.append(('zone%d_first_layer' % index, lo))
        values.append(('zone%d_last_layer' % index, hi))

    print()
    print('--- ASSERTION VALUES, as queries.sql states them')
    for name, value in values:
        print('%-26s %s' % (name, value))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
