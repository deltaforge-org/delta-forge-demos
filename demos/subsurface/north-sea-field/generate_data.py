"""Generate the fixture files for the subsurface demo.

Every file is written from a deterministic description, so the row counts the
demo asserts on are computed here rather than guessed. A seeded generator keeps
the files byte-identical across regenerations, which matters because they are
committed.
"""
import io
import math
import os
import struct

OUT = 'delta-forge-demos/demos/subsurface/north-sea-field/data'
os.makedirs(OUT, exist_ok=True)

counts = {}


def write(name, data):
    path = os.path.join(OUT, name)
    with open(path, 'wb') as f:
        f.write(data)
    print('%-34s %8d bytes' % (name, len(data)))
    return path


# ── deterministic pseudo-random, so regeneration is byte-identical ─────────
class Rng:
    def __init__(self, seed):
        self.state = seed & 0xFFFFFFFF

    def next(self):
        # A plain linear congruential generator: reproducible everywhere,
        # and the statistical quality of the noise does not matter here.
        self.state = (1103515245 * self.state + 12345) & 0x7FFFFFFF
        return self.state

    def uniform(self, low, high):
        return low + (high - low) * (self.next() / 0x7FFFFFFF)


# ═══════════════════════════════════════════════════════════════════════════
# SEG-Y: a small 3D survey, 12 inlines x 10 crosslines, 100 samples per trace
# ═══════════════════════════════════════════════════════════════════════════

def ibm_word(value):
    if value == 0.0:
        return 0
    sign = 0x80000000 if value < 0 else 0
    magnitude = abs(value)
    exponent = 0
    while magnitude >= 1.0:
        magnitude /= 16.0
        exponent += 1
    while 0.0 < magnitude < 1.0 / 16.0:
        magnitude *= 16.0
        exponent -= 1
    fraction = int(round(magnitude * 0x1000000)) & 0x00FFFFFF
    return sign | ((exponent + 64) << 24) | fraction


def ebcdic(text):
    """Encode ASCII text as EBCDIC cp037, padded to 3200 bytes of 40x80."""
    lines = text.strip('\n').split('\n')
    padded = [('C%02d %s' % (i + 1, lines[i] if i < len(lines) else '')).ljust(80)[:80]
              for i in range(40)]
    return ''.join(padded).encode('cp037')


INLINES, CROSSLINES, SAMPLES = 12, 10, 100
SEGY_TRACES = INLINES * CROSSLINES
counts['segy_traces'] = SEGY_TRACES
counts['segy_samples'] = SAMPLES

textual = """
DELTAFORGE SUBSURFACE DEMO - SYNTHETIC 3D SEISMIC VOLUME
SURVEY: NORTH SEA DEMO FIELD
FIELD: DEMO-NS
CLIENT: DELTAFORGE
PROCESSING: SYNTHETIC, GENERATED FOR DEMONSTRATION ONLY
SAMPLE INTERVAL: 4000 MICROSECONDS
SAMPLES PER TRACE: 100
FORMAT: 4-BYTE IBM FLOATING POINT
INLINE RANGE: 1000-1011
CROSSLINE RANGE: 2000-2009
COORDINATE SYSTEM: UTM ZONE 31N (EPSG 32631)
COORDINATE SCALAR: -100 (DIVIDE BY 100)
THIS IS NOT REAL DATA
"""

segy = bytearray(ebcdic(textual))

binary = bytearray(400)
struct.pack_into('>h', binary, 16, 4000)      # sample interval, microseconds
struct.pack_into('>h', binary, 20, SAMPLES)   # samples per trace
struct.pack_into('>h', binary, 24, 1)         # format code 1: IBM float
struct.pack_into('>h', binary, 12, CROSSLINES)  # traces per ensemble
struct.pack_into('>h', binary, 300, 1)        # SEG-Y revision 1
segy += binary

rng = Rng(20260824)
for il in range(INLINES):
    for xl in range(CROSSLINES):
        header = bytearray(240)
        trace_no = il * CROSSLINES + xl + 1
        struct.pack_into('>i', header, 0, trace_no)          # tracl
        struct.pack_into('>i', header, 8, 1000 + il)         # fldr
        struct.pack_into('>i', header, 20, trace_no)         # cdp
        struct.pack_into('>h', header, 28, 1)                # trid: live trace
        struct.pack_into('>h', header, 68, -100)             # elevation scalar
        struct.pack_into('>h', header, 70, -100)             # coordinate scalar
        # UTM 31N, written x100 because the scalar divides.
        struct.pack_into('>i', header, 72, (450000 + xl * 25) * 100)   # sx
        struct.pack_into('>i', header, 76, (6500000 + il * 25) * 100)  # sy
        struct.pack_into('>i', header, 180, (450000 + xl * 25) * 100)  # cdpx
        struct.pack_into('>i', header, 184, (6500000 + il * 25) * 100) # cdpy
        struct.pack_into('>h', header, 114, SAMPLES)
        struct.pack_into('>h', header, 116, 4000)
        struct.pack_into('>i', header, 188, 1000 + il)       # inline
        struct.pack_into('>i', header, 192, 2000 + xl)       # crossline
        segy += header

        for s in range(SAMPLES):
            # Two reflectors plus a little noise, so the volume looks like
            # something rather than being flat.
            t = s * 0.004
            amplitude = (
                math.sin(2 * math.pi * 12 * t) * math.exp(-1.5 * t) * 4000
                + math.sin(2 * math.pi * 30 * (t - 0.18)) * math.exp(-6 * abs(t - 0.18)) * 2500
                + rng.uniform(-120, 120)
            )
            segy += struct.pack('>I', ibm_word(amplitude))

write('demo_survey.segy', bytes(segy))


# ═══════════════════════════════════════════════════════════════════════════
# LAS: two wells, 300 depth steps each
# ═══════════════════════════════════════════════════════════════════════════

LAS_STEPS = 300
counts['las_steps_per_well'] = LAS_STEPS
counts['las_wells'] = 2
counts['las_rows'] = LAS_STEPS * 2

WELLS = [
    ('15/9-F-1', 'DEMO-NS', '5509000010', 2100.0),
    ('15/9-F-2', 'DEMO-NS', '5509000011', 2050.0),
]

for index, (well, field, uwi, start) in enumerate(WELLS):
    stop = start + (LAS_STEPS - 1) * 0.15
    lines = [
        '~Version Information',
        'VERS.                 2.0 : CWLS LOG ASCII STANDARD - VERSION 2.0',
        'WRAP.                  NO : ONE LINE PER DEPTH STEP',
        '~Well Information',
        '#MNEM.UNIT       DATA                     DESCRIPTION',
        'STRT.M       %12.4f              : START DEPTH' % start,
        'STOP.M       %12.4f              : STOP DEPTH' % stop,
        'STEP.M       %12.4f              : STEP' % 0.15,
        'NULL.        %12.4f              : NULL VALUE' % -999.25,
        'COMP.        DELTAFORGE DEMO          : COMPANY',
        'WELL.        %-24s : WELL' % well,
        'FLD .        %-24s : FIELD' % field,
        'UWI .        %-24s : UNIQUE WELL ID' % uwi,
        'SRVC.        SYNTHETIC                : SERVICE COMPANY',
        '~Curve Information',
        '#MNEM.UNIT                              DESCRIPTION',
        'DEPT.M                                  : 1  MEASURED DEPTH',
        'GR  .GAPI                               : 2  GAMMA RAY',
        'RHOB.G/C3                               : 3  BULK DENSITY',
        'NPHI.V/V                                : 4  NEUTRON POROSITY',
        'DT  .US/F                               : 5  SONIC TRANSIT TIME',
        '~Parameter Information',
        'RUN .        1                        : RUN NUMBER',
        '~ASCII',
    ]

    rng = Rng(4242 + index * 7)
    for step in range(LAS_STEPS):
        depth = start + step * 0.15
        shale = 0.5 + 0.5 * math.sin(step / 23.0 + index)
        gr = 25 + 95 * shale + rng.uniform(-4, 4)
        rhob = 2.68 - 0.28 * (1 - shale) + rng.uniform(-0.01, 0.01)
        nphi = 0.06 + 0.24 * (1 - shale) + rng.uniform(-0.005, 0.005)
        # A short washed-out interval where the density tool reads nothing.
        if 120 <= step < 135:
            rhob_field = -999.2500
        else:
            rhob_field = rhob
        dt = 55 + 45 * shale + rng.uniform(-1.5, 1.5)
        lines.append('%10.4f %9.4f %9.4f %9.4f %9.4f'
                     % (depth, gr, rhob_field, nphi, dt))

    write('well_%s.las' % well.replace('/', '_').replace('-', '_'),
          ('\n'.join(lines) + '\n').encode('ascii'))


# ═══════════════════════════════════════════════════════════════════════════
# ZMAP+: a top-reservoir depth grid, 40 rows x 50 columns
# ═══════════════════════════════════════════════════════════════════════════

ZROWS, ZCOLS = 40, 50
NULL_VALUE = 1.0e30
grid = []
null_nodes = 0
for column in range(ZCOLS):
    for row in range(ZROWS):
        x = column / (ZCOLS - 1)
        y = row / (ZROWS - 1)
        # A dome with its crest off centre, blanked outside an ellipse.
        radius = math.hypot((x - 0.45) / 0.42, (y - 0.5) / 0.46)
        if radius > 1.0:
            grid.append(NULL_VALUE)
            null_nodes += 1
        else:
            grid.append(2450.0 - 180.0 * math.cos(radius * math.pi / 2))

counts['zmap_nodes'] = ZROWS * ZCOLS
counts['zmap_null_nodes'] = null_nodes
counts['zmap_live_nodes'] = ZROWS * ZCOLS - null_nodes

zmap_lines = [
    '!',
    '! DeltaForge subsurface demo: synthetic top-reservoir depth grid',
    '! Not real data',
    '!',
    '@DEMO_TOP_RES, GRID, 5',
    '   20, 1.0E+30, , 7, 1',
    '   %d, %d, %.1f, %.1f, %.1f, %.1f' % (
        ZROWS, ZCOLS, 450000.0, 451225.0, 6500000.0, 6500975.0),
    '   0.0, 0.0, 0.0',
    '@',
]
for start in range(0, len(grid), 5):
    zmap_lines.append(''.join('%15.6E' % v for v in grid[start:start + 5]))

write('top_reservoir.zmap', ('\n'.join(zmap_lines) + '\n').encode('ascii'))


# ═══════════════════════════════════════════════════════════════════════════
# GRDECL: a 20 x 15 x 8 corner-point model with three properties
# ═══════════════════════════════════════════════════════════════════════════

NI, NJ, NK = 20, 15, 8
CELLS = NI * NJ * NK
counts['grdecl_cells'] = CELLS

rng = Rng(97531)
poro, permx, actnum, satnum = [], [], [], []
active = 0
for k in range(NK):
    for j in range(NJ):
        for i in range(NI):
            # An inactive rim, so ACTNUM is doing real work in the demo.
            is_active = 1 if (2 <= i < NI - 2 and 1 <= j < NJ - 1) else 0
            actnum.append(is_active)
            active += is_active
            layer_quality = 1.0 - k / (NK * 1.5)
            p = max(0.02, min(0.33, 0.10 + 0.16 * layer_quality + rng.uniform(-0.02, 0.02)))
            poro.append(p)
            # A rough Kozeny-Carman-shaped trend, which is what a demo deck
            # should look like even though the constants are invented.
            permx.append(max(0.1, 2500.0 * (p ** 3) / ((1 - p) ** 2)))
            satnum.append(1 if k < NK // 2 else 2)

counts['grdecl_active_cells'] = active


def run_length(values, fmt):
    """Collapse consecutive equal values into ECLIPSE count*value form."""
    out, index = [], 0
    while index < len(values):
        run = 1
        while index + run < len(values) and values[index + run] == values[index]:
            run += 1
        token = fmt % values[index]
        out.append('%d*%s' % (run, token) if run > 1 else token)
        index += run
    return out


def keyword_block(name, tokens, per_line=8):
    lines = [name]
    for start in range(0, len(tokens), per_line):
        lines.append('  ' + ' '.join(tokens[start:start + per_line]))
    lines.append('/')
    return lines


deck = [
    '-- DeltaForge subsurface demo: synthetic corner-point model',
    '-- Not real data. Property trends are illustrative.',
    '',
    'SPECGRID',
    '  %d %d %d 1 F /' % (NI, NJ, NK),
    '',
]
deck += keyword_block('PORO', ['%.5f' % v for v in poro], 8) + ['']
deck += keyword_block('PERMX', ['%.3f' % v for v in permx], 8) + ['']
deck += keyword_block('ACTNUM', run_length(actnum, '%d'), 12) + ['']
deck += keyword_block('SATNUM', run_length(satnum, '%d'), 12) + ['']

write('demo_model.grdecl', ('\n'.join(deck) + '\n').encode('ascii'))


# ═══════════════════════════════════════════════════════════════════════════
# UKOOA P1/90: navigation for the survey's shot points
# ═══════════════════════════════════════════════════════════════════════════

NAV_LINES, POINTS_PER_LINE = 4, 60
counts['nav_records'] = NAV_LINES * POINTS_PER_LINE


def packed_latitude(degrees):
    hemisphere = 'N' if degrees >= 0 else 'S'
    d = abs(degrees)
    deg = int(d)
    minutes = int((d - deg) * 60)
    seconds = (d - deg - minutes / 60.0) * 3600
    return '%02d%02d%05.2f%s' % (deg, minutes, seconds, hemisphere)


def packed_longitude(degrees):
    hemisphere = 'E' if degrees >= 0 else 'W'
    d = abs(degrees)
    deg = int(d)
    minutes = int((d - deg) * 60)
    seconds = (d - deg - minutes / 60.0) * 3600
    return '%03d%02d%05.2f%s' % (deg, minutes, seconds, hemisphere)


nav = [
    'H0100 SURVEY NAME                       NORTH SEA DEMO FIELD',
    'H0200 SURVEY AREA                       DEMO-NS',
    'H0300 CLIENT                            DELTAFORGE',
    'H0400 GEODETIC DATUM                    WGS84',
    'H0500 PROJECTION                        UTM ZONE 31N',
    'H0600 NOTE                              SYNTHETIC DATA, NOT A REAL SURVEY',
]

for line_index in range(NAV_LINES):
    line_name = 'DEMO-%04d' % (100 + line_index)
    for point in range(POINTS_PER_LINE):
        latitude = 58.6 + line_index * 0.004
        longitude = 1.85 + point * 0.0025
        easting = 450000.0 + point * 25.0
        northing = 6500000.0 + line_index * 100.0
        depth = 92.0 + (point % 7)
        seconds_of_day = 28800 + line_index * 3600 + point * 10

        record = bytearray(b' ' * 80)
        record[0:1] = b'S'
        record[1:1 + len(line_name)] = line_name.encode('ascii')
        record[19:25] = ('%6d' % (1000 + point)).encode('ascii')
        record[25:35] = packed_latitude(latitude).encode('ascii')
        record[35:46] = packed_longitude(longitude).encode('ascii')
        record[46:55] = ('%9.1f' % easting).encode('ascii')
        record[55:64] = ('%9.1f' % northing).encode('ascii')
        record[64:70] = ('%6.1f' % depth).encode('ascii')
        record[70:73] = b'236'
        record[73:79] = ('%02d%02d%02d' % (
            seconds_of_day // 3600, (seconds_of_day // 60) % 60,
            seconds_of_day % 60)).encode('ascii')
        nav.append(record.decode('ascii').rstrip())

write('demo_survey.p190', ('\n'.join(nav) + '\n').encode('ascii'))


# ═══════════════════════════════════════════════════════════════════════════

print()
print('Row counts the demo asserts on:')
for key in sorted(counts):
    print('  %-24s %d' % (key, counts[key]))
