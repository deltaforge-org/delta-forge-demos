"""Write the ECLIPSE simulator output this demo reads.

Scenario: a reservoir engineer is history matching a field before a
development plan goes in. Each iteration of the match is a full simulation
run, and the simulator writes a set of files sharing one base name:

    CASE.EGRID    grid geometry and which cells are active
    CASE.INIT     static properties, written once at initialisation
    CASE.UNRST    the restart file, one set of arrays per report step
    CASE.SMSPEC   the summary specification, naming the well vectors

Two runs are written here. Run 12 has the aquifer too weak and its pressure
falls away from the observed decline; run 13 strengthens it and lands closer.
Reviewing that difference is the whole job, and it is a SQL question once the
files are readable.

Everything is written from a literal description, so the counts the demo
asserts are known before any reader sees the bytes. Deterministic: rerunning
produces byte identical files.

    python generate_data.py

# The format

ECLIPSE binary output is Fortran unformatted records: every record is its
byte length as a big-endian int32, the payload, then the same length again.
A keyword is a 24-byte header record (an 8-character name, an element count,
a 4-character type tag) followed by data records holding the elements, in
blocks of 1000 for numbers and 105 for strings.

That block size is the part a naive reader gets wrong. A 1400-element array
is not one record, it is two, and a reader that assumes one silently returns
the first thousand.
"""
import math
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

NI, NJ, NK = 12, 12, 8
CELLS = NI * NJ * NK
REPORT_STEPS = 6

# Numeric arrays are written in blocks of 1000 elements, character arrays in
# blocks of 105. Both are the format's own limits, not ours.
NUMERIC_BLOCK = 1000
CHAR_BLOCK = 105

WELLS = ['PROD-1', 'PROD-2', 'INJ-1']
VECTORS = ['WOPR', 'WWPR', 'WBHP']
VECTOR_UNITS = {'WOPR': 'SM3/DAY', 'WWPR': 'SM3/DAY', 'WBHP': 'BARSA'}

# The two history-match iterations. `aquifer` scales how well pressure is
# supported: run 12 is too weak and drops away, run 13 is closer to observed.
RUNS = [
    ('2026-03-11', 'HM12', 0.45),
    ('2026-03-12', 'HM13', 0.80),
]

# The observed reservoir pressure the match is aiming at, one per report step.
OBSERVED_PRESSURE = [310.0, 302.0, 295.0, 289.0, 284.0, 280.0]


class Rng:
    """A linear congruential generator, so regeneration is byte identical."""

    def __init__(self, seed):
        self.state = seed & 0xFFFFFFFF

    def next(self):
        self.state = (1103515245 * self.state + 12345) & 0x7FFFFFFF
        return self.state

    def uniform(self, low, high):
        return low + (high - low) * (self.next() / 0x7FFFFFFF)


def fortran_record(payload):
    """One Fortran unformatted record: length, payload, length again."""
    return struct.pack('>i', len(payload)) + payload + struct.pack('>i', len(payload))


def keyword(name, tag, values=None, texts=None):
    """One ECLIPSE keyword: a header record then the data records."""
    values = values or []
    texts = texts or []
    count = len(texts) if tag == 'CHAR' else len(values)

    header = name.ljust(8).encode('ascii')
    header += struct.pack('>i', count)
    header += tag.ljust(4).encode('ascii')
    out = bytearray(fortran_record(header))

    block_size = CHAR_BLOCK if tag == 'CHAR' else NUMERIC_BLOCK
    emitted = 0
    while emitted < count:
        take = min(block_size, count - emitted)
        block = bytearray()
        for index in range(emitted, emitted + take):
            if tag == 'REAL':
                block += struct.pack('>f', values[index])
            elif tag == 'DOUB':
                block += struct.pack('>d', values[index])
            elif tag == 'INTE':
                block += struct.pack('>i', int(values[index]))
            elif tag == 'LOGI':
                block += struct.pack('>i', -1 if values[index] else 0)
            elif tag == 'CHAR':
                block += texts[index].ljust(8).encode('ascii')[:8]
            else:
                raise ValueError('this generator does not write %s' % tag)
        out += fortran_record(bytes(block))
        emitted += take
    return bytes(out)


def cell_index(i, j, k):
    return i + j * NI + k * NI * NJ


def actnum():
    """Which cells are active. A rim of inactive cells around the model is
    what a real grid looks like once it is cut to a fault block."""
    flags = []
    for k in range(NK):
        for j in range(NJ):
            for i in range(NI):
                edge = i == 0 or j == 0 or i == NI - 1 or j == NJ - 1
                flags.append(0 if edge and k == 0 else 1)
    return flags


def write_run(day, run, aquifer):
    rng = Rng(sum(ord(c) for c in run) * 977)
    written = []

    # ── EGRID: geometry and the active-cell flags ────────────────────────
    egrid = bytearray()
    egrid += keyword('FILEHEAD', 'INTE', [3] + [0] * 99)
    egrid += keyword('GRIDHEAD', 'INTE', [1, NI, NJ, NK] + [0] * 96)
    coord = []
    for j in range(NJ + 1):
        for i in range(NI + 1):
            coord += [i * 100.0, j * 100.0, 2400.0, i * 100.0, j * 100.0, 2480.0]
    egrid += keyword('COORD', 'REAL', coord)
    egrid += keyword('ACTNUM', 'INTE', actnum())
    written.append(('%s_%s.EGRID' % (day, run), bytes(egrid)))

    # ── INIT: static properties, written once ────────────────────────────
    init = bytearray()
    init += keyword('INTEHEAD', 'INTE', [0] * 95)
    poro, permx = [], []
    for index in range(CELLS):
        k = index // (NI * NJ)
        # Porosity falls with depth, permeability follows it exponentially,
        # which is the relationship a static model is built on.
        base = 0.26 - 0.03 * k
        p = base + rng.uniform(-0.02, 0.02)
        poro.append(p)
        permx.append(math.exp(12.0 * p - 1.2))
    init += keyword('PORO', 'REAL', poro)
    init += keyword('PERMX', 'REAL', permx)
    written.append(('%s_%s.INIT' % (day, run), bytes(init)))

    # ── UNRST: one set of arrays per report step ─────────────────────────
    # SEQNUM separates the steps in the file; the reader's `occurrence`
    # column is what separates them in SQL.
    unrst = bytearray()
    for step in range(REPORT_STEPS):
        decline = (OBSERVED_PRESSURE[0] - OBSERVED_PRESSURE[step]) / aquifer
        pressure = [OBSERVED_PRESSURE[0] - decline + rng.uniform(-1.5, 1.5)
                    for _ in range(CELLS)]
        swat = [0.22 + step * 0.045 + rng.uniform(-0.015, 0.015)
                for _ in range(CELLS)]
        unrst += keyword('SEQNUM', 'INTE', [step])
        unrst += keyword('PRESSURE', 'REAL', pressure)
        unrst += keyword('SWAT', 'REAL', swat)
    written.append(('%s_%s.UNRST' % (day, run), bytes(unrst)))

    # ── SMSPEC: the well summary vectors, as character arrays ────────────
    smspec = bytearray()
    smspec += keyword('INTEHEAD', 'INTE', [1, 100])
    smspec += keyword('KEYWORDS', 'CHAR', texts=VECTORS * len(WELLS))
    smspec += keyword('WGNAMES', 'CHAR',
                      texts=[w for w in WELLS for _ in VECTORS])
    smspec += keyword('UNITS', 'CHAR',
                      texts=[VECTOR_UNITS[v] for v in VECTORS] * len(WELLS))
    written.append(('%s_%s.SMSPEC' % (day, run), bytes(smspec)))
    return written


def main():
    os.makedirs(DATA, exist_ok=True)
    active = sum(actnum())
    total = 0
    for day, run, aquifer in RUNS:
        for name, payload in write_run(day, run, aquifer):
            with open(os.path.join(DATA, name), 'wb') as handle:
                handle.write(payload)
            total += len(payload)
            print('  %-28s %9d bytes' % (name, len(payload)))
    print()
    print('  grid            %d x %d x %d = %d cells, %d active'
          % (NI, NJ, NK, CELLS, active))
    print('  report steps    %d' % REPORT_STEPS)
    print('  summary vectors %d wells x %d vectors = %d'
          % (len(WELLS), len(VECTORS), len(WELLS) * len(VECTORS)))
    print('  bytes on disk   %d' % total)
    return 0


if __name__ == '__main__':
    sys.exit(main())
