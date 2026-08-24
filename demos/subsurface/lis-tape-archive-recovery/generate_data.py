"""Write the second tape in this demo's archive batch.

The first tape is real: Equinor's Volve 15/9-F-4 composite log as OSDU
redistributes it, byte for byte (see ATTRIBUTION.md in the parent folder).

The second is written here, because no open corpus publishes a second LIS
tape and the demo needs two deliveries to be an incremental load at all. It
is not filler: it reproduces the shape that actually breaks LIS readers and
that the real tape does not exercise.

  - Its Data Format Specification Record describes 26 curves. Twenty-six
    forty-byte Datum Specification Blocks plus the entry blocks is 1047
    bytes against the 1024-byte physical record cap every real writer
    observes, so the specification arrives split across two physical
    records with the successor and predecessor attribute bits set. A reader
    that stops at the first physical record sees a short curve list and a
    frame width that no longer divides the data, and returns plausible
    nonsense rather than an error.
  - It is wrapped in Tape Image Format, the twelve-byte tape framing that
    archived logs carry and that hides the record structure underneath from
    anything that does not know to strip it. The real Volve tape carries it
    too, which is how we know it is not a museum piece.
  - Every curve is written on a physically plausible profile whose range is
    disjoint from its neighbours'. A frame read at the wrong offset therefore
    does not merely look wrong: the value lands outside what that curve can
    physically be, and the load's own validity rules reject it rather than
    carrying it forward as a number nobody questions.

Deterministic: rerunning produces byte identical output.

    python generate_data.py
"""
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing', 'reprocessed')

# Physical record attribute bits. 0x0001 is the successor bit, which says the
# logical record continues in the next physical record, and 0x0002 is the
# predecessor bit on that continuation.
PR_SUCCESSOR = 0x0001
PR_PREDECESSOR = 0x0002

# LIS-79 logical record types. 64 is the Data Format Specification Record;
# 34, which is easy to mistake for it, is Well Site Data.
LR_DATA = 0
LR_FORMAT_SPEC = 64
LR_FILE_HEADER = 128
LR_FILE_TRAILER = 129

PHYSICAL_RECORD_CAP = 1024
TIF_LENGTH = 12

# The reprocessed composite's curve set. The first nine are the core the
# curated table keeps; the rest are the reprocessing products that make the
# specification too wide for one physical record. Each carries the units it
# is recorded
# in and a value profile. The profiles are physically plausible AND mutually
# disjoint in range, so a frame read at the wrong offset does not merely look
# wrong, it lands outside the curve's possible values and the load's own
# validity rules reject it.
#
# (mnemonic, units, base, step, period)  ->  base + step * (frame % period)
CURVES = [
    ('DEPT', 'M   ', 2700.0,  0.1524, 100000),
    ('GR  ', 'GAPI',   40.0,  1.0,        60),
    ('CALI', 'IN  ',    8.5,  0.05,       10),
    ('RDEP', 'OHMM',    1.0,  0.1,        50),
    ('RMED', 'OHMM',    0.9,  0.1,        50),
    ('DEN ', 'G/CC',    2.20, 0.01,       40),
    ('NEU ', 'V/V ',    0.10, 0.005,      30),
    ('AC  ', 'US/F',   70.0,  0.5,        40),
    ('BS  ', 'IN  ',    8.5,  0.0,         1),
    ('DENC', 'G/CC',   -0.05, 0.005,      20),
    ('PEF ', 'B/E ',    1.8,  0.05,       30),
    ('ACS ', 'US/F',  120.0,  0.6,        40),
    ('SP  ', 'MV  ',  -80.0,  0.5,        60),
    ('ILD ', 'OHMM',    1.1,  0.1,        50),
    ('ILM ', 'OHMM',    1.0,  0.1,        50),
    ('SFLU', 'OHMM',    0.8,  0.1,        50),
    ('TENS', 'LBF ', 4000.0, 10.0,        80),
    ('TEMP', 'DEGC',   60.0,  0.1,       100),
    ('VSH ', 'V/V ',    0.05, 0.01,       50),
    ('PHIE', 'V/V ',    0.12, 0.004,      40),
    ('PHIT', 'V/V ',    0.15, 0.004,      40),
    ('SW  ', 'V/V ',    0.20, 0.01,       60),
    ('KLOG', 'MD  ',   10.0,  5.0,       100),
    ('NETG', 'V/V ',    0.40, 0.01,       50),
    ('RHOM', 'G/CC',    2.65, 0.001,      30),
    ('DTCO', 'US/F',   65.0,  0.4,        50),
]

FRAMES = 640

# Representation code 68 is the LIS 32-bit float, one of the four codes with
# an unambiguous published layout.
REPR_LIS_FLOAT = 68


def lis_float(value):
    """LIS 32-bit float: sign, excess-128 exponent, 23-bit fraction, no
    hidden bit. Unlike IEEE 754 the fraction is a plain binary fraction, so
    an encoder that assumes a hidden bit is out by a factor of two."""
    if value == 0.0:
        return struct.pack('>I', 0)
    sign = 0x80000000 if value < 0 else 0
    magnitude = abs(value)
    exponent = 0
    while magnitude >= 1.0:
        magnitude /= 2.0
        exponent += 1
    while magnitude < 0.5:
        magnitude *= 2.0
        exponent -= 1
    fraction = int(round(magnitude * 8388608.0)) & 0x007FFFFF
    return struct.pack('>I', sign | ((exponent + 128) << 23) | fraction)


def datum_block(mnemonic, unit, code=REPR_LIS_FLOAT, size=4, samples=1):
    """One forty-byte Datum Specification Block.

    LIS-79 numbers its fields from one, so they sit one lower here: the
    mnemonic at 0, units at 18, the block size at 28, samples per frame at 33
    and the representation code at 34. Reading units at 16 instead of 18
    turns INCH into IN, which looks like a units convention rather than a
    bug.
    """
    block = bytearray(40)
    block[0:4] = mnemonic.ljust(4)[:4].encode('ascii')
    block[18:22] = unit.ljust(4)[:4].encode('ascii')
    struct.pack_into('>H', block, 28, size)
    block[33] = samples
    block[34] = code
    return bytes(block)


def physical_records(kind, body, cap=PHYSICAL_RECORD_CAP):
    """One logical record, split across physical records when it does not fit.

    The first piece carries the type byte and sets the successor bit; every
    continuation sets the predecessor bit and carries content only.
    """
    payload = bytes([kind, 0x00]) + body
    if len(payload) + 4 <= cap:
        return struct.pack('>HH', 4 + len(payload), 0x0000) + payload

    out = bytearray()
    room = cap - 4
    pieces = [payload[i:i + room] for i in range(0, len(payload), room)]
    for index, piece in enumerate(pieces):
        attributes = 0x0000
        if index > 0:
            attributes |= PR_PREDECESSOR
        if index < len(pieces) - 1:
            attributes |= PR_SUCCESSOR
        out += struct.pack('>HH', 4 + len(piece), attributes) + piece
    return bytes(out)


def tape_image(blocks):
    """Wrap each block in Tape Image Format framing.

    A TIF header is twelve bytes: the record type, the offset of the previous
    header and the offset of the next, all little endian. Every offset has to
    be computed against the framed layout, not the unframed one.
    """
    out = bytearray()
    previous = 0
    for block in blocks:
        start = len(out)
        nxt = start + TIF_LENGTH + len(block)
        out += struct.pack('<III', 0, previous, nxt)
        out += block
        previous = start
    # The end of tape: two markers, type 1 then type 2.
    for kind in (1, 2):
        start = len(out)
        out += struct.pack('<III', kind, previous, start + TIF_LENGTH)
        previous = start
    return bytes(out)


def main():
    os.makedirs(DATA, exist_ok=True)

    spec = bytearray()
    spec += bytes([1, 1, 66, 0])      # one entry block
    spec += bytes([0, 0, 0])          # entry block terminator
    for mnemonic, unit, _, _, _ in CURVES:
        spec += datum_block(mnemonic, unit)

    frame_width = len(CURVES) * 4
    frames = bytearray()
    for frame in range(FRAMES):
        for _, _, base, step, period in CURVES:
            frames += lis_float(base + step * (frame % period))

    blocks = [physical_records(LR_FILE_HEADER,
                               b'VOLVE ARCHIVE REPROCESSED 15/9-F-4 A'.ljust(58))]
    blocks.append(physical_records(LR_FORMAT_SPEC, bytes(spec)))
    for frame in range(FRAMES):
        at = frame * frame_width
        blocks.append(physical_records(LR_DATA,
                                       bytes(frames[at:at + frame_width])))
    blocks.append(physical_records(LR_FILE_TRAILER,
                                   b'VOLVE ARCHIVE REPROCESSED 15/9-F-4 A'.ljust(58)))

    payload = tape_image(blocks)
    name = '2026-03-12_15_9-F-4A_REPROCESSED.lis'
    with open(os.path.join(DATA, name), 'wb') as handle:
        handle.write(payload)

    spec_bytes = len(spec) + 2
    print('  %s' % name)
    print('    %d bytes, tape image framed' % len(payload))
    print('    %d curves, frame width %d bytes, %d frames'
          % (len(CURVES), frame_width, FRAMES))
    print('    specification record body %d bytes, so it needs %d physical '
          'records at the %d byte cap'
          % (spec_bytes,
             (spec_bytes + PHYSICAL_RECORD_CAP - 5) // (PHYSICAL_RECORD_CAP - 4),
             PHYSICAL_RECORD_CAP))
    return 0


if __name__ == '__main__':
    sys.exit(main())
