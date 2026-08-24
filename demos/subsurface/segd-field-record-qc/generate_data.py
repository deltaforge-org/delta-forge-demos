"""Write the SEG-D field records this demo reads.

Scenario: a land seismic crew in the Delaware Basin records one SEG-D field
record per vibroseis sweep point and hands the day's records to the QC desk
before they go to the processing centre. Six records are written here, and two
of them carry the faults a QC desk exists to find:

  - fr_1044 recorded 18 channels instead of 24, because a receiver line was
    disconnected part way through the swath;
  - fr_1045 came off the crew's second recording truck, which was still
    configured for a 2 ms sample interval and channel set 2 while every other
    record is 4 ms on channel set 1. Merging those into one processing project
    without noticing is a day of reprocessing.

Everything here is written from a literal description, so the counts the demo
asserts are known before any reader sees the bytes. The generator is
deterministic: rerunning it produces byte identical files.

Run from anywhere:

    python generate_data.py
"""
import math
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

# General header byte 22 holds the base scan interval in sixteenths of a
# millisecond, so an interval in microseconds is interval * 16 / 1000.
GENERAL_HEADER_BYTES = 32
SCAN_TYPE_HEADER_BYTES = 32
TRACE_HEADER_BYTES = 20

# SEG-D sample format codes.
FORMAT_INT16 = 8048

# The crew drops one folder of field records per acquisition day, and the
# acquisition date is part of every file name because that is what a scheduled
# loader keys its run on.
#
# (acquisition date, file number, traces, samples, interval us, scan type,
#  channel set, note)
RECORDS = [
    ('2026-03-11', 1041, 24, 512, 4000, 1, 1, 'nominal swath, 24 live channels'),
    ('2026-03-11', 1042, 24, 512, 4000, 1, 1, 'nominal swath, 24 live channels'),
    ('2026-03-11', 1043, 24, 512, 4000, 1, 1, 'nominal swath, 24 live channels'),
    ('2026-03-12', 1044, 18, 512, 4000, 1, 1, 'receiver line 3 down: 18 channels'),
    ('2026-03-12', 1045, 24, 1024, 2000, 1, 2, 'second truck, 2 ms, channel set 2'),
    ('2026-03-12', 1046, 24, 512, 4000, 1, 1, 'nominal swath, 24 live channels'),
]


class Rng:
    """A linear congruential generator, so regeneration is byte identical."""

    def __init__(self, seed):
        self.state = seed & 0xFFFFFFFF

    def next(self):
        self.state = (1103515245 * self.state + 12345) & 0x7FFFFFFF
        return self.state

    def uniform(self, low, high):
        return low + (high - low) * (self.next() / 0x7FFFFFFF)


def set_bcd(buffer, start_nibble, value, digits):
    """Write `digits` binary-coded-decimal digits from a nibble index.

    Nibble n is the high half of byte n // 2 when n is even and the low half
    when it is odd. A multi-digit field written into the wrong half shifts
    every digit and yields a plausible but wrong number, so the halves are
    handled explicitly rather than by byte arithmetic.
    """
    text = str(int(value)).rjust(digits, '0')[-digits:]
    for offset, character in enumerate(text):
        nibble = start_nibble + offset
        index = nibble // 2
        digit = int(character)
        if nibble % 2 == 0:
            buffer[index] = (buffer[index] & 0x0F) | (digit << 4)
        else:
            buffer[index] = (buffer[index] & 0xF0) | digit


def field_record(file_number, traces, samples, interval_us, scan_type,
                 channel_set, seed):
    """One SEG-D field record: general header, scan type header, then traces."""
    general = bytearray(GENERAL_HEADER_BYTES)
    set_bcd(general, 0, file_number, 4)        # file number, nibbles 0-3
    set_bcd(general, 4, FORMAT_INT16, 4)       # format code, nibbles 4-7
    general[11] = 0x00                         # no additional general headers
    general[22] = interval_us * 16 // 1000     # base scan interval

    # Record length is in units of 1.024 seconds at nibbles 50-52. The reader
    # derives the sample count back out of it, so this has to round trip
    # exactly: samples = units * 1_024_000 / interval_us.
    units = samples * interval_us // 1_024_000
    assert units * 1_024_000 // interval_us == samples, (
        'record length does not round trip for %d samples at %d us'
        % (samples, interval_us)
    )
    set_bcd(general, 50, units, 3)
    set_bcd(general, 56, 1, 2)                 # one channel set descriptor

    data = bytearray(general)
    data += bytearray(SCAN_TYPE_HEADER_BYTES)

    rng = Rng(seed)
    for trace in range(traces):
        header = bytearray(TRACE_HEADER_BYTES)
        set_bcd(header, 0, file_number, 4)
        set_bcd(header, 4, scan_type, 2)
        set_bcd(header, 6, channel_set, 2)
        set_bcd(header, 8, trace + 1, 4)
        data += header

        # A vibroseis correlated sweep: a decaying wavelet at the first break
        # whose arrival walks out with offset, plus low-level ambient noise.
        first_break = 40 + trace * 3
        for sample in range(samples):
            lag = sample - first_break
            amplitude = rng.uniform(-40, 40)
            if lag >= 0:
                amplitude += (
                    math.sin(2 * math.pi * lag / 18.0)
                    * math.exp(-lag / 90.0)
                    * 9000
                )
            data += struct.pack('>h', max(-32768, min(32767, int(amplitude))))
    return bytes(data)


def main():
    os.makedirs(DATA, exist_ok=True)
    total_traces = 0
    for (day, number, traces, samples, interval_us, scan_type, channel_set,
         note) in RECORDS:
        payload = field_record(number, traces, samples, interval_us,
                               scan_type, channel_set, seed=number * 7)
        name = '%s_fr_%d.segd' % (day, number)
        with open(os.path.join(DATA, name), 'wb') as handle:
            handle.write(payload)
        total_traces += traces
        print('  %-28s %8d bytes  %3d traces  %4d samples @ %d us  (%s)'
              % (name, len(payload), traces, samples, interval_us, note))
    print()
    print('  %d field records, %d traces in total' % (len(RECORDS), total_traces))
    return 0


if __name__ == '__main__':
    sys.exit(main())
