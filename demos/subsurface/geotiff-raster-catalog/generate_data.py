"""Write the GeoTIFF tiles this demo catalogues.

Scenario: an environmental baseline survey delivers orthophoto and elevation
tiles for a licence area. Before anything is processed the GIS team has to
know what arrived: how much ground it covers, at what resolution, and whether
every tile is in the same coordinate system. Answering that by opening the
images means reading gigabytes; answering it from the tag directories means
reading kilobytes.

Six tiles over two days. Two of them carry the faults a catalogue exists to
find:

  - one tile was delivered in UTM zone 14N while the rest of the survey is in
    zone 13N, so it plots several hundred kilometres east of where it belongs;
  - one tile is at half the resolution of the others, which is invisible in a
    file listing and obvious in the tag directory.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

# What is actually written

A TIFF is a header pointing at a chain of image file directories. Each
directory is a list of tags and a pointer to the next, and an overview
pyramid is simply more directories in the same file: full resolution first,
then each halving. This generator writes three directories per tile and
chains them, because a reader that follows only the first sees a third of
what the file describes.

No pixel data is written at all, which is the point: the catalogue reads the
tag directories and never touches a raster.
"""
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

# TIFF tags this generator writes.
TAG_IMAGE_WIDTH = 256
TAG_IMAGE_LENGTH = 257
TAG_BITS_PER_SAMPLE = 258
TAG_COMPRESSION = 259
TAG_SAMPLES_PER_PIXEL = 277
TAG_TILE_WIDTH = 322
TAG_TILE_LENGTH = 323
TAG_MODEL_PIXEL_SCALE = 33550
TAG_MODEL_TIEPOINT = 33922
TAG_GEO_KEY_DIRECTORY = 34735

TYPE_SHORT = 3
TYPE_LONG = 4
TYPE_DOUBLE = 12

TYPE_SIZE = {TYPE_SHORT: 2, TYPE_LONG: 4, TYPE_DOUBLE: 8}

COMPRESSION_DEFLATE = 8
TILE_SIZE = 512

# (delivery date, tile name, width, height, pixel scale, EPSG, bands,
#  bits per sample, easting of north-west corner, northing, BigTIFF, note)
TILES = [
    ('2026-03-11', 'ortho_n01e01', 20000, 20000, 0.25, 32613, 3, 8,
     512000.0, 3548000.0, False, 'nominal'),
    ('2026-03-11', 'ortho_n01e02', 20000, 20000, 0.25, 32613, 3, 8,
     517000.0, 3548000.0, False, 'nominal'),
    ('2026-03-11', 'ortho_n02e01', 20000, 20000, 0.25, 32613, 3, 8,
     512000.0, 3543000.0, False, 'nominal'),
    ('2026-03-12', 'ortho_n02e02', 20000, 20000, 0.25, 32614, 3, 8,
     517000.0, 3543000.0, False, 'WRONG ZONE: 14N, survey is 13N'),
    ('2026-03-12', 'ortho_n03e01', 10000, 10000, 0.50, 32613, 3, 8,
     512000.0, 3538000.0, False, 'half resolution'),
    ('2026-03-12', 'dem_n01e01',   40000, 40000, 0.25, 32613, 1, 32,
     512000.0, 3548000.0, True,  'elevation model, BigTIFF, 32-bit float'),
]

OVERVIEW_LEVELS = 3


def entry(tag, kind, count, payload, big):
    """One directory entry, with its value inline when it fits."""
    inline_bytes = 8 if big else 4
    size = TYPE_SIZE[kind] * count
    out = struct.pack('<HH', tag, kind)
    out += struct.pack('<Q' if big else '<I', count)
    if size <= inline_bytes:
        # Inline values are left justified in the field.
        raw = payload.ljust(inline_bytes, b'\x00')[:inline_bytes]
        out += raw
    else:
        out += struct.pack('<Q' if big else '<I', 0)   # patched later
    return out


def shorts(values):
    return b''.join(struct.pack('<H', v) for v in values)


def longs(values):
    return b''.join(struct.pack('<I', v) for v in values)


def doubles(values):
    return b''.join(struct.pack('<d', v) for v in values)


def build(tile, big):
    """Assemble a TIFF with one directory per overview level."""
    (_, _, width, height, scale, epsg, bands, bits, east, north, _, _) = tile

    header_size = 16 if big else 8
    entry_size = 20 if big else 12
    count_size = 8 if big else 2
    offset_size = 8 if big else 4
    pack_offset = '<Q' if big else '<I'

    # Each level halves the raster and doubles the ground sample distance.
    levels = []
    for level in range(OVERVIEW_LEVELS):
        levels.append({
            'width': width >> level,
            'height': height >> level,
            'scale': scale * (2 ** level),
        })

    # Two passes: lay out to learn the offsets, then emit with them patched.
    # An overview chain cannot be written in one pass because a directory has
    # to name the offset of the one after it.
    directories = []
    cursor = header_size
    for level in levels:
        out_of_line = [
            (TAG_MODEL_PIXEL_SCALE, TYPE_DOUBLE, 3,
             doubles([level['scale'], level['scale'], 0.0])),
            (TAG_MODEL_TIEPOINT, TYPE_DOUBLE, 6,
             doubles([0.0, 0.0, 0.0, east, north, 0.0])),
            (TAG_GEO_KEY_DIRECTORY, TYPE_SHORT, 8,
             shorts([1, 1, 0, 1, 3072, 0, 1, epsg])),
        ]
        inline = [
            (TAG_IMAGE_WIDTH, TYPE_LONG, 1, longs([level['width']])),
            (TAG_IMAGE_LENGTH, TYPE_LONG, 1, longs([level['height']])),
            (TAG_BITS_PER_SAMPLE, TYPE_SHORT, 1, shorts([bits])),
            (TAG_COMPRESSION, TYPE_SHORT, 1, shorts([COMPRESSION_DEFLATE])),
            (TAG_SAMPLES_PER_PIXEL, TYPE_SHORT, 1, shorts([bands])),
            (TAG_TILE_WIDTH, TYPE_SHORT, 1, shorts([TILE_SIZE])),
            (TAG_TILE_LENGTH, TYPE_SHORT, 1, shorts([TILE_SIZE])),
        ]
        # Tags must be written in ascending order, which the standard requires
        # and some readers rely on for a binary search.
        tags = sorted(inline + out_of_line, key=lambda t: t[0])
        entries_at = cursor
        values_at = entries_at + count_size + len(tags) * entry_size + offset_size
        placements = {}
        at = values_at
        for tag, kind, count, payload in tags:
            if TYPE_SIZE[kind] * count > (8 if big else 4):
                placements[tag] = at
                at += len(payload)
        directories.append({
            'tags': tags, 'entries_at': entries_at,
            'placements': placements, 'end': at,
        })
        cursor = at

    data = bytearray()
    data += b'II'
    if big:
        data += struct.pack('<H', 43)
        data += struct.pack('<H', 8)      # offset size
        data += struct.pack('<H', 0)      # reserved
        data += struct.pack('<Q', directories[0]['entries_at'])
    else:
        data += struct.pack('<H', 42)
        data += struct.pack('<I', directories[0]['entries_at'])

    for index, directory in enumerate(directories):
        assert len(data) == directory['entries_at'], (
            'directory %d starts at %d, expected %d'
            % (index, len(data), directory['entries_at']))
        data += struct.pack('<Q' if big else '<H', len(directory['tags']))
        for tag, kind, count, payload in directory['tags']:
            raw = entry(tag, kind, count, payload, big)
            if tag in directory['placements']:
                raw = raw[:-offset_size] + struct.pack(
                    pack_offset, directory['placements'][tag])
            data += raw
        nxt = directories[index + 1]['entries_at'] if index + 1 < len(directories) else 0
        data += struct.pack(pack_offset, nxt)
        for tag, kind, count, payload in directory['tags']:
            if tag in directory['placements']:
                assert len(data) == directory['placements'][tag]
                data += payload
    return bytes(data), levels


def main():
    os.makedirs(DATA, exist_ok=True)
    total = 0
    for tile in TILES:
        day, name, width, height, scale, epsg, bands, bits, e, n, big, note = tile
        payload, levels = build(tile, big)
        file_name = '%s_%s.tif' % (day, name)
        with open(os.path.join(DATA, file_name), 'wb') as handle:
            handle.write(payload)
        total += len(payload)
        ground = width * scale
        print('  %-34s %5d bytes  %5dx%-5d @ %.2f m  EPSG %d  %d dirs  %.0f m across  (%s)'
              % (file_name, len(payload), width, height, scale, epsg,
                 len(levels), ground, note))
    print()
    print('  %d tiles, %d image directories, %d bytes on disk'
          % (len(TILES), len(TILES) * OVERVIEW_LEVELS, total))
    print('  no pixel data is written: the catalogue reads tag directories only')
    return 0


if __name__ == '__main__':
    sys.exit(main())
