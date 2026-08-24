"""Write the GeoJSON licence-block awards this demo reads.

Scenario: a licensing authority publishes the blocks awarded in each round as
GeoJSON. An operator loads them to track the acreage it holds, and more
importantly the relinquishment obligations attached to it: a licence typically
requires half the area back after a fixed term, and missing that date costs
the whole block.

Two rounds, delivered a day apart. The blocks are laid out on the real
Norwegian quadrant grid, one degree of latitude by two of longitude divided
into twelve, so a block is a genuine graticule rectangle rather than an
arbitrary polygon.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

# Why the documents are shaped this way

GeoJSON is JSON, and DeltaForge reads it through the JSON engine under a
curated profile rather than a parser of its own. The profile takes the
features array as the row source and keeps `geometry` whole: flattening a
polygon's coordinate array would produce a column per vertex, which is not a
schema, it is a rash.
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

# A Norwegian quadrant is one degree of latitude by two of longitude, split
# into a 4 x 3 grid of twelve blocks. Block 1 is the north-west corner and
# numbering runs east then south, which is why the arithmetic below is not
# simply row-major.
QUADRANT_HEIGHT = 1.0
QUADRANT_WIDTH = 2.0
BLOCKS_ACROSS = 3
BLOCKS_DOWN = 4

# (delivery date, round, quadrant, south latitude, west longitude, awards)
# An award is (block number, licence, operator, term years, work commitment).
ROUNDS = [
    ('2026-03-11', 'APA-2025', 15, 58.0, 1.0, [
        (3,  'PL-1201', 'DeltaForge Energy',    6, 'One firm well'),
        (6,  'PL-1201', 'DeltaForge Energy',    6, 'One firm well'),
        (9,  'PL-1204', 'Nordfjell Petroleum',  4, 'Seismic reprocessing'),
        (12, 'PL-1207', 'DeltaForge Energy',    6, 'One firm well'),
    ]),
    ('2026-03-11', 'APA-2025', 16, 58.0, 3.0, [
        (1,  'PL-1210', 'Havlys Exploration',   4, 'Seismic reprocessing'),
        (2,  'PL-1210', 'Havlys Exploration',   4, 'Seismic reprocessing'),
        (7,  'PL-1213', 'DeltaForge Energy',    6, 'Two firm wells'),
    ]),
    ('2026-03-12', 'APA-2026', 25, 59.0, 1.0, [
        (4,  'PL-1301', 'DeltaForge Energy',    6, 'One firm well'),
        (5,  'PL-1301', 'DeltaForge Energy',    6, 'One firm well'),
        (8,  'PL-1305', 'Nordfjell Petroleum',  4, 'Seismic reprocessing'),
        (10, 'PL-1308', 'Havlys Exploration',   6, 'One firm well'),
        (11, 'PL-1308', 'Havlys Exploration',   6, 'One firm well'),
    ]),
]

AWARD_YEAR = {'APA-2025': 2025, 'APA-2026': 2026}

# One degree of latitude is 111.32 km; a degree of longitude shrinks with the
# cosine of latitude. At 58 to 60 north the blocks are about 490 square
# kilometres, which is the right order for a Norwegian block.
KM_PER_DEGREE_LAT = 111.32


def block_bounds(number, south, west):
    """The graticule rectangle for a block number within its quadrant."""
    index = number - 1
    row = index // BLOCKS_ACROSS          # 0 at the north edge
    column = index % BLOCKS_ACROSS        # 0 at the west edge
    block_height = QUADRANT_HEIGHT / BLOCKS_DOWN
    block_width = QUADRANT_WIDTH / BLOCKS_ACROSS
    north_edge = south + QUADRANT_HEIGHT - row * block_height
    return {
        'west': west + column * block_width,
        'east': west + (column + 1) * block_width,
        'north': north_edge,
        'south': north_edge - block_height,
    }


def area_km2(bounds):
    """Area of a graticule rectangle, to the nearest square kilometre."""
    import math
    mean_latitude = math.radians((bounds['north'] + bounds['south']) / 2.0)
    height = (bounds['north'] - bounds['south']) * KM_PER_DEGREE_LAT
    width = ((bounds['east'] - bounds['west']) * KM_PER_DEGREE_LAT
             * math.cos(mean_latitude))
    return round(height * width, 1)


def feature(round_name, quadrant, number, licence, operator, term, commitment,
            south, west, identifier):
    bounds = block_bounds(number, south, west)
    ring = [
        [bounds['west'], bounds['south']],
        [bounds['east'], bounds['south']],
        [bounds['east'], bounds['north']],
        [bounds['west'], bounds['north']],
        [bounds['west'], bounds['south']],
    ]
    awarded = AWARD_YEAR[round_name]
    return {
        'type': 'Feature',
        'id': identifier,
        'geometry': {'type': 'Polygon', 'coordinates': [ring]},
        'properties': {
            'block': '%d/%d' % (quadrant, number),
            'quadrant': quadrant,
            'licence': licence,
            'operator': operator,
            'licence_round': round_name,
            'awarded_year': awarded,
            'term_years': term,
            'relinquish_by': awarded + term,
            'work_commitment': commitment,
            'area_km2': area_km2(bounds),
        },
    }


def main():
    os.makedirs(DATA, exist_ok=True)
    by_file = {}
    identifier = 1
    for day, round_name, quadrant, south, west, awards in ROUNDS:
        name = '%s_%s_quadrant_%d.geojson' % (day, round_name, quadrant)
        features = []
        for number, licence, operator, term, commitment in awards:
            features.append(feature(round_name, quadrant, number, licence,
                                    operator, term, commitment, south, west,
                                    identifier))
            identifier += 1
        by_file[name] = {'type': 'FeatureCollection', 'features': features}

    total = 0
    blocks = 0
    for name, collection in by_file.items():
        text = json.dumps(collection, indent=2) + '\n'
        with open(os.path.join(DATA, name), 'w',
                  encoding='utf-8', newline='\n') as handle:
            handle.write(text)
        total += len(text.encode('utf-8'))
        blocks += len(collection['features'])
        acreage = sum(f['properties']['area_km2'] for f in collection['features'])
        print('  %-44s %6d bytes  %d blocks  %.1f km2'
              % (name, len(text.encode('utf-8')), len(collection['features']),
                 acreage))
    print()
    print('  %d files, %d blocks, %d bytes' % (len(by_file), blocks, total))
    return 0


if __name__ == '__main__':
    sys.exit(main())
