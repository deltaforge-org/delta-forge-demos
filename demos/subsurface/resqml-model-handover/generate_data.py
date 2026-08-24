"""Write the RESQML packages this demo audits.

Scenario: a partner delivers a static reservoir model for a unitisation study
as a RESQML package. Before the asset team loads it into the modelling
software somebody has to audit what actually arrived, and the question that
matters is not how many objects there are: it is which of them point at bulk
arrays living in a companion HDF5 file that may or may not have been
delivered alongside.

That is the classic handover failure. The .epc opens, every object is present,
every citation is filled in, and the grid has no geometry because the .h5 it
names never arrived. RESQML records the reference rather than following it, so
the dependency is visible in SQL instead of silently absent.

Two versions, delivered a day apart: the initial model and a revision that
adds a horizon, a surface, a property and a second well.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

The loose part sits in its own folder rather than beside the packages,
because a directory scan filters on the format's own extensions and a bare
`.xml` next to `.epc` files would simply not be picked up.

# What an .epc actually is

An Open Packaging Conventions ZIP: one XML part per object, plus a content
type manifest and a `_rels` tree that are packaging rather than data. The
reader opens the archive itself rather than handing it to an XML engine,
because an XML engine reads files and this is a container.

A ZIP holding XML parts is also what an .xlsx is, which is why the detector
has to look past the ZIP header: left to a generic Office Open XML check, a
reservoir model is a spreadsheet.
"""
import os
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')
PACKAGES = os.path.join(DATA, 'packages')
UNPACKED = os.path.join(DATA, 'unpacked')

ORIGINATOR = 'Nordfjell Petroleum'
CREATION = '2026-03-09T14:22:00Z'
SCHEMA_VERSION = '2.0'

CRS = '11111111-0000-0000-0000-000000000001'
HDF = '22222222-0000-0000-0000-000000000002'
EARTH_MODEL = '33333333-0000-0000-0000-000000000003'
ORGANIZATION = '33333333-0000-0000-0000-000000000004'
TOP_HUGIN = '44444444-0000-0000-0000-000000000005'
BASE_HUGIN = '44444444-0000-0000-0000-000000000006'
FAULT_F1 = '55555555-0000-0000-0000-000000000007'
GRID = '66666666-0000-0000-0000-000000000008'
TOP_SLEIPNER = '44444444-0000-0000-0000-000000000009'

# (object type, uuid, title, [referenced uuids], [external array paths])
# Referenced uuids become DataObjectReference elements; external array paths
# become PathInExternalFile elements alongside an HdfProxy reference, which is
# why every object with an array also references the HDF proxy.
BASE_OBJECTS = [
    ('obj_LocalDepth3dCrs', CRS, 'ED50 UTM 31N depth', [], []),
    ('obj_EpcExternalPartReference', HDF, 'Bulk array store', [], []),
    ('obj_OrganizationFeature', ORGANIZATION, 'Sleipner East structure', [], []),
    ('obj_EarthModelInterpretation', EARTH_MODEL, 'Static model 2026',
     [ORGANIZATION], []),
    ('obj_HorizonInterpretation', TOP_HUGIN, 'Top Hugin', [EARTH_MODEL], []),
    ('obj_HorizonInterpretation', BASE_HUGIN, 'Base Hugin', [EARTH_MODEL], []),
    ('obj_FaultInterpretation', FAULT_F1, 'Fault F-1', [EARTH_MODEL], []),
    ('obj_Grid2dRepresentation', '77777777-0000-0000-0000-00000000000a',
     'Top Hugin depth surface', [TOP_HUGIN, CRS], ['/RESQML/top_hugin/points']),
    ('obj_IjkGridRepresentation', GRID, 'Static model grid 60x48x22',
     [EARTH_MODEL, CRS], ['/RESQML/grid/points']),
    ('obj_ContinuousProperty', '88888888-0000-0000-0000-00000000000b',
     'Porosity', [GRID], ['/RESQML/grid/poro']),
    ('obj_ContinuousProperty', '88888888-0000-0000-0000-00000000000c',
     'Permeability I', [GRID], ['/RESQML/grid/permi']),
    ('obj_DiscreteProperty', '99999999-0000-0000-0000-00000000000d',
     'Facies', [GRID], ['/RESQML/grid/facies']),
    ('obj_WellboreTrajectoryRepresentation',
     'aaaaaaaa-0000-0000-0000-00000000000e', 'Trajectory 15/9-F-11',
     [CRS], ['/RESQML/wells/f11/control_points']),
    ('obj_TriangulatedSetRepresentation',
     'bbbbbbbb-0000-0000-0000-00000000000f', 'Fault F-1 surface',
     [FAULT_F1, CRS], ['/RESQML/faults/f1/triangles']),
]

REVISION_OBJECTS = [
    ('obj_HorizonInterpretation', TOP_SLEIPNER, 'Top Sleipner',
     [EARTH_MODEL], []),
    ('obj_Grid2dRepresentation', '77777777-0000-0000-0000-000000000010',
     'Top Sleipner depth surface', [TOP_SLEIPNER, CRS],
     ['/RESQML/top_sleipner/points']),
    ('obj_ContinuousProperty', '88888888-0000-0000-0000-000000000011',
     'Water saturation', [GRID], ['/RESQML/grid/sw']),
    ('obj_WellboreTrajectoryRepresentation',
     'aaaaaaaa-0000-0000-0000-000000000012', 'Trajectory 15/9-F-12',
     [CRS], ['/RESQML/wells/f12/control_points']),
]


def crc32(data):
    table = []
    for index in range(256):
        value = index
        for _ in range(8):
            value = (0xEDB88320 ^ (value >> 1)) if value & 1 else (value >> 1)
        table.append(value)
    crc = 0xFFFFFFFF
    for byte in data:
        crc = table[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def zip_stored(parts):
    """A ZIP with no compression, which an OPC package is allowed to be and
    which keeps the archive readable byte for byte."""
    data = bytearray()
    directory = bytearray()
    for name, content in parts:
        payload = content.encode('utf-8')
        offset = len(data)
        checksum = crc32(payload)
        data += struct.pack('<IHHHHHIIIHH', 0x04034b50, 20, 0, 0, 0, 0,
                            checksum, len(payload), len(payload),
                            len(name), 0)
        data += name.encode('ascii') + payload
        directory += struct.pack('<IHHHHHHIIIHHHHHII', 0x02014b50, 20, 20, 0,
                                 0, 0, 0, checksum, len(payload),
                                 len(payload), len(name), 0, 0, 0, 0, 0,
                                 offset)
        directory += name.encode('ascii')
    directory_offset = len(data)
    data += directory
    data += struct.pack('<IHHHHIIH', 0x06054b50, 0, 0, len(parts), len(parts),
                        len(directory), directory_offset, 0)
    return bytes(data)


def part(object_type, uuid, title, references, arrays):
    """One RESQML object part."""
    lines = []
    lines.append('<?xml version="1.0" encoding="UTF-8"?>')
    lines.append('<resqml2:%s' % object_type)
    lines.append('    xmlns:resqml2="http://www.energistics.org/energyml/data/resqmlv2"')
    lines.append('    xmlns:eml="http://www.energistics.org/energyml/data/commonv2"')
    lines.append('    schemaVersion="%s"' % SCHEMA_VERSION)
    lines.append('    uuid="%s">' % uuid)
    lines.append('  <eml:Citation>')
    lines.append('    <eml:Title>%s</eml:Title>' % title)
    lines.append('    <eml:Originator>%s</eml:Originator>' % ORIGINATOR)
    lines.append('    <eml:Creation>%s</eml:Creation>' % CREATION)
    lines.append('  </eml:Citation>')
    for referenced in references:
        lines.append('  <resqml2:RepresentedInterpretation>')
        lines.append('    <eml:DataObjectReference uuid="%s"/>' % referenced)
        lines.append('  </resqml2:RepresentedInterpretation>')
    for path in arrays:
        lines.append('  <resqml2:Geometry>')
        lines.append('    <resqml2:Points>')
        lines.append('      <eml:PathInExternalFile>%s</eml:PathInExternalFile>'
                     % path)
        lines.append('      <eml:HdfProxy uuid="%s"/>' % HDF)
        lines.append('    </resqml2:Points>')
        lines.append('  </resqml2:Geometry>')
    lines.append('</resqml2:%s>' % object_type)
    return '\n'.join(lines) + '\n'


def package(objects):
    parts = [
        ('[Content_Types].xml',
         '<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org'
         '/package/2006/content-types"><Default Extension="xml" '
         'ContentType="application/x-resqml+xml"/></Types>'),
        ('_rels/.rels', '<?xml version="1.0"?><Relationships '
                        'xmlns="http://schemas.openxmlformats.org/package/2006'
                        '/relationships"/>'),
    ]
    for index, (kind, uuid, title, references, arrays) in enumerate(objects):
        parts.append(('%s_%d.xml' % (kind, index + 1),
                      part(kind, uuid, title, references, arrays)))
    return zip_stored(parts)


def main():
    os.makedirs(PACKAGES, exist_ok=True)
    os.makedirs(UNPACKED, exist_ok=True)
    total = 0

    versions = [
        ('2026-03-11', 'static_model_v1', BASE_OBJECTS),
        ('2026-03-12', 'static_model_v2', BASE_OBJECTS + REVISION_OBJECTS),
    ]
    for day, name, objects in versions:
        payload = package(objects)
        file_name = '%s_%s.epc' % (day, name)
        with open(os.path.join(PACKAGES, file_name), 'wb') as handle:
            handle.write(payload)
        total += len(payload)
        with_arrays = sum(1 for o in objects if o[4])
        print('  %-36s %6d bytes  %2d objects, %d referencing external arrays'
              % (file_name, len(payload), len(objects), with_arrays))

    # A loose part, which is how RESQML arrives once a package is unpacked.
    loose = part('obj_Grid2dRepresentation',
                 'cccccccc-0000-0000-0000-000000000013',
                 'Top Hugin depth surface, unpacked',
                 [TOP_HUGIN, CRS], ['/RESQML/top_hugin/points'])
    loose_name = '2026-03-12_top_hugin_unpacked.xml'
    with open(os.path.join(UNPACKED, loose_name), 'w',
              encoding='utf-8', newline='\n') as handle:
        handle.write(loose)
    total += len(loose.encode('utf-8'))
    print('  %-36s %6d bytes  1 object, unpacked from a package'
          % (loose_name, len(loose.encode('utf-8'))))

    print()
    print('  bytes on disk %d' % total)
    return 0


if __name__ == '__main__':
    sys.exit(main())
