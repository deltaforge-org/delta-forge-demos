"""Write the PRODML monthly production reports this demo reads.

Scenario: an operator reports monthly produced volumes per facility to the
regulator as PRODML, one document per facility per reporting year. The
production engineer loads them to track decline, watch the water cut, and
total the year for the annual statement.

Three facilities, delivered over two days. Their production follows a
hyperbolic decline with a rising water cut and a rising gas/oil ratio, which
is what a maturing field does: the oil rate falls, the well makes more water
every month, and gas breaks out of solution as the reservoir pressure drops.
The numbers are therefore internally consistent rather than decorative, and
the demo's water-cut crossover is a real feature of the data rather than a
value planted to be found.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

# Why the documents are shaped this way

PRODML is XML, and DeltaForge reads it through the XML engine under a curated
profile rather than a parser of its own. The profile names `//productVolume`
as the row and explodes `//period`, so one row comes out per reporting month
with the facility's own fields repeated down it. One facility per document
keeps that explosion single-valued, which is how a regulator's monthly return
is actually filed.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

# (delivery date, facility uid, facility name, first-month oil rate in m3/d,
#  monthly decline fraction, starting water cut, water cut rise per month,
#  starting gas/oil ratio in m3/m3, GOR rise per month)
FACILITIES = [
    ('2026-03-11', 'f-sleipner-a', 'SLEIPNER-A', 1850.0, 0.031, 0.28, 0.021, 148.0, 5.5),
    ('2026-03-11', 'f-gudrun-b',   'GUDRUN-B',    980.0, 0.019, 0.44, 0.017, 205.0, 3.2),
    ('2026-03-12', 'f-utgard-c',   'UTGARD-C',   2400.0, 0.042, 0.11, 0.026,  96.0, 7.8),
]

YEAR = 2026
DAYS_IN_MONTH = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def monthly_volumes(oil_rate, decline, water_cut, water_rise, gor, gor_rise):
    """Twelve months of produced volumes.

    Oil declines month on month. Water cut is the fraction of total liquid
    that is water, so the water volume follows from the oil volume and the
    cut rather than being an independent number: that is what makes the
    demo's water-cut arithmetic reproducible from the file.
    """
    rows = []
    for month in range(12):
        days = DAYS_IN_MONTH[month]
        rate = oil_rate * ((1.0 - decline) ** month)
        oil = rate * days
        cut = min(0.94, water_cut + water_rise * month)
        # cut = water / (oil + water), so water = oil * cut / (1 - cut).
        water = oil * cut / (1.0 - cut)
        gas = oil * (gor + gor_rise * month)
        rows.append({
            'month': month + 1,
            'days': days,
            'oil': oil,
            'water': water,
            'gas': gas,
            'cut': cut,
        })
    return rows


def document(uid, name, rows):
    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<productVolume '
                 'xmlns="http://www.energistics.org/energyml/data/prodmlv2">')
    parts.append('  <name>%s monthly production %d</name>' % (name, YEAR))
    parts.append('  <dTimStart>%d-01-01T00:00:00Z</dTimStart>' % YEAR)
    parts.append('  <dTimEnd>%d-12-31T23:59:59Z</dTimEnd>' % YEAR)
    parts.append('  <facility uid="%s">' % uid)
    parts.append('    <name>%s</name>' % name)
    parts.append('    <product>oil</product>')
    for row in rows:
        parts.append('    <period>')
        parts.append('      <dTimStart>%d-%02d-01T00:00:00Z</dTimStart>'
                     % (YEAR, row['month']))
        parts.append('      <volume uom="m3">%.1f</volume>' % row['oil'])
        parts.append('      <waterVolume uom="m3">%.1f</waterVolume>' % row['water'])
        parts.append('      <gasVolume uom="m3">%.1f</gasVolume>' % row['gas'])
        parts.append('    </period>')
    parts.append('  </facility>')
    parts.append('  <commonData>')
    parts.append('    <comments>Monthly allocated volumes, regulator return</comments>')
    parts.append('  </commonData>')
    parts.append('</productVolume>')
    return '\n'.join(parts) + '\n'


def main():
    os.makedirs(DATA, exist_ok=True)
    total_bytes = 0
    for (day, uid, name, rate, decline, cut, rise, gor, gor_rise) in FACILITIES:
        rows = monthly_volumes(rate, decline, cut, rise, gor, gor_rise)
        text = document(uid, name, rows)
        file_name = '%s_%s_%d.prodml' % (day, name, YEAR)
        with open(os.path.join(DATA, file_name), 'w',
                  encoding='utf-8', newline='\n') as handle:
            handle.write(text)
        total_bytes += len(text.encode('utf-8'))
        annual_oil = sum(r['oil'] for r in rows)
        annual_water = sum(r['water'] for r in rows)
        crossover = next((r['month'] for r in rows if r['cut'] > 0.5), None)
        print('  %-38s %6d bytes  12 periods  oil %9.1f m3  water %9.1f m3  '
              'cut>50%% from month %s'
              % (file_name, len(text.encode('utf-8')), annual_oil,
                 annual_water, crossover))
    print()
    print('  %d facilities, %d monthly periods in total'
          % (len(FACILITIES), len(FACILITIES) * 12))
    print('  bytes on disk %d' % total_bytes)
    return 0


if __name__ == '__main__':
    sys.exit(main())
