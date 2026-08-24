"""Write the WITSML definitive surveys this demo reads.

Scenario: a directional driller hands over the definitive survey at the end of
each bit run, as WITSML. The well engineer needs those trajectories in the
lake to run anti-collision checks against the wells already drilled from the
same platform, and to tie each well to the geological model.

Three wells from one template, delivered over two days. The trajectories are
computed with the minimum-curvature method, which is what a real survey
company uses, so the true vertical depths and the north/east offsets are
internally consistent rather than decorative: a reader that mangles a station
produces a well path that does not close.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

# Why the documents are shaped this way

WITSML is XML, and DeltaForge reads it through the XML engine under a curated
profile rather than a parser of its own. The profile names `//well` as the row
and explodes `//trajectoryStation`, so one row comes out per survey station
with the well's own fields repeated down it. Element names are the standard's,
including the camelCase ones, because the profile matches on them.
"""
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

# The three wells are drilled from one platform template, so they start a few
# metres apart at surface and diverge with depth. That surface offset is the
# whole reason an anti-collision check exists: the wells are closest where
# they leave the template, not where they land.
#
# (delivery date, well uid, well name, slot north, slot east, kick-off depth,
#  build rate deg/30m, final inclination, azimuth, station count)
WELLS = [
    ('2026-03-11', 'w-15-9-F-11',  '15/9-F-11',   0.0, 0.0, 520.0, 2.4, 62.0, 135.0, 40),
    ('2026-03-11', 'w-15-9-F-11A', '15/9-F-11 A', 0.0, 4.5, 610.0, 3.0, 74.0, 210.0, 32),
    ('2026-03-12', 'w-15-9-F-12',  '15/9-F-12',   4.5, 0.0, 480.0, 1.8, 48.0,  22.0, 48),
]

STATION_SPACING = 30.0
FIELD = 'VOLVE-DEMO'
OPERATOR = 'DeltaForge Energy'


def stations(slot_north, slot_east, kick_off, build_rate, final_inclination,
             azimuth, count):
    """Minimum-curvature survey stations from a build-and-hold profile.

    Inclination builds at `build_rate` degrees per 30 m from the kick-off
    point until it reaches `final_inclination`, then holds. Each station's
    true vertical depth and north/east offsets are integrated from the one
    before it, which is what makes the path close.
    """
    rows = []
    md = kick_off - 4 * STATION_SPACING
    inclination = 0.0
    tvd = md
    north = slot_north
    east = slot_east

    for index in range(count):
        if index > 0:
            previous_inclination = inclination
            if md >= kick_off:
                inclination = min(final_inclination,
                                  inclination + build_rate)
            mean = math.radians((previous_inclination + inclination) / 2.0)
            tvd += STATION_SPACING * math.cos(mean)
            horizontal = STATION_SPACING * math.sin(mean)
            north += horizontal * math.cos(math.radians(azimuth))
            east += horizontal * math.sin(math.radians(azimuth))
        rows.append({
            'index': index,
            'md': md,
            'tvd': tvd,
            'incl': inclination,
            'azi': azimuth if inclination > 0.0 else 0.0,
            'north': north,
            'east': east,
        })
        md += STATION_SPACING
    return rows


def document(uid, name, survey):
    parts = []
    parts.append('<?xml version="1.0" encoding="UTF-8"?>')
    parts.append('<wells xmlns="http://www.witsml.org/schemas/1series" '
                 'version="1.4.1.1">')
    parts.append('  <well uid="%s">' % uid)
    parts.append('    <name>%s</name>' % name)
    parts.append('    <field>%s</field>' % FIELD)
    parts.append('    <country>NO</country>')
    parts.append('    <operator>%s</operator>' % OPERATOR)
    parts.append('    <trajectory uid="%s-def">' % uid)
    parts.append('      <name>Definitive Survey</name>')
    for station in survey:
        parts.append('      <trajectoryStation uid="%s-ts-%03d">'
                     % (uid, station['index']))
        parts.append('        <md uom="m">%.2f</md>' % station['md'])
        parts.append('        <tvd uom="m">%.2f</tvd>' % station['tvd'])
        parts.append('        <incl uom="dega">%.2f</incl>' % station['incl'])
        parts.append('        <azi uom="dega">%.2f</azi>' % station['azi'])
        parts.append('        <dispNs uom="m">%.2f</dispNs>' % station['north'])
        parts.append('        <dispEw uom="m">%.2f</dispEw>' % station['east'])
        parts.append('      </trajectoryStation>')
    parts.append('    </trajectory>')
    parts.append('    <commonData>')
    parts.append('      <comments>Definitive survey, minimum curvature</comments>')
    parts.append('    </commonData>')
    parts.append('  </well>')
    parts.append('</wells>')
    return '\n'.join(parts) + '\n'


def main():
    os.makedirs(DATA, exist_ok=True)
    total = 0
    total_stations = 0
    for (day, uid, name, slot_n, slot_e, kick_off, build, final, azimuth,
         count) in WELLS:
        survey = stations(slot_n, slot_e, kick_off, build, final, azimuth,
                          count)
        text = document(uid, name, survey)
        file_name = '%s_%s.witsml' % (day, name.replace('/', '_').replace(' ', ''))
        with open(os.path.join(DATA, file_name), 'w',
                  encoding='utf-8', newline='\n') as handle:
            handle.write(text)
        total += len(text.encode('utf-8'))
        total_stations += count
        print('  %-34s %7d bytes  %2d stations  md %.0f to %.0f m, final incl %.1f'
              % (file_name, len(text.encode('utf-8')), count,
                 survey[0]['md'], survey[-1]['md'], survey[-1]['incl']))
    print()
    print('  %d wells, %d survey stations in total' % (len(WELLS), total_stations))
    print('  bytes on disk %d' % total)
    return 0


if __name__ == '__main__':
    sys.exit(main())
