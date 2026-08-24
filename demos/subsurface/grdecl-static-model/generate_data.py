"""Write the GRDECL static-model decks this demo reads.

Scenario: a geomodeller exports the static reservoir model as an ECLIPSE
GRDECL deck and hands it to the simulation team. The deck is plain text, and
the simulation team loads it to check the property distributions before
building a run from it: is the porosity sensible layer by layer, does the
permeability follow it, and how many cells does the model actually solve.

Two versions over two days. The revision adds a net-to-gross property, which
is the usual next step once someone asks why the volumes look optimistic.

Deterministic: rerunning produces byte identical files.

    python generate_data.py

# The format, and the thing that goes wrong in it

A GRDECL deck is a keyword followed by numbers followed by a slash. The
numbers use RUN-LENGTH ENCODING: `720*0.18` means seven hundred and twenty
cells of 0.18, not one cell holding the string. A deck of 7200 cells is
routinely a few hundred tokens, and a reader that does not expand the
repeats produces a model with a few hundred cells and no error.

This generator deliberately writes long runs, so the token count and the cell
count are wildly different numbers and the expansion is actually exercised.
Comments (`--`) are scattered through the deck for the same reason.
"""
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data', 'landing')

NI, NJ, NK = 30, 24, 10
CELLS = NI * NJ * NK

# Layer properties. Porosity falls with depth and permeability follows it,
# which is the relationship a static model is built on rather than a
# coincidence. Values are constant within a layer so the run-length encoding
# has something long to encode.
LAYERS = [
    # (porosity, permeability mD, saturation region, net-to-gross)
    (0.263, 1850.0, 1, 0.94),
    (0.251, 1420.0, 1, 0.92),
    (0.244, 1180.0, 1, 0.90),
    (0.229,  830.0, 1, 0.86),
    (0.218,  640.0, 2, 0.81),
    (0.203,  430.0, 2, 0.74),
    (0.191,  310.0, 2, 0.68),
    (0.176,  190.0, 2, 0.59),
    (0.162,  115.0, 3, 0.47),
    (0.148,   72.0, 3, 0.38),
]


def actnum():
    """Which cells the simulator solves.

    The model is cut to a fault block, so a wedge on the eastern flank of the
    top two layers is outside it. Everything else is active.
    """
    flags = []
    for k in range(NK):
        for j in range(NJ):
            for i in range(NI):
                cut = k < 2 and i >= NI - 4 and j >= NJ - 6
                flags.append(0 if cut else 1)
    return flags


def layer_values(index):
    """One value per cell, constant within each layer."""
    return [LAYERS[k][index] for k in range(NK) for _ in range(NI * NJ)]


def run_length(values, fmt):
    """Collapse consecutive equal values into `count*value`.

    This is what makes a GRDECL deck small, and what a reader has to undo.
    """
    tokens = []
    run_value = values[0]
    run_length_count = 1
    for value in values[1:]:
        if value == run_value:
            run_length_count += 1
        else:
            tokens.append((run_length_count, run_value))
            run_value = value
            run_length_count = 1
    tokens.append((run_length_count, run_value))

    out = []
    for count, value in tokens:
        text = fmt % value
        out.append('%d*%s' % (count, text) if count > 1 else text)
    return out


def keyword_block(name, tokens, comment, per_line=6):
    lines = ['-- %s' % comment, name]
    for start in range(0, len(tokens), per_line):
        lines.append('  ' + '  '.join(tokens[start:start + per_line]))
    lines.append('/')
    lines.append('')
    return lines, len(tokens)


def deck(with_ntg):
    lines = []
    lines.append('-- ' + '=' * 68)
    lines.append('-- Static reservoir model, corner point grid')
    lines.append('-- Exported for the simulation team')
    lines.append('-- ' + '=' * 68)
    lines.append('')
    lines.append('SPECGRID')
    lines.append('  %d %d %d 1 F /' % (NI, NJ, NK))
    lines.append('')

    token_counts = {}

    block, count = keyword_block(
        'ACTNUM', run_length(actnum(), '%d'),
        'Cells the simulator solves. A wedge on the eastern flank of the '
        'top two layers is outside the fault block.')
    lines += block
    token_counts['ACTNUM'] = count

    block, count = keyword_block(
        'PORO', run_length(layer_values(0), '%.3f'),
        'Porosity, constant within each layer and falling with depth.')
    lines += block
    token_counts['PORO'] = count

    block, count = keyword_block(
        'PERMX', run_length(layer_values(1), '%.1f'),
        'Horizontal permeability, following porosity.')
    lines += block
    token_counts['PERMX'] = count

    block, count = keyword_block(
        'SATNUM', run_length([float(v) for v in layer_values(2)], '%.0f'),
        'Saturation regions: three of them, stacked.')
    lines += block
    token_counts['SATNUM'] = count

    if with_ntg:
        block, count = keyword_block(
            'NTG', run_length(layer_values(3), '%.2f'),
            'Net to gross, added in the revision after the volumes looked '
            'optimistic.')
        lines += block
        token_counts['NTG'] = count

    return '\n'.join(lines) + '\n', token_counts


def main():
    os.makedirs(DATA, exist_ok=True)
    active = sum(actnum())
    total = 0

    for day, name, with_ntg in [
        ('2026-03-11', 'static_model_v1', False),
        ('2026-03-12', 'static_model_v2', True),
    ]:
        text, token_counts = deck(with_ntg)
        file_name = '%s_%s.grdecl' % (day, name)
        with open(os.path.join(DATA, file_name), 'w',
                  encoding='utf-8', newline='\n') as handle:
            handle.write(text)
        total += len(text.encode('utf-8'))
        tokens = sum(token_counts.values())
        print('  %-38s %6d bytes  %d keywords, %d tokens for %d cells each'
              % (file_name, len(text.encode('utf-8')), len(token_counts),
                 tokens, CELLS))

    print()
    print('  grid %d x %d x %d = %d cells, %d active, %d inactive'
          % (NI, NJ, NK, CELLS, active, CELLS - active))
    print('  run-length encoding is doing real work: a property is %d tokens '
          'for %d cells' % (NK, CELLS))
    print('  bytes on disk %d' % total)
    return 0


if __name__ == '__main__':
    sys.exit(main())
