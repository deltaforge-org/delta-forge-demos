# Subsurface: North Sea Demo Field

Five upstream energy formats over one synthetic field, read in place. No
export step, no conversion, no second copy of the truth.

| Table | Format | Row shape | Rows |
|---|---|---|---|
| `seismic_traces` | SEG-Y | one trace, samples as an array | 120 |
| `seismic_headers` | SEG-Y | one trace, no samples decoded | 120 |
| `well_logs` | LAS 2.0 | one depth step, one column per curve | 600 |
| `top_reservoir` | ZMAP+ | one grid node with real coordinates | 1,164 |
| `reservoir_model` | GRDECL | one cell, one column per property | 1,664 |
| `survey_navigation` | UKOOA P1/90 | one shot point | 240 |

## The point of the demo

Each format keeps the row shape that is natural to it. A trace, a depth step,
a grid node and a cell are different things, and flattening them into a common
shape would lose what makes each queryable. Query 19 then joins the seismic
volume to the survey navigation on real UTM coordinates, which is only possible
because both arrive already corrected: SEG-Y coordinate scalars are applied on
read, and P1/90 packed degrees-minutes-seconds arrive as decimal degrees.

## What each query is checking

The counts are not decorative. Several of them are the only way to tell a
correct reader from a plausible one:

- **Query 3** would report 45,000,000 instead of 450,000 if the SEG-Y
  coordinate scalar in bytes 71-72 were ignored.
- **Queries 7 and 8** pin that the LAS `-999.25` sentinel is a real SQL `NULL`.
  Read as a number it would drag every density average through the floor.
- **Query 12** catches a transposed ZMAP+ grid. ZMAP+ writes its values column
  by column; a row-major reader would still produce 1,164 nodes with values, so
  a count alone would not catch it. What catches it is the row and column
  indices staying inside their own bounds.
- **Query 13** is 1,664 rather than 2,400 because `ACTNUM` marks an inactive
  rim, and it is not some smaller number because the deck's run-length values
  (`3*0.25` meaning three of `0.25`) expanded correctly.
- **Query 17** would return nothing if P1/90's packed `DDMMSS.SSH` latitude
  were parsed as a plain number: 58.6 degrees is written `583600.00N`.

## The data

All six files are synthetic, generated deterministically, and are not real
survey or well data. They are small enough to commit and structured enough to
exercise the parts of each format that matter:

- `demo_survey.segy` carries an EBCDIC textual header, IBM hexadecimal float
  samples, and a negative coordinate scalar.
- The two LAS files carry a full `~W` well-information section and a
  washed-out density interval written with the file's own null value.
- `top_reservoir.zmap` blanks every node outside the mapped dome.
- `demo_model.grdecl` uses run-length values, inline `--` comments, and an
  `ACTNUM` rim.
- `demo_survey.p190` opens with six `H` header records that must not become
  rows.

The row counts this demo asserts on are verified without a running engine by
`delta-forge-tests/tests/format_discovery/subsurface_demo_fixtures.rs`, so a
failing assertion here means the engine changed rather than that a hand-written
count drifted.

## Running it

```
demo-test --local delta-forge-demos/demos/subsurface/north-sea-field
```

## Going further

- `include_samples = 'false'` on a real 40 GB volume is the difference between
  reading its geometry and reading the volume.
- `long_form = 'true'` on SEG-Y explodes one row per sample, for when the
  question is per-sample arithmetic rather than per-trace selection.
- The Subsurface page in the GUI (`/subsurface`) lists all fifteen formats in
  the family with their row shapes and options.
