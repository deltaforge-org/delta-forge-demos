# Subsurface and energy demos

Upstream energy formats read in place: seismic, well logs, reservoir models,
simulation decks, survey navigation and geospatial rasters. Fifteen formats are
supported, and each is getting a demo of its own.

Every demo here is an **incremental load**, not a read. A landing folder
receives one delivery per day, `DISCOVER` registers it from the file's own
bytes rather than its extension, and a re-runnable loader appends only what has
not landed yet, keyed on the source file name. Each demo runs one delivery
twice and asserts the row count did not move, because that assertion is the
only thing that distinguishes an incremental load from a reload that happens to
look right the first time.

| Demo | Format | Data | What it shows |
| --- | --- | --- | --- |
| [dlis-wireline-petrophysics](dlis-wireline-petrophysics/) | DLIS | **real Volve** | Two deliveries, the second six channels wider, so the load widens the table. Depth in two different units across one drop; the -999.25 sentinel becomes a real NULL. Ends on net pay in the Hugin Formation. |
| [lis-tape-archive-recovery](lis-tape-archive-recovery/) | LIS-79 | **real Volve** plus one written tape | Tape Image Format stripped, the curve list found in record type 64, a specification split across two physical records rejoined. The real tape has no null convention and carries a density correction of minus 57338, so the load applies physical validity rules. |
| [segd-field-record-qc](segd-field-record-qc/) | SEG-D | written for the demo | A crew's daily field records, with two planted faults the QC finds: one record short six channels, one recorded at the wrong sample interval. Binary-coded-decimal headers throughout. |
| [segy-2d-survey-index](segy-2d-survey-index/) | SEG-Y | **real Volve** | A 2D survey delivered line by line into a CDP index. 12 MB per line, read as 240 bytes per trace. 186 traces carry no source coordinate and the loader nulls them once. |
| [ukooa-survey-navigation](ukooa-survey-navigation/) | UKOOA P1/90 | **real Volve** | Navigation delivered per line into a shot-point database. Found two reader bugs: C records dropped as comments, and right-justified degrees parsed from the left. |
| [las-well-log-library](las-well-log-library/) | LAS 2.0 | **real NLOG** | Seven Dutch wells logged 1958 to 1990, whose curve sets differ by thirty years of tool development. One table over the union; the well header rides on every row so the library groups by well and field with no join. |
| [eclipse-history-match-review](eclipse-history-match-review/) | ECLIPSE binary | written for the demo | Two history-match runs of simulator output. Long form in, pivoted to one row per cell per report step. Every array spans two records because 1152 cells beats the format's 1000-element limit. |
| [witsml-drilling-operations](witsml-drilling-operations/) | WITSML | written for the demo | Definitive surveys from one platform template, read through the XML engine under a curated profile so nobody writes an XPath. Ends on a real anti-collision scan: two wells within 3 m at 550 m TVD. |
| [prodml-production-allocation](prodml-production-allocation/) | PRODML | written for the demo | A year of monthly regulator returns per facility. Water volumes follow from the oil and the cut, so the 50 percent crossover the demo finds is real: May for the mature facility, December for another, never for the third. |
| [north-sea-field](north-sea-field/) | SEG-Y, LAS, ZMAP+, GRDECL, UKOOA P1/90 | written for the demo | The cross-format integration demo: five formats over one field, joined on real coordinates. |

## Where the data comes from

Some demos ship real third-party data and some ship data written for them.
[ATTRIBUTION.md](ATTRIBUTION.md) records which is which, under what licence,
and the exact original path of every real file.

The short version: the OSDU Forum's open test data covers SEG-Y, LAS, UKOOA
P1/90, SEG-P1, DLIS and LIS, and those demos use it. It publishes nothing in
SEG-D, ECLIPSE binary, GRDECL, ZMAP+, RESQML, WITSML, PRODML, GeoJSON,
Shapefile or GeoTIFF, and no other open corpus covers that set, so those demos
ship data written by a generator committed beside them. Each generator names
the real scenario it reproduces and the specification behaviour it exercises.

## Regenerating the written data

Demos whose data is written for them carry a `generate_data.py`. It is
deterministic, so regenerating produces byte identical files:

```text
python demos/subsurface/<demo>/generate_data.py
```

The full format list, with row shapes and options, is on the Subsurface page in
the GUI (`/subsurface`) and in `wiki/features/subsurface-formats.md`.
