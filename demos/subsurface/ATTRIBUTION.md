# Data provenance for the subsurface demos

Some of these demos ship real, third-party subsurface data rather than data
written for the demo. Real data is worth the licence paperwork here because
the awkward parts are the point: a depth channel recorded in two different
units across one delivery, a sentinel value that is a number until something
makes it a null, a wireline run that is six channels wider than the LWD pass
before it. Those are not faults anyone would think to invent.

This file records where each file came from and under what terms. Every demo
that ships third-party data links here.

## OSDU Forum open test data

Redistributed by The Open Group's OSDU Forum from

    https://community.opengroup.org/osdu/platform/data-flow/data-loading/open-test-data

under the **Apache License 2.0** (see the `LICENSE` file at the root of that
repository), and from the companion public-read bucket
`s3://osdu-seismic-test-data`.

The underlying data is contributed by its original owners and carries their
terms as well as OSDU's:

| Contributor | Dataset | Original terms |
|---|---|---|
| Equinor and the Volve licence partners | Volve field data, released 2018 | Equinor Open Data Licence, free for commercial, research and educational use with attribution |
| TNO / NLOG | Netherlands well data from the NLOG portal | Dutch public data, freely available from `nlog.nl` |

### Files used, and where they came from

| Demo | File as shipped | Original path |
|---|---|---|
| `dlis-wireline-petrophysics` | `landing/lwd/2026-03-11_15_9-F-9_WLC_COMPOSITE_1.dlis` | `open-test-data/v2019-09-13/1-data/2-wip/volve/Well Logs/15_9-F-9 WLC_COMPOSITE_1.DLIS` |
| `dlis-wireline-petrophysics` | `landing/lwd/2026-03-11_15_9-F-11_WLC_COMPOSITE_1.dlis` | `open-test-data/v2019-09-13/1-data/2-wip/volve/Well Logs/15_9-F-11 WLC_COMPOSITE_1.DLIS` |
| `dlis-wireline-petrophysics` | `landing/wireline/2026-03-12_15_9-F-15C_WLC_COMPOSITE_2.dlis` | `open-test-data/v2019-09-13/1-data/2-wip/volve/Well Logs/15_9-F-15 C WLC_COMPOSITE_2.DLIS` |
| `lis-tape-archive-recovery` | `landing/volve/2026-03-11_15_9-F-4_WLC_COMPOSITE_1.lis` | `open-test-data/v2019-09-13/1-data/2-wip/volve/Well Logs/15_9-F-4 WLC_COMPOSITE_1.LIS` |
| `segy-2d-survey-index` | `landing/2026-03-11_ST0299-05005_MIG_FIN.segy` | `s3://osdu-seismic-test-data/volve/seismic/st0299/ST0299-05005+MIG_FIN.segy` |
| `segy-2d-survey-index` | `landing/2026-03-12_ST0299-15010_MIG_FIN.segy` | `s3://osdu-seismic-test-data/volve/seismic/st0299/ST0299-15010+MIG_FIN.segy` |
| `ukooa-survey-navigation` | `landing/2026-03-11_ST0299-CMP-05002.p190` | `s3://osdu-seismic-test-data/volve/seismic/st0299/navigation_2D/ST0299-CMP-05002.p190` |
| `ukooa-survey-navigation` | `landing/2026-03-11_ST0299-CMP-05003.p190` | `s3://osdu-seismic-test-data/volve/seismic/st0299/navigation_2D/ST0299-CMP-05003.p190` |
| `ukooa-survey-navigation` | `landing/2026-03-12_ST0299-CMP-05004.p190` | `s3://osdu-seismic-test-data/volve/seismic/st0299/navigation_2D/ST0299-CMP-05004.p190` |
| `ukooa-survey-navigation` | `landing/2026-03-12_ST0299-CMP-05005.p190` | `s3://osdu-seismic-test-data/volve/seismic/st0299/navigation_2D/ST0299-CMP-05005.p190` |
| `las-well-log-library` | `landing/2026-03-11_1958_MED-01.las` | `s3://osdu-seismic-test-data/r1/data/provided/well-logs/1601_med01_1958_comp.las` |
| `las-well-log-library` | `landing/2026-03-11_1961_WAS-25.las` | `s3://osdu-seismic-test-data/r1/data/provided/well-logs/2406_was25_1961_comp.las` |
| `las-well-log-library` | `landing/2026-03-11_1963_MED-05.las` | `s3://osdu-seismic-test-data/r1/data/provided/well-logs/1605_med05_1963_comp.las` |
| `las-well-log-library` | `landing/2026-03-12_1969_D15-01.las` | `s3://osdu-seismic-test-data/r1/data/provided/well-logs/7019_d1501_1969_comp.las` |
| `las-well-log-library` | `landing/2026-03-12_1972_K08-02.las` | `s3://osdu-seismic-test-data/r1/data/provided/well-logs/7104_k0802_1972_comp.las` |
| `las-well-log-library` | `landing/2026-03-12_1979_GRW-01.las` | `s3://osdu-seismic-test-data/r1/data/provided/well-logs/1348_grw01_1979_comp.las` |
| `las-well-log-library` | `landing/2026-03-12_1990_L09-06.las` | `s3://osdu-seismic-test-data/r1/data/provided/well-logs/8802_l0906_1990_comp.las` |

The LAS files are additionally renamed to carry the well and the logging
year, because the demo's loader reads the vintage out of the delivery name:
that is the data manager's convention, not something in the file. The well
name itself still comes from each file's own `~W` block.

**The only change made to any of them is the file name.** The bytes are
unmodified. Each is prefixed with the delivery date the demo's landing zone
uses, and the extension is lower-cased, because the demos model a landing
folder where a scheduled loader keys its run on the drop date. Nothing inside
the files was touched, which is why the counts the demos assert are
properties of the released data and not of anything done here.

## What reading the real files found

Real data is not just more convincing, it is a better test. Reading these
files turned up defects in the readers that every generated fixture had
passed, because a generator and a reader written from the same reading of a
specification agree with each other:

- **P1/90 `C` records were dropped as comments.** A CMP deliverable carries no
  `S` or `R` records at all, so the whole class of file returned zero rows,
  silently. Volve's ST0299 navigation is exactly this shape.
- **P1/90 packed longitudes were parsed from the left.** The degrees are right
  justified in their field, so a survey at 1 degree 56 minutes east reads as
  156 degrees: the North Sea relocated to central Siberia, with no error.
- **Two Volve DLIS composites record depth in different units**, tenths of an
  inch and millimetres, three orders of magnitude apart with nothing in the
  numbers saying so.
- **The Volve LIS tape has no null convention.** All 21505 frames claim a
  density and 4808 are physically possible, and 1114 density corrections are
  below -1 g/cc with the worst at -57338.

## What OSDU does not publish

The OSDU open test data covers SEG-Y, LAS, UKOOA P1/90, SEG-P1, DLIS and LIS.
It publishes nothing in SEG-D, ECLIPSE binary, GRDECL, ZMAP+, RESQML, WITSML,
PRODML, GeoJSON, Shapefile or GeoTIFF, and no other open corpus covers that
set either.

The demos for those formats therefore ship data written for the demo by a
generator committed alongside it, from a literal description of what the file
should contain. That is a deliberate trade rather than a shortcut: a generated
file lets a test assert an exact count that was known before any reader saw
the bytes, where a downloaded file's expected values would have to come from
our own reader and the assertion would pass whatever that reader did. Each
generator names the real-world scenario it is reproducing and the real
specification behaviour it is exercising.
