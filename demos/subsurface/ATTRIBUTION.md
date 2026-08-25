# Data provenance for the subsurface demos

Some of these demos ship real, third-party subsurface data rather than data
written for the demo. Real data is worth the licence paperwork here because
the awkward parts are the point: a depth channel recorded in two different
units across one delivery, a sentinel value that is a number until something
makes it a null, a wireline run that is six channels wider than the LWD pass
before it. Those are not faults anyone would think to invent.

This file records where each file came from and under what terms. Every demo
that ships third-party data links here.

## A correction

An earlier version of this file said that no open corpus covers the formats
OSDU does not publish. **That was wrong, and it was written without searching
properly.** Open, redistributable data exists for several of them, and the
demos are being moved onto it. What follows is what was actually found.

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

## resqpy example models

From the **bp/resqpy** project's `example_data` directory,

    https://github.com/bp/resqpy

under the **MIT License**. Written by resqpy itself, and shipped here with the
`.h5` companions they name, so the handover the demo audits is a complete one.

| Demo | File as shipped | Original |
|---|---|---|
| `resqml-model-handover` | `landing/packages/2026-03-11_block.epc` | `example_data/block.epc` |
| `resqml-model-handover` | `landing/arrays/2026-03-11_block.h5` | `example_data/block.h5` |
| `resqml-model-handover` | `landing/packages/2026-03-12_s_bend.epc` | `example_data/s_bend.epc` |
| `resqml-model-handover` | `landing/arrays/2026-03-12_s_bend.h5` | `example_data/s_bend.h5` |
| `resqml-model-handover` | `landing/packages/2026-03-12_tic_tac_toe.epc` | `example_data/tic_tac_toe.epc` |
| `resqml-model-handover` | `landing/arrays/2026-03-12_tic_tac_toe.h5` | `example_data/tic_tac_toe.h5` |

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
- **RESQML emitted package metadata as an object.** `docProps/core.xml` is
  Open Packaging Conventions metadata about the package, and it ends in `.xml`
  outside `_rels/`, so it satisfied every condition the reader tested. One
  spurious row per package, in a table whose whole purpose is an inventory.
- **RESQML read no references at all.** A DataObjectReference names its target
  in an `<eml:UUID>` child element, not a `uuid` attribute. Reading only the
  attribute form left `reference_count` at zero for all 56 objects in these
  three packages, so the reference graph was empty while every other column
  was correct.
- **Two Volve DLIS composites record depth in different units**, tenths of an
  inch and millimetres, three orders of magnitude apart with nothing in the
  numbers saying so.
- **The Volve LIS tape has no null convention.** All 21505 frames claim a
  density and 4808 are physically possible, and 1114 density corrections are
  below -1 g/cc with the worst at -57338.

## Norne, and what a real reservoir model brought with it

`demos/subsurface/grdecl-static-model` reads the published static model of the
Norne field, offshore Norway.

| | |
|---|---|
| Source | `OPM/opm-data`, the Open Porous Media data repository |
| Files used | `NORNE_ATW2013.DATA` (grid dimensions), `INCLUDE/GRID/ACTNUM_0704.prop`, `INCLUDE/PETRO/PORO_0704.prop`, `INCLUDE/PETRO/PERM_0704.prop`, `INCLUDE/PETRO/NTG_0704.prop` |
| Licence | Open Database License 1.0, contents under the Database Contents License 1.0 |
| Copyright | Copyright (C) 2015 Statoil |

**What was done to the files.** The Norne deck distributes its properties as
separate files pulled in by `INCLUDE` statements, and the demo's reader treats
`INCLUDE` as a grid keyword to skip rather than a file to follow, so the four
property files were concatenated behind a `DIMENS 46 112 22 /` header taken
from the master deck. That is exactly what a simulator holds in memory once it
has resolved those includes. Each property block is byte for byte the file it
came from, including the ODbL notice and the Statoil copyright line that each
one carries inline. No value was edited, reordered, resampled or rounded.

**Every asserted number was computed by an independent GRDECL reader** written
for the purpose, not by the engine. 37 of them, all reproducing from the
shipped bytes.

**What the real model turned out to contain**, none of which a generated deck
would have produced:

- Only 44,927 of its 113,344 cells are active. Three cells in five are outside
  the simulated volume, so whether inactive cells are dropped is the
  difference between a mean porosity of 24 percent and a meaningless one.
- **Layer 3 is entirely inactive.** All 5152 of its cells sit in the grid and
  not one is solved, so a `GROUP BY k` over the active model returns 21 groups
  and not 22. A generated deck would have had no reason to contain that.
- **Nothing repeats.** Layer 0 holds 2221 active cells with 2221 distinct
  porosities, 2220 distinct permeabilities (exactly one pair collides), and
  1981 distinct net-to-gross values. The deck is 5.7 MB precisely because
  run-length encoding has nothing to compress, which is the opposite of what
  GRDECL is usually shaped like.

That last point is why the demo did not simply replace its written deck. The
coarse sector model beside Norne writes 36,000 values as 62 numeric tokens,
and the run-length expansion is the failure mode that silently turns a model
into a few dozen cells. Norne does not exercise it at all. The two decks sit
in one landing folder and one curated table because between them they cover
both halves of the format, and neither alone does.

## The demos that still ship written data

SEG-D, ECLIPSE binary, ZMAP+, WITSML, PRODML, GeoJSON, Shapefile and GeoTIFF
currently ship data written by a generator committed beside them. GRDECL no
longer does; see the Norne section below. For the rest:

| Format | Candidate source | Licence | Status |
|---|---|---|---|
| GeoTIFF | GDAL `autotest/gcore/data` | MIT/X11 | Examined, not adopted |
| Shapefile, GeoJSON | Natural Earth | Public domain | Examined, not adopted |
| WITSML, PRODML | `F2I-Consulting/fesapi`, `geosiris/energyml-*` | Apache-2.0 | Not yet examined |
| ECLIPSE binary | Not found: `opm-data` ships input decks, not simulator output | | Open |
| SEG-D, ZMAP+ | Not found | | Open |

**Why GeoTIFF and Natural Earth were examined and not adopted.** GDAL's
`autotest` tree is 235 TIFF files, and they are other people's synthetic test
fixtures (`sasha.tif`, `stefan_full_rgba.tif`, `quad-lzw-old-style.tif`),
not survey rasters. Swapping to them would trade a coherent subsurface
scenario, with its deliberate CRS and resolution outliers, for files that are
real bytes but not real subsurface data, which is not the property that was
wanted. Natural Earth is genuinely real and genuinely public domain, but it is
world political geography: adopting it would move the Shapefile and GeoJSON
demos off well pads and licence blocks and onto country borders. On-domain
alternatives were tried and did not serve: Sodir returned 500, NLOG 404, and
BOEM served HTML rather than the archive. Both remain open if an on-domain
source appears.

**ODbL is share-alike on the database.** That obligation attaches to Norne and
does not attach to the MIT, Apache-2.0 and public-domain sources above. It is
recorded so nobody has to rediscover it.

A generated file still has one real advantage worth stating: a test can assert
an exact count that was known before any reader saw the bytes, where a
downloaded file's expected values have to be established by reading it. Every
count asserted against real data in these demos was therefore computed by an
independent parser written for the purpose, never by the engine's own reader.
