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

**The only change made to any of them is the file name.** The bytes are
unmodified. Each is prefixed with the delivery date the demo's landing zone
uses, and the extension is lower-cased, because the demos model a landing
folder where a scheduled loader keys its run on the drop date. Nothing inside
the files was touched, which is why the counts the demos assert are
properties of the released data and not of anything done here.

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
