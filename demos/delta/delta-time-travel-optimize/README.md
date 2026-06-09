# Delta Time Travel & OPTIMIZE — Version History

Demonstrates Delta table versioning through multiple operations and
file compaction with OPTIMIZE.

## Data Story

A warehouse inventory system tracks 25 items. Over time, low-stock items
are restocked, discontinued products are removed, new items are added, and
the table is optimized for faster reads. Each operation creates a new
version that can be browsed in the DeltaForge GUI.

## Table

| Object | Type | Rows | Versions | Purpose |
|--------|------|------|----------|---------|
| `inventory` | Delta Table | 32 (final) | 5 (V0-V4) | Versioned inventory |

## Version History

| Version | Operation | Rows | Key Changes |
|---------|-----------|------|-------------|
| 0 | CREATE + INSERT | 25 | Initial 25 items |
| 1 | UPDATE | 25 | 5 restocked (qty += 100) |
| 2 | DELETE | 22 | 3 discontinued removed |
| 3 | INSERT | 32 | 10 new items added |
| 4 | OPTIMIZE | 32 | Files compacted |

## Schema

**inventory:** `id INT, item VARCHAR, category VARCHAR, qty INT, price DOUBLE, warehouse VARCHAR`

## Verification

7 automated PASS/FAIL checks verify final state. Use the GUI version
browser to explore each historical version.
