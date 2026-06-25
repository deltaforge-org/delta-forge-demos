-- ============================================================================
-- Genomic Variant Catalog - A PGM Learned Index over a Clustered Composite Key
-- ============================================================================
-- 24,000,000 short variants across the 24 human chromosomes, 1,000,000 per
-- chromosome, ingested in genome order. The lookup key is the LOCUS, a
-- composite (chromosome, position).
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │   TWO ROW-LEVEL INDEX ALGORITHMS: B+ TREE vs PGM (LEARNED)            │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ A B+ tree stores every key in a balanced node hierarchy: predictable, │
--  │ but one node per fan-out step and a large footprint at scale.          │
--  │                                                                        │
--  │ A PGM (Piecewise Geometric Model) index instead LEARNS the key        │
--  │ distribution. It approximates the sorted keys with a small set of      │
--  │ line segments and stores only the segments plus a tiny recursive       │
--  │ model on top. Lookup is O(log log n). For a near-linear key, such as   │
--  │ monotonically rising genomic coordinates, a few segments cover         │
--  │ millions of keys, so the model is dramatically smaller than a B+ tree  │
--  │ while locating just as precisely. PGM is the DEFAULT algorithm; this   │
--  │ demo names it explicitly with USING pgm.                               │
--  └──────────────────────────────────────────────────────────────────────┘
--
--  ┌──────────────────────────────────────────────────────────────────────┐
--  │   SCATTERED KEY vs CLUSTERED KEY                                       │
--  ├──────────────────────────────────────────────────────────────────────┤
--  │ A scattered key (a random transaction id) lands its matching rows in   │
--  │ every file, so a RANGE on it still has to scan everything: the index   │
--  │ helps only point lookups.                                              │
--  │                                                                        │
--  │ A genomic variant catalog is the opposite: it is CLUSTERED. Positions  │
--  │ rise monotonically within a chromosome and the files are laid down in  │
--  │ locus order. A coordinate window therefore maps to a CONTIGUOUS run of │
--  │ rows, so the learned index serves the whole RANGE, not just a point.   │
--  │ That is the headline of this demo.                                     │
--  └──────────────────────────────────────────────────────────────────────┘
--
-- The honest signal, the same one the engine reports for any scan:
--   ACTUAL rows_consumed IS NULL  -> the index served the rows, NO scan/decode.
--   ACTUAL rows_consumed populated -> a table scan ran and decoded rows.
-- ============================================================================


-- ============================================================================
-- RESET: start index-free so the baseline contrast is honest on every run
-- ============================================================================
-- The baseline below must measure the table with NO index. Dropping it first
-- makes that true even when this script is re-run without re-running setup (the
-- index is a child Delta table that otherwise persists from a prior run).

DROP INDEX IF EXISTS idx_variant_locus ON TABLE {{zone_name}}.delta_demos.genomic_variants;


-- ============================================================================
-- EXPLORE: the shape of the catalog
-- ============================================================================
-- 24,000,000 variants, 24 chromosomes, positions on a 100bp grid from 100 to
-- 100,000,000 within each chromosome.

ASSERT ROW_COUNT = 1
ASSERT VALUE total_variants = 24000000
ASSERT VALUE distinct_chromosomes = 24
ASSERT VALUE min_pos = 100
ASSERT VALUE max_pos = 100000000
SELECT COUNT(*)                    AS total_variants,
       COUNT(DISTINCT chromosome)  AS distinct_chromosomes,
       MIN(position)               AS min_pos,
       MAX(position)               AS max_pos
FROM {{zone_name}}.delta_demos.genomic_variants;


-- ============================================================================
-- BASELINE: locate one variant with NO index
-- ============================================================================
-- A clinician needs the variant at chr7:5,000,000 (an EGFR locus). The key is
-- clustered, so Delta's per-file min/max can prune files, but the engine still
-- DECODES the surviving row group to extract the single row: rows_consumed is
-- populated, rows_returned is 1.

ASSERT VALUE value IS NOT NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '1' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT chromosome, position, gene_symbol, clinical_significance
FROM {{zone_name}}.delta_demos.genomic_variants
WHERE chromosome = 'chr7' AND position = 5000000;


-- ============================================================================
-- BUILD: a PGM learned index on the composite locus key
-- ============================================================================
-- USING pgm makes the algorithm explicit (it is also the default). The index is
-- keyed on (chromosome, position): chromosome first, then position within each
-- chromosome, matching the clustered layout on disk.

CREATE INDEX IF NOT EXISTS idx_variant_locus
    ON TABLE {{zone_name}}.delta_demos.genomic_variants (chromosome, position)
    USING pgm
    WITH (auto_update = true);


-- ============================================================================
-- HELPS (1): the SAME point lookup, now served by the learned index
-- ============================================================================
-- Nothing is decoded: rows_consumed IS NULL because the PGM model located the
-- one row directly through its pointer.

ASSERT VALUE value IS NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '1' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT chromosome, position, gene_symbol, clinical_significance
FROM {{zone_name}}.delta_demos.genomic_variants
WHERE chromosome = 'chr7' AND position = 5000000;

-- Correctness: the indexed lookup returns exactly the right variant.

ASSERT ROW_COUNT = 1
ASSERT VALUE chromosome = 'chr7'
ASSERT VALUE position = 5000000
ASSERT VALUE ref_allele = 'A'
ASSERT VALUE alt_allele = 'C'
ASSERT VALUE gene_symbol = 'EGFR'
ASSERT VALUE clinical_significance = 'pathogenic'
SELECT chromosome, position, ref_allele, alt_allele, gene_symbol, clinical_significance, allele_frequency
FROM {{zone_name}}.delta_demos.genomic_variants
WHERE chromosome = 'chr7' AND position = 5000000;


-- ============================================================================
-- HELPS (2): a RANGE scan across a 1-megabase window - the clustered headline
-- ============================================================================
-- Every variant in chr7 between position 5,000,100 and 6,000,000. Because the
-- locus key is clustered, these 10,000 variants are a CONTIGUOUS run, so the
-- learned index serves the whole range: rows_consumed IS NULL. This is the
-- payoff a scattered key can never give a range query.

ASSERT VALUE value IS NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '10,000' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT chromosome, position, gene_symbol, clinical_significance
FROM {{zone_name}}.delta_demos.genomic_variants
WHERE chromosome = 'chr7' AND position BETWEEN 5000100 AND 6000000;

-- Correctness: the window holds 10,000 variants, 10 of them pathogenic, bounded
-- exactly by the requested coordinates.

ASSERT ROW_COUNT = 1
ASSERT VALUE variant_count = 10000
ASSERT VALUE pathogenic_count = 10
ASSERT VALUE window_start = 5000100
ASSERT VALUE window_end = 6000000
SELECT COUNT(*)                                                   AS variant_count,
       COUNT(*) FILTER (WHERE clinical_significance = 'pathogenic') AS pathogenic_count,
       MIN(position)                                              AS window_start,
       MAX(position)                                              AS window_end
FROM {{zone_name}}.delta_demos.genomic_variants
WHERE chromosome = 'chr7' AND position BETWEEN 5000100 AND 6000000;


-- ============================================================================
-- NO BENEFIT: a position window WITHOUT the chromosome - leftmost-prefix broken
-- ============================================================================
-- The same coordinate window with no chromosome predicate asks for that band on
-- EVERY chromosome. The index is keyed (chromosome, position), so skipping the
-- leading column scatters the matches across all 24 chromosomes: the leftmost-
-- prefix rule is violated and the query falls back to a scan. The answer is
-- still correct - 10,000 per chromosome x 24 = 240,000 rows - it just is not
-- index-served: rows_consumed is populated.

ASSERT VALUE value IS NOT NULL WHERE metric = 'rows_consumed'
ASSERT VALUE value = '240,000' WHERE metric = 'rows_returned'
SHOW STATS ACTUAL
SELECT chromosome, position
FROM {{zone_name}}.delta_demos.genomic_variants
WHERE position BETWEEN 5000100 AND 6000000;


-- ============================================================================
-- THE FOOTPRINT: what the learned index keeps
-- ============================================================================
-- One sorted leaf per row (24,000,000), modeled by the PGM segments and kept
-- current on every write while auto_update is on. The algorithm column confirms
-- this is the learned index, not a B+ tree.

ASSERT ROW_COUNT = 1
ASSERT VALUE name = 'idx_variant_locus'
ASSERT VALUE algorithm = 'pgm'
ASSERT VALUE auto_update = true
ASSERT VALUE status = 'current'
ASSERT VALUE leaf_count = 24000000
DESCRIBE INDEXES ON TABLE {{zone_name}}.delta_demos.genomic_variants;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The clinical-significance mix across all 24,000,000 variants: rare pathogenic
-- and likely-pathogenic calls, a long tail of uncertain, and a benign majority.

ASSERT ROW_COUNT = 5
ASSERT VALUE cnt = 12000000 WHERE clinical_significance = 'benign'
ASSERT VALUE cnt = 7200000  WHERE clinical_significance = 'likely_benign'
ASSERT VALUE cnt = 216000   WHERE clinical_significance = 'likely_pathogenic'
ASSERT VALUE cnt = 24000     WHERE clinical_significance = 'pathogenic'
ASSERT VALUE cnt = 4560000  WHERE clinical_significance = 'uncertain_significance'
SELECT clinical_significance, COUNT(*) AS cnt
FROM {{zone_name}}.delta_demos.genomic_variants
GROUP BY clinical_significance
ORDER BY clinical_significance;
