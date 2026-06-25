-- ============================================================================
-- Genomic Variant Catalog - PGM Learned Index on a Clustered Composite Key - Setup
-- ============================================================================
-- A population genomics variant catalog: 24,000,000 short variants spanning the
-- 24 human chromosomes (chr1..chr22, chrX, chrY), 1,000,000 variants per
-- chromosome.
--
-- The lookup key here is the genomic LOCUS, a composite (chromosome, position).
-- Unlike a scattered transaction id, a variant catalog is CLUSTERED: it is
-- ingested in genome order, so positions rise monotonically within each
-- chromosome and the files on disk are laid out in locus order. That clustering
-- is exactly the shape a PGM (Piecewise Geometric Model) learned index models
-- best: a near-linear key distribution it can approximate with a handful of
-- line segments, giving O(log log n) locate with a far smaller footprint than a
-- classical B+ tree.
--
-- This setup only seeds the data. The composite CREATE INDEX statement lives in
-- queries.sql so the learned-index lesson is taught alongside the queries it
-- accelerates.
--
-- Tables created:
--   1. genomic_variants - 24,000,000 variants, clustered by (chromosome, position)
--
-- Generated inline with generate_series in six 4,000,000-row chunks (no external
-- files); each chunk lays down exactly four chromosomes in locus order. Deletion
-- vectors are on so a keyed UPDATE marks one row instead of rewriting its file.
-- ============================================================================

CREATE ZONE IF NOT EXISTS {{zone_name}} TYPE EXTERNAL
    COMMENT 'External and Delta tables - demo datasets';

CREATE SCHEMA IF NOT EXISTS {{zone_name}}.delta_demos
    COMMENT 'Delta table management tutorial demos';


-- ============================================================================
-- TABLE: genomic_variants - 24,000,000 variants, clustered by (chromosome, position)
-- ============================================================================

DROP INDEX IF EXISTS idx_variant_locus ON TABLE {{zone_name}}.delta_demos.genomic_variants;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.genomic_variants WITH FILES;

CREATE DELTA TABLE {{zone_name}}.delta_demos.genomic_variants (
    chromosome            VARCHAR,
    position              BIGINT,
    ref_allele            VARCHAR,
    alt_allele            VARCHAR,
    gene_symbol           VARCHAR,
    clinical_significance VARCHAR,
    allele_frequency      DOUBLE
) LOCATION 'delta-row-index-genomic-variants/genomic_variants'
TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');

-- A single global sequence g = 1..24,000,000 drives everything:
--   chromosome            = chr1..chr22, chrX, chrY  (g grouped in 1,000,000 blocks)
--   position              = ((g-1) mod 1,000,000 + 1) * 100   (100 .. 100,000,000)
--   ref/alt alleles       = rotating A/C/G/T so ref <> alt
--   gene_symbol           = one of six well-known genes
--   clinical_significance = pathogenic (0.1%), likely_pathogenic (0.9%),
--                           uncertain (19%), likely_benign (30%), benign (50%)
--   allele_frequency      = (g mod 1000) / 1000, so the rarest classes are rarest
-- Because g is monotonic in the generated id and each chunk is written in id
-- order, the table lands physically ordered by (chromosome, position): a
-- clustered key.

INSERT INTO {{zone_name}}.delta_demos.genomic_variants
WITH src AS (SELECT (CAST(id AS BIGINT) + 0) AS g FROM generate_series(1, 4000000) AS t(id))
SELECT
    CASE WHEN (g-1)/1000000 = 22 THEN 'chrX'
         WHEN (g-1)/1000000 = 23 THEN 'chrY'
         ELSE 'chr' || CAST((g-1)/1000000 + 1 AS VARCHAR) END           AS chromosome,
    ((g-1) % 1000000 + 1) * 100                                         AS position,
    CASE g % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END     AS ref_allele,
    CASE (g+1) % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END AS alt_allele,
    CASE g % 6 WHEN 0 THEN 'BRCA2' WHEN 1 THEN 'TP53' WHEN 2 THEN 'EGFR' WHEN 3 THEN 'CFTR' WHEN 4 THEN 'APOE' ELSE 'MTHFR' END AS gene_symbol,
    CASE WHEN g % 1000 = 0 THEN 'pathogenic'
         WHEN g % 1000 < 10 THEN 'likely_pathogenic'
         WHEN g % 1000 < 200 THEN 'uncertain_significance'
         WHEN g % 1000 < 500 THEN 'likely_benign'
         ELSE 'benign' END                                              AS clinical_significance,
    ROUND((g % 1000) / 1000.0, 4)                                       AS allele_frequency
FROM src;

INSERT INTO {{zone_name}}.delta_demos.genomic_variants
WITH src AS (SELECT (CAST(id AS BIGINT) + 4000000) AS g FROM generate_series(1, 4000000) AS t(id))
SELECT
    CASE WHEN (g-1)/1000000 = 22 THEN 'chrX'
         WHEN (g-1)/1000000 = 23 THEN 'chrY'
         ELSE 'chr' || CAST((g-1)/1000000 + 1 AS VARCHAR) END           AS chromosome,
    ((g-1) % 1000000 + 1) * 100                                         AS position,
    CASE g % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END     AS ref_allele,
    CASE (g+1) % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END AS alt_allele,
    CASE g % 6 WHEN 0 THEN 'BRCA2' WHEN 1 THEN 'TP53' WHEN 2 THEN 'EGFR' WHEN 3 THEN 'CFTR' WHEN 4 THEN 'APOE' ELSE 'MTHFR' END AS gene_symbol,
    CASE WHEN g % 1000 = 0 THEN 'pathogenic'
         WHEN g % 1000 < 10 THEN 'likely_pathogenic'
         WHEN g % 1000 < 200 THEN 'uncertain_significance'
         WHEN g % 1000 < 500 THEN 'likely_benign'
         ELSE 'benign' END                                              AS clinical_significance,
    ROUND((g % 1000) / 1000.0, 4)                                       AS allele_frequency
FROM src;

INSERT INTO {{zone_name}}.delta_demos.genomic_variants
WITH src AS (SELECT (CAST(id AS BIGINT) + 8000000) AS g FROM generate_series(1, 4000000) AS t(id))
SELECT
    CASE WHEN (g-1)/1000000 = 22 THEN 'chrX'
         WHEN (g-1)/1000000 = 23 THEN 'chrY'
         ELSE 'chr' || CAST((g-1)/1000000 + 1 AS VARCHAR) END           AS chromosome,
    ((g-1) % 1000000 + 1) * 100                                         AS position,
    CASE g % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END     AS ref_allele,
    CASE (g+1) % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END AS alt_allele,
    CASE g % 6 WHEN 0 THEN 'BRCA2' WHEN 1 THEN 'TP53' WHEN 2 THEN 'EGFR' WHEN 3 THEN 'CFTR' WHEN 4 THEN 'APOE' ELSE 'MTHFR' END AS gene_symbol,
    CASE WHEN g % 1000 = 0 THEN 'pathogenic'
         WHEN g % 1000 < 10 THEN 'likely_pathogenic'
         WHEN g % 1000 < 200 THEN 'uncertain_significance'
         WHEN g % 1000 < 500 THEN 'likely_benign'
         ELSE 'benign' END                                              AS clinical_significance,
    ROUND((g % 1000) / 1000.0, 4)                                       AS allele_frequency
FROM src;

INSERT INTO {{zone_name}}.delta_demos.genomic_variants
WITH src AS (SELECT (CAST(id AS BIGINT) + 12000000) AS g FROM generate_series(1, 4000000) AS t(id))
SELECT
    CASE WHEN (g-1)/1000000 = 22 THEN 'chrX'
         WHEN (g-1)/1000000 = 23 THEN 'chrY'
         ELSE 'chr' || CAST((g-1)/1000000 + 1 AS VARCHAR) END           AS chromosome,
    ((g-1) % 1000000 + 1) * 100                                         AS position,
    CASE g % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END     AS ref_allele,
    CASE (g+1) % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END AS alt_allele,
    CASE g % 6 WHEN 0 THEN 'BRCA2' WHEN 1 THEN 'TP53' WHEN 2 THEN 'EGFR' WHEN 3 THEN 'CFTR' WHEN 4 THEN 'APOE' ELSE 'MTHFR' END AS gene_symbol,
    CASE WHEN g % 1000 = 0 THEN 'pathogenic'
         WHEN g % 1000 < 10 THEN 'likely_pathogenic'
         WHEN g % 1000 < 200 THEN 'uncertain_significance'
         WHEN g % 1000 < 500 THEN 'likely_benign'
         ELSE 'benign' END                                              AS clinical_significance,
    ROUND((g % 1000) / 1000.0, 4)                                       AS allele_frequency
FROM src;

INSERT INTO {{zone_name}}.delta_demos.genomic_variants
WITH src AS (SELECT (CAST(id AS BIGINT) + 16000000) AS g FROM generate_series(1, 4000000) AS t(id))
SELECT
    CASE WHEN (g-1)/1000000 = 22 THEN 'chrX'
         WHEN (g-1)/1000000 = 23 THEN 'chrY'
         ELSE 'chr' || CAST((g-1)/1000000 + 1 AS VARCHAR) END           AS chromosome,
    ((g-1) % 1000000 + 1) * 100                                         AS position,
    CASE g % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END     AS ref_allele,
    CASE (g+1) % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END AS alt_allele,
    CASE g % 6 WHEN 0 THEN 'BRCA2' WHEN 1 THEN 'TP53' WHEN 2 THEN 'EGFR' WHEN 3 THEN 'CFTR' WHEN 4 THEN 'APOE' ELSE 'MTHFR' END AS gene_symbol,
    CASE WHEN g % 1000 = 0 THEN 'pathogenic'
         WHEN g % 1000 < 10 THEN 'likely_pathogenic'
         WHEN g % 1000 < 200 THEN 'uncertain_significance'
         WHEN g % 1000 < 500 THEN 'likely_benign'
         ELSE 'benign' END                                              AS clinical_significance,
    ROUND((g % 1000) / 1000.0, 4)                                       AS allele_frequency
FROM src;

INSERT INTO {{zone_name}}.delta_demos.genomic_variants
WITH src AS (SELECT (CAST(id AS BIGINT) + 20000000) AS g FROM generate_series(1, 4000000) AS t(id))
SELECT
    CASE WHEN (g-1)/1000000 = 22 THEN 'chrX'
         WHEN (g-1)/1000000 = 23 THEN 'chrY'
         ELSE 'chr' || CAST((g-1)/1000000 + 1 AS VARCHAR) END           AS chromosome,
    ((g-1) % 1000000 + 1) * 100                                         AS position,
    CASE g % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END     AS ref_allele,
    CASE (g+1) % 4 WHEN 0 THEN 'A' WHEN 1 THEN 'C' WHEN 2 THEN 'G' ELSE 'T' END AS alt_allele,
    CASE g % 6 WHEN 0 THEN 'BRCA2' WHEN 1 THEN 'TP53' WHEN 2 THEN 'EGFR' WHEN 3 THEN 'CFTR' WHEN 4 THEN 'APOE' ELSE 'MTHFR' END AS gene_symbol,
    CASE WHEN g % 1000 = 0 THEN 'pathogenic'
         WHEN g % 1000 < 10 THEN 'likely_pathogenic'
         WHEN g % 1000 < 200 THEN 'uncertain_significance'
         WHEN g % 1000 < 500 THEN 'likely_benign'
         ELSE 'benign' END                                              AS clinical_significance,
    ROUND((g % 1000) / 1000.0, 4)                                       AS allele_frequency
FROM src;


-- ============================================================================
-- Schema Detection & Permissions
-- ============================================================================

DETECT SCHEMA FOR TABLE {{zone_name}}.delta_demos.genomic_variants;
GRANT ADMIN ON TABLE {{zone_name}}.delta_demos.genomic_variants TO USER {{current_user}};
