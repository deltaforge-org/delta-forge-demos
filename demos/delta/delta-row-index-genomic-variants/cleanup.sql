-- Cleanup: Genomic Variant Catalog - PGM Learned Index on a Clustered Composite Key

DROP INDEX IF EXISTS idx_variant_locus ON TABLE {{zone_name}}.delta_demos.genomic_variants;

DROP DELTA TABLE IF EXISTS {{zone_name}}.delta_demos.genomic_variants WITH FILES;

-- Remove the per-demo wrapper folder (now empty after the table drops)
DROP FOLDER 'delta-row-index-genomic-variants' IF EXISTS IN ZONE {{zone_name}};

DROP SCHEMA IF EXISTS {{zone_name}}.delta_demos;
