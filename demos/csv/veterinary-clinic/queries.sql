-- ============================================================================
-- Veterinary Clinic Patient Records — Verification Queries
-- ============================================================================
-- Tests recursive scanning, file_filter, max_rows, and analytical queries
-- across a multi-branch veterinary clinic dataset.
-- ============================================================================


-- ============================================================================
-- 1. FULL SCAN — all_visits (recursive across 3 branches)
-- ============================================================================
-- Verify all 75 rows are read and visit_ids from each branch are present.

ASSERT ROW_COUNT = 75
SELECT *
FROM {{zone_name}}.csv_vet.all_visits;

ASSERT ROW_COUNT = 25
SELECT visit_id
FROM {{zone_name}}.csv_vet.all_visits
WHERE visit_id LIKE 'N-%';

ASSERT ROW_COUNT = 25
SELECT visit_id
FROM {{zone_name}}.csv_vet.all_visits
WHERE visit_id LIKE 'S-%';

ASSERT ROW_COUNT = 25
SELECT visit_id
FROM {{zone_name}}.csv_vet.all_visits
WHERE visit_id LIKE 'E-%';


-- ============================================================================
-- 2. NORTH BRANCH FILTER — north_only (file_filter = '*north*')
-- ============================================================================
-- Verify only north branch data is read (25 rows, all N- prefixed).

ASSERT ROW_COUNT = 25
SELECT *
FROM {{zone_name}}.csv_vet.north_only;

ASSERT ROW_COUNT = 25
SELECT visit_id
FROM {{zone_name}}.csv_vet.north_only
WHERE visit_id LIKE 'N-%';

ASSERT VALUE pet_name = 'Ruby' WHERE visit_id = 'N-1'
ASSERT VALUE species = 'Cat' WHERE visit_id = 'N-1'
SELECT visit_id, pet_name, species, owner_name
FROM {{zone_name}}.csv_vet.north_only
ORDER BY visit_id;


-- ============================================================================
-- 3. SAMPLED VISITS — max_rows = 10 per file (30 total)
-- ============================================================================
-- With 3 files and max_rows=10, we expect 30 rows total.

ASSERT ROW_COUNT = 30
SELECT *
FROM {{zone_name}}.csv_vet.sampled_visits;


-- ============================================================================
-- 4. SPECIES BREAKDOWN — GROUP BY species
-- ============================================================================
-- Verify species distribution across all branches.

ASSERT ROW_COUNT = 5
ASSERT VALUE visit_count = 12 WHERE species = 'Bird'
ASSERT VALUE visit_count = 20 WHERE species = 'Cat'
ASSERT VALUE visit_count = 15 WHERE species = 'Dog'
ASSERT VALUE visit_count = 15 WHERE species = 'Hamster'
ASSERT VALUE visit_count = 13 WHERE species = 'Rabbit'
SELECT species, COUNT(*) AS visit_count
FROM {{zone_name}}.csv_vet.all_visits
GROUP BY species
ORDER BY species;


-- ============================================================================
-- 5. TREATMENT COST ANALYSIS — AVG and SUM by species
-- ============================================================================
-- Verify cost aggregations per species.

ASSERT VALUE total_cost = 11415.00
SELECT ROUND(SUM(CAST(treatment_cost AS DOUBLE)), 2) AS total_cost
FROM {{zone_name}}.csv_vet.all_visits;

ASSERT VALUE species_sum = 1710.00 WHERE species = 'Bird'
ASSERT VALUE species_sum = 2560.00 WHERE species = 'Cat'
ASSERT VALUE species_sum = 1985.00 WHERE species = 'Dog'
ASSERT VALUE species_sum = 3170.00 WHERE species = 'Hamster'
ASSERT VALUE species_sum = 1990.00 WHERE species = 'Rabbit'
SELECT species,
       ROUND(SUM(CAST(treatment_cost AS DOUBLE)), 2) AS species_sum,
       ROUND(AVG(CAST(treatment_cost AS DOUBLE)), 2) AS species_avg
FROM {{zone_name}}.csv_vet.all_visits
GROUP BY species
ORDER BY species;


-- ============================================================================
-- 6. NULL HANDLING — Count NULLs in breed and diagnosis
-- ============================================================================
-- Verify NULL counts match expected values.

ASSERT VALUE null_breed_count = 9
SELECT COUNT(*) FILTER (WHERE breed IS NULL OR breed = '') AS null_breed_count
FROM {{zone_name}}.csv_vet.all_visits;

ASSERT VALUE null_diag_count = 4
SELECT COUNT(*) FILTER (WHERE diagnosis IS NULL OR diagnosis = '') AS null_diag_count
FROM {{zone_name}}.csv_vet.all_visits;

ASSERT VALUE non_null_breed = 66
SELECT COUNT(*) FILTER (WHERE breed IS NOT NULL AND breed <> '') AS non_null_breed
FROM {{zone_name}}.csv_vet.all_visits;


-- ============================================================================
-- VERIFY: Grand totals
-- ============================================================================
-- Comprehensive verification of all key metrics.

ASSERT VALUE total_rows = 75
ASSERT VALUE distinct_species = 5
ASSERT VALUE sum_treatment_cost = 11415.00
ASSERT VALUE avg_weight_kg = 7.31
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT species) AS distinct_species,
    ROUND(SUM(CAST(treatment_cost AS DOUBLE)), 2) AS sum_treatment_cost,
    ROUND(AVG(CAST(weight_kg AS DOUBLE)), 2) AS avg_weight_kg
FROM {{zone_name}}.csv_vet.all_visits;
