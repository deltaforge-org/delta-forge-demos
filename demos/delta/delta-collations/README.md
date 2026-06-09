# Delta Collations — Language-Aware Sorting & Comparison

Demonstrates language-aware data patterns in Delta tables using a global
contact directory with multilingual names.

## Data Story

A global contact directory stores names with diacritics (Mueller, Garcia),
CJK-origin names, and various international scripts. The sort_key column stores
ASCII-normalized versions for consistent sorting (Mueller->mueller, Garcia->garcia).
This demonstrates how Delta tables handle multilingual data and the pattern of
collation-aware design where a normalized sort key enables consistent ordering
regardless of the original script or diacritics.

## Table

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `global_contacts` | Delta Table | 40 | Multilingual contact records with sort keys |

## Schema

**global_contacts:** `id INT, first_name VARCHAR, last_name VARCHAR, city VARCHAR, country VARCHAR, language VARCHAR, sort_key VARCHAR, email VARCHAR`

## Contact Distribution

- **European (ids 1-20):** 20 contacts — German (5), French (5), Spanish (5), Scandinavian (5: Sweden, Norway, Denmark, Finland)
- **Asian (ids 21-30):** 10 contacts — Japanese (4), Chinese (3), Korean (3)
- **International (ids 31-40):** 10 contacts — UAE, Egypt, India, Russia, Nigeria, Senegal, Brazil, Italy

## Operations

1. INSERT 20 rows — European names with diacritics and accented characters
2. INSERT 10 rows — Asian names (Japanese, Chinese, Korean)
3. INSERT 10 rows — mixed international names
4. UPDATE — normalize sort_key for 5 entries

## Verification

8 automated PASS/FAIL checks verify total row count (40), European count (20),
Asian count (10), other count (10), distinct countries (18), distinct languages (16),
accented names (20), and sort key populated (40).
