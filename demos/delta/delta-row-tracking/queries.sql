-- ============================================================================
-- Delta Row Tracking — Stable Row IDs & Audit Trail — Educational Queries
-- ============================================================================
-- WHAT: Row tracking assigns stable, unique IDs to each row in a Delta table,
--       enabling precise change tracking across versions.
-- WHY:  Without row tracking, identifying which specific rows changed between
--       versions requires expensive full-table comparisons. Row tracking makes
--       CDC, MERGE, and audit queries efficient and reliable.
-- HOW:  Delta stores hidden row ID and commit version columns in each Parquet
--       file. These IDs persist across rewrites, so you can always trace a
--       row back to when it was first created.
-- ============================================================================


-- ============================================================================
-- EXPLORE: The Audit Trail Overview
-- ============================================================================
-- This compliance_audit table simulates row-level tracking patterns.
-- Each row has a record_id (stable identifier), action (what happened),
-- old_value/new_value (change capture), and version_tag (lineage).

ASSERT ROW_COUNT = 4
SELECT action,
       COUNT(*) AS action_count
FROM {{zone_name}}.delta_demos.compliance_audit
GROUP BY action
ORDER BY action;


-- ============================================================================
-- EXPLORE: Tracing a Single Record Across Its Lifecycle
-- ============================================================================
-- Row tracking's key value: follow one entity through all its changes.
-- Let's trace REC-001 (Acme Corp Checking) from creation through updates.
-- Each row in the audit table captures the before and after state.

-- Verify REC-001 has a create and an update action
ASSERT ROW_COUNT = 2
SELECT id, record_id, entity_name, action,
       old_value, new_value, actor, version_tag, audit_timestamp
FROM {{zone_name}}.delta_demos.compliance_audit
WHERE record_id = 'REC-001'
ORDER BY audit_timestamp;


-- ============================================================================
-- LEARN: The Version Tag Pattern
-- ============================================================================
-- The version_tag column simulates what Delta's row tracking does at the
-- protocol level. When a row is modified, its version_tag increments,
-- making it easy to identify which rows have been touched.
--
-- version_tag=1: original state or first action
-- version_tag=2: row was updated or re-reviewed
--
-- 18 rows have version_tag >= 2 (10 updates + 8 re-reviewed entries).
-- This pattern is how CDC systems detect "what changed since last sync."

-- Verify version_tag=1 has 32 rows (original), version_tag=2 has 18 (modified)
ASSERT VALUE row_count = 32 WHERE version_tag = 1
ASSERT VALUE row_count = 18 WHERE version_tag = 2
ASSERT ROW_COUNT = 2
SELECT version_tag,
       COUNT(*) AS row_count,
       COUNT(DISTINCT record_id) AS distinct_records
FROM {{zone_name}}.delta_demos.compliance_audit
GROUP BY version_tag
ORDER BY version_tag;


-- ============================================================================
-- LEARN: Entity-Level Change Lineage
-- ============================================================================
-- In regulated systems, auditors need to see the full history of each entity.
-- Row tracking makes this possible without scanning the entire transaction log.
-- Here we show which entity types have been most actively modified:

-- Verify 4 distinct entity types with their action breakdown
ASSERT VALUE total_actions = 14 WHERE entity_type = 'account'
ASSERT VALUE total_actions = 13 WHERE entity_type = 'transaction'
ASSERT ROW_COUNT = 4
SELECT entity_type,
       COUNT(*) AS total_actions,
       COUNT(*) FILTER (WHERE action = 'create') AS creates,
       COUNT(*) FILTER (WHERE action = 'update') AS updates,
       COUNT(*) FILTER (WHERE action = 'review') AS reviews,
       COUNT(*) FILTER (WHERE action = 'delete') AS deletes
FROM {{zone_name}}.delta_demos.compliance_audit
GROUP BY entity_type
ORDER BY total_actions DESC;


-- ============================================================================
-- LEARN: Before/After Change Capture
-- ============================================================================
-- Row tracking paired with change data feed captures the exact before and
-- after values of each modification. This is critical for audit compliance
-- where you must prove what changed and who changed it.

ASSERT ROW_COUNT = 10
SELECT record_id, entity_name, action, actor,
       old_value, new_value, audit_timestamp
FROM {{zone_name}}.delta_demos.compliance_audit
WHERE action = 'update'
ORDER BY audit_timestamp;


-- ============================================================================
-- EXPLORE: Who Made Changes?
-- ============================================================================
-- Row tracking enables actor-level auditing. Combined with timestamps,
-- you get a complete chain of custody for every data modification.

-- Verify 6 distinct actors performed audit actions
ASSERT VALUE actions_performed = 14 WHERE actor = 'ops_clerk'
ASSERT VALUE records_touched = 11 WHERE actor = 'ops_clerk'
ASSERT ROW_COUNT = 6
SELECT actor,
       COUNT(*) AS actions_performed,
       COUNT(DISTINCT record_id) AS records_touched,
       MIN(audit_timestamp) AS first_action,
       MAX(audit_timestamp) AS last_action
FROM {{zone_name}}.delta_demos.compliance_audit
GROUP BY actor
ORDER BY actions_performed DESC;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================

-- Verify total_row_count: 50 audit entries total
ASSERT ROW_COUNT = 50
SELECT * FROM {{zone_name}}.delta_demos.compliance_audit;

-- Verify create_action_count: 30 create actions
ASSERT VALUE cnt = 30
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.compliance_audit WHERE action = 'create';

-- Verify update_action_count: 10 update actions
ASSERT VALUE cnt = 10
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.compliance_audit WHERE action = 'update';

-- Verify review_action_count: 5 review actions
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.compliance_audit WHERE action = 'review';

-- Verify delete_action_count: 5 delete actions
ASSERT VALUE cnt = 5
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.compliance_audit WHERE action = 'delete';

-- Verify unique_record_ids: 30 distinct record IDs
ASSERT VALUE cnt = 30
SELECT COUNT(DISTINCT record_id) AS cnt FROM {{zone_name}}.delta_demos.compliance_audit;

-- Verify version_tag_2_count: 18 rows with version_tag >= 2 (modified rows)
ASSERT VALUE cnt = 18
SELECT COUNT(*) AS cnt FROM {{zone_name}}.delta_demos.compliance_audit WHERE version_tag >= 2;

-- Verify entity_type_count: 4 distinct entity types
ASSERT VALUE cnt = 4
SELECT COUNT(DISTINCT entity_type) AS cnt FROM {{zone_name}}.delta_demos.compliance_audit;
