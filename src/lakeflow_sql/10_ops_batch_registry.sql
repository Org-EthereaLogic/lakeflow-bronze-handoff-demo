-- ============================================================================
-- 10_ops_batch_registry.sql
-- Operational view: batch-level metadata, first-seen tracking, replay flags
-- ============================================================================
-- This materialized view builds a batch-level operational registry from the
-- raw ingest table. It keys on batch_id and tracks:
--   - How many files delivered that batch_id
--   - When the batch was first seen
--   - How many total rows arrived per batch
--   - Whether the batch has been replayed (seen more than once)
--
-- File-level ingestion guarantees (Auto Loader checkpoint) do not replace
-- business-level replay controls. A renamed file with the same batch_id
-- is a new file to Auto Loader but a duplicate to the business.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ops_batch_registry
COMMENT 'Batch-level registry showing first-seen timestamps, file counts, row counts, and replay flags.'
AS
WITH batch_files AS (
  SELECT
    batch_id,
    source_file,
    MIN(ingest_ts)   AS first_seen_ts,
    COUNT(*)         AS row_count
  FROM LIVE.bronze_orders_raw
  GROUP BY batch_id, source_file
),
batch_summary AS (
  SELECT
    batch_id,
    MIN(first_seen_ts)                        AS batch_first_seen_ts,
    COUNT(DISTINCT source_file)               AS file_count,
    SUM(row_count)                            AS total_rows,
    COLLECT_SET(source_file)                  AS source_files
  FROM batch_files
  GROUP BY batch_id
)
SELECT
  batch_id,
  batch_first_seen_ts,
  file_count,
  total_rows,
  source_files,
  CASE
    WHEN file_count > 1 THEN true
    ELSE false
  END AS is_replay
FROM batch_summary;
