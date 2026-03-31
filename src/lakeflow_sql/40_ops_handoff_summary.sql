-- ============================================================================
-- 40_ops_handoff_summary.sql
-- Operational dashboard view: landed / ready / quarantined / duplicate ratios
-- ============================================================================
-- This materialized view aggregates the handoff pipeline outputs into a
-- single summary suitable for operational dashboards and alerting.
--
-- Key metrics:
--   total_landed       — all rows ingested by Auto Loader
--   total_ready        — rows that passed all handoff checks
--   total_quarantined  — rows that failed at least one check
--   total_rescued      — rows with non-null _rescued_data (schema drift)
--   total_duplicate    — rows from replayed batches (non-first file)
--   ready_pct          — percentage of landed rows that are ready
--   quarantine_pct     — percentage of landed rows that are quarantined
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ops_handoff_summary
COMMENT 'Aggregated handoff metrics: landed, ready, quarantined, rescued, and duplicate counts with ratios.'
AS
WITH landed AS (
  SELECT COUNT(*) AS total_landed
  FROM LIVE.bronze_orders_raw
),
ready AS (
  SELECT COUNT(*) AS total_ready
  FROM LIVE.bronze_orders_ready
),
quarantined AS (
  SELECT COUNT(*) AS total_quarantined
  FROM LIVE.ops_quarantine_rows
),
rescued AS (
  SELECT COUNT(*) AS total_rescued
  FROM LIVE.ops_quarantine_rows
  WHERE ARRAY_CONTAINS(quarantine_reasons, 'rescued_data_empty')
),
duplicates AS (
  SELECT COUNT(*) AS total_duplicate
  FROM LIVE.ops_quarantine_rows
  WHERE ARRAY_CONTAINS(quarantine_reasons, 'not_duplicate_batch')
)
SELECT
  l.total_landed,
  r.total_ready,
  q.total_quarantined,
  res.total_rescued,
  d.total_duplicate,
  ROUND(r.total_ready * 100.0 / NULLIF(l.total_landed, 0), 2)        AS ready_pct,
  ROUND(q.total_quarantined * 100.0 / NULLIF(l.total_landed, 0), 2)  AS quarantine_pct,
  ROUND(res.total_rescued * 100.0 / NULLIF(l.total_landed, 0), 2)    AS rescued_pct,
  ROUND(d.total_duplicate * 100.0 / NULLIF(l.total_landed, 0), 2)    AS duplicate_pct
FROM landed l
CROSS JOIN ready r
CROSS JOIN quarantined q
CROSS JOIN rescued res
CROSS JOIN duplicates d;
