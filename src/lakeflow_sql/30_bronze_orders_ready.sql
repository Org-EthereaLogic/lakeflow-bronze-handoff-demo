-- ============================================================================
-- 30_bronze_orders_ready.sql
-- Contract-compliant rows safe for downstream Bronze/Silver consumers
-- ============================================================================
-- This materialized view publishes only the rows that pass ALL handoff
-- checks. It is the inverse of ops_quarantine_rows: if a row appears here,
-- every named contract rule was satisfied.
--
-- Downstream consumers should read from this view, not bronze_orders_raw.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW bronze_orders_ready
COMMENT 'Order rows that passed all handoff checks. Safe for downstream Bronze/Silver consumption.'
AS
WITH replay_batches AS (
  SELECT batch_id
  FROM LIVE.ops_batch_registry
  WHERE is_replay = true
),
ranked_files AS (
  SELECT
    r.*,
    DENSE_RANK() OVER (
      PARTITION BY r.batch_id
      ORDER BY r.file_mod_ts ASC, r.source_file ASC
    ) AS file_rank
  FROM LIVE.bronze_orders_raw r
)
SELECT
  rf.batch_id,
  rf.order_id,
  rf.customer_id,
  rf.order_total,
  rf.event_ts,
  rf.source_file,
  rf.source_file_name,
  rf.ingest_ts
FROM ranked_files rf
LEFT JOIN replay_batches rb ON rf.batch_id = rb.batch_id
WHERE
  rf.batch_id        IS NOT NULL
  AND rf.order_id    IS NOT NULL
  AND rf.customer_id IS NOT NULL
  AND rf.order_total >= 0
  AND rf.event_ts    IS NOT NULL
  AND rf._rescued_data IS NULL
  AND (rb.batch_id IS NULL OR rf.file_rank = 1);
