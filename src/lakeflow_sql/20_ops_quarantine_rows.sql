-- ============================================================================
-- 20_ops_quarantine_rows.sql
-- Operational view: rows that failed one or more handoff checks
-- ============================================================================
-- A row is quarantined when any of the following contract rules fail:
--
--   required_batch_id       — batch_id must not be null
--   required_order_id       — order_id must not be null
--   required_customer_id    — customer_id must not be null
--   non_negative_order_total — order_total must not be null and must be >= 0
--   valid_event_ts          — event_ts must not be null
--   rescued_data_empty      — _rescued_data must be null (no drift)
--   not_duplicate_batch     — batch_id must not be flagged as replay
--
-- Quarantine is explicit: bad rows remain inspectable rather than silently
-- dropped. Downstream consumers should trust bronze_orders_ready, not this.
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW ops_quarantine_rows
COMMENT 'Rows that failed at least one handoff check. Kept for inspection, not downstream consumption.'
AS
WITH replay_batches AS (
  SELECT batch_id
  FROM LIVE.ops_batch_registry
  WHERE is_replay = true
),
ranked_files AS (
  -- For replay detection: identify rows from non-first files per batch_id
  SELECT
    r.*,
    DENSE_RANK() OVER (
      PARTITION BY r.batch_id
      ORDER BY r.file_mod_ts ASC, r.source_file ASC
    ) AS file_rank
  FROM LIVE.bronze_orders_raw r
),
quarantine_checks AS (
  SELECT
    rf.*,
    CASE WHEN rf.batch_id IS NULL             THEN 'required_batch_id' END       AS chk_batch_id,
    CASE WHEN rf.order_id IS NULL             THEN 'required_order_id' END       AS chk_order_id,
    CASE WHEN rf.customer_id IS NULL          THEN 'required_customer_id' END    AS chk_customer_id,
    CASE WHEN rf.order_total IS NULL OR rf.order_total < 0 THEN 'non_negative_order_total' END AS chk_total,
    CASE WHEN rf.event_ts IS NULL             THEN 'valid_event_ts' END          AS chk_event_ts,
    CASE WHEN rf._rescued_data IS NOT NULL    THEN 'rescued_data_empty' END      AS chk_rescued,
    CASE
      WHEN rb.batch_id IS NOT NULL AND rf.file_rank > 1
      THEN 'not_duplicate_batch'
    END                                                                           AS chk_replay
  FROM ranked_files rf
  LEFT JOIN replay_batches rb ON rf.batch_id = rb.batch_id
)
SELECT
  batch_id,
  order_id,
  customer_id,
  order_total,
  event_ts,
  _rescued_data,
  source_file,
  source_file_name,
  ingest_ts,
  ARRAY_COMPACT(ARRAY(
    chk_batch_id,
    chk_order_id,
    chk_customer_id,
    chk_total,
    chk_event_ts,
    chk_rescued,
    chk_replay
  )) AS quarantine_reasons
FROM quarantine_checks
WHERE
  chk_batch_id   IS NOT NULL OR
  chk_order_id   IS NOT NULL OR
  chk_customer_id IS NOT NULL OR
  chk_total       IS NOT NULL OR
  chk_event_ts    IS NOT NULL OR
  chk_rescued     IS NOT NULL OR
  chk_replay      IS NOT NULL;
