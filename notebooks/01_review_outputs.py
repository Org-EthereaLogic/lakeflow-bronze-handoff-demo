# Databricks notebook source
# MAGIC %md
# MAGIC # Review Pipeline Outputs
# MAGIC
# MAGIC Run this notebook after a pipeline refresh to inspect the Bronze handoff
# MAGIC results: raw ingest counts, batch registry, quarantine details, ready
# MAGIC rows, and the operational summary.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Handoff Summary — the single-pane operational view

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM main.bronze_handoff_demo.ops_handoff_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Batch Registry — per-batch metadata and replay flags

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   batch_id,
# MAGIC   batch_first_seen_ts,
# MAGIC   file_count,
# MAGIC   total_rows,
# MAGIC   is_replay,
# MAGIC   source_files
# MAGIC FROM main.bronze_handoff_demo.ops_batch_registry
# MAGIC ORDER BY batch_first_seen_ts;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quarantine Details — failed rows with reasons

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   batch_id,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_total,
# MAGIC   event_ts,
# MAGIC   quarantine_reasons,
# MAGIC   source_file_name
# MAGIC FROM main.bronze_handoff_demo.ops_quarantine_rows
# MAGIC ORDER BY batch_id, source_file_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ready Rows — contract-compliant orders for downstream

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   batch_id,
# MAGIC   COUNT(*) AS ready_rows
# MAGIC FROM main.bronze_handoff_demo.bronze_orders_ready
# MAGIC GROUP BY batch_id
# MAGIC ORDER BY batch_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Raw Ingest — full row counts by batch and file

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   batch_id,
# MAGIC   source_file_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   MIN(ingest_ts) AS first_ingest,
# MAGIC   SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS rescued_rows
# MAGIC FROM main.bronze_handoff_demo.bronze_orders_raw
# MAGIC GROUP BY batch_id, source_file_name
# MAGIC ORDER BY batch_id, source_file_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Event Log — flow progress and expectation metrics
# MAGIC
# MAGIC Replace `<pipeline-id>` below with the ID from the pipeline details page or
# MAGIC the update URL after you launch a refresh. Run these cells on a shared
# MAGIC cluster or SQL warehouse.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW event_log_raw AS
# MAGIC SELECT * FROM event_log('<pipeline-id>');
# MAGIC
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   level,
# MAGIC   event_type,
# MAGIC   message
# MAGIC FROM event_log_raw
# MAGIC WHERE event_type IN ('create_update', 'flow_progress')
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW latest_update AS
# MAGIC SELECT origin.update_id AS id
# MAGIC FROM event_log_raw
# MAGIC WHERE event_type = 'create_update'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 1;
# MAGIC
# MAGIC WITH expectations_parsed AS (
# MAGIC   SELECT
# MAGIC     explode(
# MAGIC       from_json(
# MAGIC         details:flow_progress:data_quality:expectations,
# MAGIC         'array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>'
# MAGIC       )
# MAGIC     ) AS row_expectation
# MAGIC   FROM event_log_raw, latest_update
# MAGIC   WHERE event_type = 'flow_progress'
# MAGIC     AND origin.update_id = latest_update.id
# MAGIC )
# MAGIC SELECT
# MAGIC   row_expectation.dataset AS dataset,
# MAGIC   row_expectation.name AS expectation,
# MAGIC   SUM(row_expectation.passed_records) AS passing_records,
# MAGIC   SUM(row_expectation.failed_records) AS failing_records
# MAGIC FROM expectations_parsed
# MAGIC GROUP BY row_expectation.dataset, row_expectation.name
# MAGIC ORDER BY dataset, expectation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Results Summary
# MAGIC
# MAGIC | Batch | Scenario | Expected Outcome |
# MAGIC |-------|----------|-----------------|
# MAGIC | B-001 | Clean baseline | All 10 rows in `bronze_orders_ready` |
# MAGIC | B-002 | Schema drift | Rescued/quarantined rows for type mismatch, new column, case mismatch |
# MAGIC | B-001 (replay) | Duplicate replay | Flagged as replay in batch registry, blocked from ready |
# MAGIC | B-004 | Partial payload | All 5 rows quarantined (null fields, negative total, null batch_id) |
