-- ============================================================================
-- 00_bronze_orders_raw.sql
-- Raw streaming table: Auto Loader ingest from Unity Catalog volume
-- ============================================================================
-- Auto Loader reads landed JSON files incrementally from the configured
-- volume path. Schema inference is enabled with rescued-data mode so that
-- unexpected columns, type mismatches, and case drift remain visible in
-- the _rescued_data column rather than silently expanding the schema.
--
-- This is intentional: schema drift should be inspectable, not invisible.
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_orders_raw
COMMENT 'Raw order events ingested by Auto Loader from the landing volume. Schema drift is rescued, not silently absorbed.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  *,
  _metadata.file_path        AS source_file,
  regexp_extract(_metadata.file_path, '[^/]+$') AS source_file_name,
  _metadata.file_modification_time AS file_mod_ts,
  current_timestamp()         AS ingest_ts
FROM STREAM read_files(
  '${demo.landing_path}',
  format          => 'json',
  inferColumnTypes => true,
  rescuedDataColumn => '_rescued_data',
  schemaEvolutionMode => 'rescue'
);
