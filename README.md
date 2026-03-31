# Lakeflow Bronze Handoff Demo with Auto Loader, Quarantine, and Replay Protection

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/d1c01009838a47559b921ee956ce7376)](https://app.codacy.com/gh/Org-EthereaLogic/lakeflow-bronze-handoff-demo?utm_source=github.com&utm_medium=referral&utm_content=Org-EthereaLogic/lakeflow-bronze-handoff-demo&utm_campaign=Badge_Grade)

Built by Anthony Johnson | EthereaLogic LLC

Most public Databricks demos start after data is already inside Bronze. This project focuses on the messier boundary that enterprise teams actually struggle with first: landed files, schema drift, rescued data, duplicate batch replay, and quarantine controls before downstream users trust Bronze outputs.

This demo uses Lakeflow Spark Declarative Pipelines, Auto Loader, Unity Catalog, and Declarative Automation Bundles (formerly Databricks Asset Bundles) to show a practical Bronze handoff pattern that is public-safe, testable, and easy to review.

## What this repo demonstrates

- Incremental file ingestion from a Unity Catalog volume using Auto Loader
- Lakeflow streaming-table ingestion for raw Bronze capture
- Expectation-style handoff checks for required fields and contract validity
- Rescued-data visibility for schema/type drift
- Duplicate replay protection at the business batch level
- Quarantine outputs for invalid or untrusted rows
- Operational summary views for landed, ready, quarantined, and duplicate records
- Source-controlled bundle deployment and pipeline refresh

## Why this matters

Auto Loader already helps ingest files efficiently and reliably, but real enterprise ingestion still needs business-level controls:

- Was this batch already sent once?
- Did the source quietly introduce a new column?
- Did type drift get rescued instead of parsed cleanly?
- Are required business fields missing?
- Can downstream Bronze/Silver consumers trust this batch yet?

This repo answers those questions with a small, clear demo.

## Architecture

```
Landing Volume
  → Auto Loader / raw ingest
  → bronze_orders_raw
  → batch registry + quarantine rules
  → bronze_orders_ready
  → ops_handoff_summary
```

See [docs/architecture.md](docs/architecture.md) for the full diagram and design rationale.

## Runtime posture

- `serverless: true` is enabled for the pipeline resource, so the demo aligns with the current Databricks recommendation to start new pipelines on serverless.
- Published datasets live in Unity Catalog (`catalog.schema.*`), and landed files are staged in Unity Catalog volumes instead of ad hoc workspace storage.
- The bundle includes both `dev` and `prod` targets so reviewers can see the intended promotion model, even though the public demo defaults to the `dev` target.

## Demo scenarios

The sample data includes four landed batches:

1. **batch_001_good** — Clean rows that should pass all handoff checks.
2. **batch_002_schema_drift** — New/changed fields that should appear as rescued or quarantined.
3. **batch_003_duplicate_replay** — A replay of an already-seen business batch under a different file name.
4. **batch_004_partial_payload** — Rows with missing required fields.

## Published outputs

This pipeline publishes the following objects in Unity Catalog:

- `catalog.schema.bronze_orders_raw` — streaming table, raw Auto Loader ingest
- `catalog.schema.ops_batch_registry` — materialized view, batch-level operational metadata
- `catalog.schema.ops_quarantine_rows` — materialized view, rows that failed handoff checks
- `catalog.schema.bronze_orders_ready` — materialized view, contract-compliant rows for downstream
- `catalog.schema.ops_handoff_summary` — materialized view, landed/ready/quarantined/duplicate ratios

## Repo layout

```text
lakeflow_bronze_handoff_demo/
├── databricks.yml
├── resources/
│   ├── bronze_handoff.pipeline.yml
│   └── refresh_demo.job.yml
├── src/
│   ├── lakeflow_sql/
│   │   ├── 00_bronze_orders_raw.sql
│   │   ├── 10_ops_batch_registry.sql
│   │   ├── 20_ops_quarantine_rows.sql
│   │   ├── 30_bronze_orders_ready.sql
│   │   └── 40_ops_handoff_summary.sql
│   └── bronze_handoff_demo/
│       ├── __init__.py
│       ├── sample_data.py
│       ├── manifests.py
│       └── rules.py
├── notebooks/
│   ├── 00_seed_demo_files.py
│   └── 01_review_outputs.py
├── data/sample/
├── docs/
├── tests/
└── .github/workflows/ci.yml
```

## Local development

This repo includes a small Python utility package for sample-data generation, docs visuals, and tests.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev,docs]"
pytest -q
ruff check src tests docs
```

## Databricks quickstart

### 1. Authenticate

Configure Databricks CLI access to your workspace.
If you use a named Databricks CLI profile, set it for the session before running
bundle commands:

```bash
export DATABRICKS_CONFIG_PROFILE=<profile-name>
```

You can also pass `--profile <profile-name>` to each Databricks CLI command instead.

### 2. Review bundle variables

Update these in `databricks.yml` as needed:

- `catalog`
- `schema`
- `landing_path`
- `checkpoint_root`

### 3. Validate the bundle

```bash
databricks bundle validate --target dev
```

### 4. Deploy to your dev target

```bash
databricks bundle deploy --target dev
```

### 5. Seed sample files

Run the `notebooks/00_seed_demo_files.py` notebook to copy the sample batches into the configured landing path.

### 6. Run the pipeline

```bash
databricks pipelines run --target dev
```

### 7. Review outputs

Open the pipeline update URL from the CLI, then query:

```sql
SELECT * FROM main.bronze_handoff_demo.ops_handoff_summary;

SELECT batch_id, quarantine_reasons, source_file_name
FROM main.bronze_handoff_demo.ops_quarantine_rows
ORDER BY batch_id, source_file_name;

SELECT batch_id, count(*) AS ready_rows
FROM main.bronze_handoff_demo.bronze_orders_ready
GROUP BY batch_id
ORDER BY batch_id;
```

## Event log queries

Databricks treats the pipeline event log as the primary observability surface for updates, quality metrics, and flow progress. After a refresh, capture the pipeline ID from the update URL or the pipeline details page, create a temporary view for that event log on a shared cluster or SQL warehouse, then run queries like:

```sql
CREATE OR REPLACE TEMP VIEW event_log_raw AS
SELECT * FROM event_log('<pipeline-id>');

SELECT timestamp, level, event_type, message
FROM event_log_raw
WHERE event_type IN ('create_update', 'flow_progress')
ORDER BY timestamp DESC
LIMIT 50;
```

```sql
CREATE OR REPLACE TEMP VIEW latest_update AS
SELECT origin.update_id AS id
FROM event_log_raw
WHERE event_type = 'create_update'
ORDER BY timestamp DESC
LIMIT 1;

WITH expectations_parsed AS (
  SELECT
    explode(
      from_json(
        details:flow_progress:data_quality:expectations,
        'array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>'
      )
    ) AS row_expectation
  FROM event_log_raw, latest_update
  WHERE event_type = 'flow_progress'
    AND origin.update_id = latest_update.id
)
SELECT
  row_expectation.dataset AS dataset,
  row_expectation.name AS expectation,
  SUM(row_expectation.passed_records) AS passing_records,
  SUM(row_expectation.failed_records) AS failing_records
FROM expectations_parsed
GROUP BY row_expectation.dataset, row_expectation.name
ORDER BY dataset, expectation;
```

## Expected results

- **batch_001_good** lands in `bronze_orders_ready`
- **batch_002_schema_drift** produces rescued/quarantined rows
- **batch_003_duplicate_replay** is flagged in the batch registry and blocked from ready output
- **batch_004_partial_payload** lands in quarantine
- **ops_handoff_summary** shows the ready/quarantine split clearly

## Design choices

### Why Auto Loader?

Because file-based ingestion should be incremental, restart-safe, and operationally visible.

### Why a Unity Catalog volume?

Because the demo should look like a governed Databricks project, not an ad hoc notebook.

### Why quarantine instead of silent drop?

Because downstream trust improves when bad rows remain inspectable.

### Why a batch registry?

Because file-level ingestion guarantees do not replace business-level replay controls.

## Public-safe boundaries

This repo is intentionally public-safe. It does **not** include:

- Customer data
- Client identifiers
- Proprietary formulas
- Production secrets
- Enterprise-specific IAM or network configuration

## Production notes

This demo defaults to the simplest runnable path. For production, I would typically:

- Switch from simple directory discovery to Databricks file events / Auto Loader file notification mode for scale
- Use environment-specific catalogs and schemas
- Separate landing, checkpoint, and published paths cleanly
- Add alerting on quarantine spikes and replay detection
- Publish operational dashboards from `ops_handoff_summary`

## Future enhancements

- Expectation rules stored in Unity Catalog tables
- Scheduled refresh job with notifications
- Change-data-capture extension
- Silver handoff contract
- Dashboard for quarantine trends and replay rate
