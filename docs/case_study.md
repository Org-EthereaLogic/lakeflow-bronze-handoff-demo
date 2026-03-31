# Case Study — Bronze Handoff Controls for Enterprise File Ingestion

Author: Anthony Johnson | EthereaLogic LLC

## The problem

Most Databricks demos and quickstart guides begin after data is already inside Bronze. But the messiest operational problems happen at the boundary between landed files and trusted Bronze tables:

- Source systems quietly add, rename, or retype columns.
- Operations teams resend a batch under a new file name after a perceived failure.
- Required business fields arrive as nulls because an upstream extract job partially failed.
- Downstream consumers start querying Bronze before anyone has validated the batch.

These problems are not theoretical. They are the daily reality of enterprise data engineering, and they are the problems that erode trust in a data platform faster than any Silver or Gold bug.

## The approach

This project implements a **Bronze handoff pattern** that sits between Auto Loader's raw ingest and the point where downstream consumers are allowed to trust the data.

The pattern has four components:

1. **Raw ingest with rescued data** — Auto Loader captures everything, including schema drift, into a streaming table. Drift is rescued into a dedicated column rather than silently absorbed into the schema.

2. **Batch registry** — A materialized view that groups ingested rows by business batch_id and tracks first-seen timestamps, file counts, and replay flags. This provides business-level deduplication on top of Auto Loader's file-level guarantees.

3. **Quarantine rules** — Seven named contract checks that every row must pass before it reaches the ready output. Failed rows are quarantined with explicit reason arrays, not silently dropped.

4. **Handoff summary** — A single materialized view that aggregates the full pipeline into operational metrics: landed, ready, quarantined, rescued, and duplicate counts with ratios.

## The demo scenarios

The sample data includes four landed batches that exercise the four most common enterprise ingestion problems:

| Batch | Scenario | What breaks | Expected outcome |
|-------|----------|-------------|-----------------|
| B-001 | Clean baseline | Nothing | All rows pass to ready |
| B-002 | Schema drift | New column, type mismatch, case mismatch | Rescued/quarantined rows |
| B-001 (replay) | Duplicate replay | Same batch_id, different file name | Flagged and blocked |
| B-004 | Partial payload | Null required fields, negative total | All rows quarantined |

## The result

After a single pipeline refresh, the `ops_handoff_summary` view shows the ready/quarantine split clearly. An operator can immediately see how many rows passed, how many were quarantined and why, whether any batches were replayed, and whether schema drift occurred.

This is the operational visibility that enterprise teams need before they can trust Bronze outputs for downstream processing.

## Positioning

This repo is **Project 3** in a three-project public portfolio:

- **Project 1** (entropy_governed_medallion_demo) — governed medallion flow
- **Project 2** (entropy_quality_drift_benchmark) — benchmarked quality and drift validation
- **Project 3** (lakeflow_bronze_handoff_demo) — operational landing-to-Bronze control

Together, the three projects tell a complete story: from governed pipeline architecture, through quality enforcement, to the operational boundary where files become trusted data.
