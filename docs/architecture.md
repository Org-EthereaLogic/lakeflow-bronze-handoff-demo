# Architecture — Lakeflow Bronze Handoff Demo

Author: Anthony Johnson | EthereaLogic LLC

## Overview

This document describes the data flow and design rationale for the Bronze handoff pattern implemented in this repo.

## Data flow

```
┌─────────────────────────────┐
│   Source System              │
│   (daily order events)       │
└──────────┬──────────────────┘
           │  JSON files landed
           ▼
┌─────────────────────────────┐
│   Unity Catalog Volume       │
│   /Volumes/.../landing       │
└──────────┬──────────────────┘
           │  Auto Loader (STREAM read_files)
           │  - directory listing mode
           │  - rescued data for drift
           ▼
┌─────────────────────────────┐
│   bronze_orders_raw          │
│   (streaming table)          │
│   + source_file metadata     │
│   + ingest_ts                │
│   + _rescued_data            │
└──────────┬──────────────────┘
           │
     ┌─────┴───────────────┐
     │                     │
     ▼                     ▼
┌──────────────┐   ┌──────────────────┐
│ ops_batch    │   │ Handoff checks   │
│ _registry    │   │ (7 named rules)  │
│ (MV)         │   │                  │
└──────┬───────┘   └────────┬─────────┘
       │                    │
       │              ┌─────┴──────┐
       │              │            │
       │              ▼            ▼
       │    ┌──────────────┐ ┌──────────────┐
       │    │ bronze_orders│ │ ops_quarantine│
       │    │ _ready (MV)  │ │ _rows (MV)   │
       │    └──────────────┘ └──────────────┘
       │              │            │
       └──────────────┴────────────┘
                      │
                      ▼
              ┌──────────────────┐
              │ ops_handoff      │
              │ _summary (MV)   │
              └──────────────────┘
```

## Layer responsibilities

### bronze_orders_raw (streaming table)

Auto Loader ingests JSON files incrementally from the landing volume. Schema inference runs with rescued-data mode enabled so that unexpected columns, type mismatches, and case drift appear in `_rescued_data` rather than silently expanding the trusted schema. File-level metadata (`source_file`, `file_mod_ts`) is captured for traceability.

### ops_batch_registry (materialized view)

Groups raw rows by `batch_id` and `source_file` to produce batch-level operational metadata. Tracks first-seen timestamps, file counts per batch, and flags replays (same batch_id arriving from multiple files). This view exists because Auto Loader's file-level exactly-once guarantees do not cover business-level replay scenarios like renamed re-sends.

### ops_quarantine_rows (materialized view)

Materializes every row that fails at least one of the seven handoff contract rules. Each quarantined row carries an array of failed rule names so that operators can inspect why a row was rejected without re-running the pipeline.

### bronze_orders_ready (materialized view)

The inverse of quarantine: only rows that pass all seven handoff checks. This is the only view that downstream Bronze/Silver consumers should read from. It excludes rows with rescued data, null required fields, negative totals, and duplicate batch replays.

### ops_handoff_summary (materialized view)

Aggregates the full pipeline into a single operational summary: total landed, total ready, total quarantined, total rescued, total duplicate, and percentage ratios. Designed for dashboards and alerting.

## Contract rules

| Rule | Condition | Behavior |
|------|-----------|----------|
| required_batch_id | batch_id IS NULL | quarantine |
| required_order_id | order_id IS NULL | quarantine |
| required_customer_id | customer_id IS NULL | quarantine |
| non_negative_order_total | order_total IS NULL OR < 0 | quarantine |
| valid_event_ts | event_ts IS NULL | quarantine |
| rescued_data_empty | _rescued_data IS NOT NULL | quarantine |
| not_duplicate_batch | batch replayed beyond first file | quarantine |

## Design choices

### Schema drift is visible, not invisible

Auto Loader's rescue behavior exists precisely because new columns, type mismatches, and case mismatches should remain visible until someone decides they are trusted. This repo does not use automatic schema expansion as the happy path.

### Quarantine over silent drop

Bad rows remain in an inspectable view rather than being silently discarded. This improves downstream trust because operators can always explain why a row was rejected.

### Batch registry for business-level replay

Auto Loader tracks files via checkpoint state, which prevents the same file from being processed twice. But a renamed file with the same business batch_id is a new file to Auto Loader. The batch registry provides the business-level layer that file-level guarantees cannot.

## Production guidance

For production deployments beyond this demo:

- Switch Auto Loader to managed file events for scale (millions of files/hour)
- Use Unity Catalog volume paths for file discovery
- Store checkpoints in Unity Catalog-managed storage
- Add alerting on quarantine spike thresholds
- Publish ops_handoff_summary to operational dashboards
- Use environment-specific catalog/schema targets
