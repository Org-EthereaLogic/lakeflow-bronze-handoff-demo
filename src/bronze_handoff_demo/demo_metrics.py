"""
demo_metrics — Derive reproducible demo counts from the sample batches.

The docs visuals should not hardcode operational numbers. This module builds
the counts from the repo's sample files plus the documented handoff rules so
chart labels stay aligned with the demo data.

This is a local model of the documented pipeline behavior. It does not replace
live Databricks verification, but it does make the repo's own published
numbers traceable and testable.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from .rules import evaluate_row
from .sample_data import ALL_BATCHES

BASELINE_SCHEMA_TYPES: dict[str, tuple[type[Any], ...]] = {
    "batch_id": (str,),
    "order_id": (str,),
    "customer_id": (str,),
    "order_total": (int, float),
    "event_ts": (str,),
    "region": (str,),
}

REQUIRED_FIELD_RULES = {
    "required_batch_id",
    "required_order_id",
    "required_customer_id",
    "valid_event_ts",
}
ORDER_TOTAL_RULES = {"non_negative_order_total"}
DRIFT_RULES = {"rescued_data_empty"}
REPLAY_RULES = {"not_duplicate_batch"}


def _is_expected_type(field_name: str, value: Any) -> bool:
    return isinstance(value, BASELINE_SCHEMA_TYPES[field_name])


def _materialize_raw_row(
    row: dict[str, Any], source_file: str, delivery_order: int, row_number: int
) -> dict[str, Any]:
    """Model the raw table row after Auto Loader rescue semantics."""
    raw_row: dict[str, Any] = {field: None for field in BASELINE_SCHEMA_TYPES}
    rescued_fields: dict[str, Any] = {}

    for field_name, value in row.items():
        if field_name not in BASELINE_SCHEMA_TYPES:
            rescued_fields[field_name] = value
            continue

        if value is None:
            raw_row[field_name] = None
            continue

        if _is_expected_type(field_name, value):
            raw_row[field_name] = float(value) if field_name == "order_total" else value
            continue

        raw_row[field_name] = None
        rescued_fields[field_name] = value

    raw_row["_rescued_data"] = (
        json.dumps(rescued_fields, sort_keys=True) if rescued_fields else None
    )
    raw_row["source_file"] = source_file
    raw_row["source_file_name"] = Path(source_file).name
    raw_row["_delivery_order"] = delivery_order
    raw_row["_row_number"] = row_number
    return raw_row


def build_demo_pipeline_rows() -> list[dict[str, Any]]:
    """Return deterministic row-level records with simulated rescue/replay flags."""
    rows: list[dict[str, Any]] = []

    for delivery_order, (source_file, generator) in enumerate(ALL_BATCHES.items()):
        for row_number, row in enumerate(generator(), start=1):
            rows.append(
                _materialize_raw_row(
                    row=row,
                    source_file=source_file,
                    delivery_order=delivery_order,
                    row_number=row_number,
                )
            )

    batch_file_order: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        batch_id = row["batch_id"]
        if batch_id is None:
            continue
        if row["source_file"] not in batch_file_order[batch_id]:
            batch_file_order[batch_id].append(row["source_file"])

    replay_ranks = {
        (batch_id, source_file): rank
        for batch_id, source_files in batch_file_order.items()
        for rank, source_file in enumerate(source_files, start=1)
    }

    for row in rows:
        batch_id = row["batch_id"]
        source_file = row["source_file"]
        file_rank = replay_ranks.get((batch_id, source_file), 1)
        is_replay_batch = batch_id is not None and len(batch_file_order[batch_id]) > 1
        row["file_rank"] = file_rank
        row["_is_replay_file"] = is_replay_batch and file_rank > 1
        row["failed_rules"] = evaluate_row(row)

    return rows


def compute_quarantine_funnel() -> list[tuple[str, int]]:
    """Return the staged row counts used by the docs funnel chart."""
    rows = build_demo_pipeline_rows()

    passed_required = [
        row for row in rows if not (set(row["failed_rules"]) & REQUIRED_FIELD_RULES)
    ]
    passed_order_total = [
        row for row in passed_required if not (set(row["failed_rules"]) & ORDER_TOTAL_RULES)
    ]
    passed_drift = [
        row for row in passed_order_total if not (set(row["failed_rules"]) & DRIFT_RULES)
    ]
    passed_replay = [
        row for row in passed_drift if not (set(row["failed_rules"]) & REPLAY_RULES)
    ]

    return [
        ("Landed", len(rows)),
        ("Passed Required\nFields", len(passed_required)),
        ("Passed Order\nTotal Check", len(passed_order_total)),
        ("Passed Drift\nCheck", len(passed_drift)),
        ("Passed Replay\nCheck", len(passed_replay)),
        ("Ready Output", len(passed_replay)),
    ]


def compute_demo_summary() -> dict[str, int]:
    """Return aggregate counts that mirror the operational summary view."""
    rows = build_demo_pipeline_rows()

    return {
        "total_landed": len(rows),
        "total_ready": sum(1 for row in rows if not row["failed_rules"]),
        "total_quarantined": sum(1 for row in rows if row["failed_rules"]),
        "total_rescued": sum(
            1 for row in rows if "rescued_data_empty" in row["failed_rules"]
        ),
        "total_duplicate": sum(
            1 for row in rows if "not_duplicate_batch" in row["failed_rules"]
        ),
    }
