"""
rules — Handoff contract rule definitions for the Bronze demo.

Each rule is a named callable that takes a single row dict and returns
True if the row passes the check, False if it should be quarantined.
Rules can be combined, serialized, and tested independently of the
Lakeflow SQL pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class HandoffRule:
    """A single named handoff check."""

    name: str
    description: str
    check: Callable[[dict[str, Any]], bool]


def required_batch_id(row: dict[str, Any]) -> bool:
    return row.get("batch_id") is not None


def required_order_id(row: dict[str, Any]) -> bool:
    return row.get("order_id") is not None


def required_customer_id(row: dict[str, Any]) -> bool:
    return row.get("customer_id") is not None


def non_negative_order_total(row: dict[str, Any]) -> bool:
    total = row.get("order_total")
    if total is None:
        return False
    try:
        return float(total) >= 0
    except (ValueError, TypeError):
        return False


def valid_event_ts(row: dict[str, Any]) -> bool:
    return row.get("event_ts") is not None


def rescued_data_empty(row: dict[str, Any]) -> bool:
    return row.get("_rescued_data") is None


def not_duplicate_batch(row: dict[str, Any]) -> bool:
    """Check that a row is not from a replayed batch delivery.

    Note: Full replay detection requires batch-registry context (file rank
    within a batch_id).  In the SQL pipeline this is resolved via a JOIN to
    ops_batch_registry.  The Python callable uses an ``_is_replay_file``
    flag that must be set by the caller when batch-level context is available.
    Without that flag the rule passes by default.
    """
    return not row.get("_is_replay_file", False)


# ── Registry ────────────────────────────────────────────────────────────────

HANDOFF_RULES: list[HandoffRule] = [
    HandoffRule("required_batch_id", "batch_id must not be null", required_batch_id),
    HandoffRule("required_order_id", "order_id must not be null", required_order_id),
    HandoffRule("required_customer_id", "customer_id must not be null", required_customer_id),
    HandoffRule(
        "non_negative_order_total",
        "order_total must not be null and must be >= 0",
        non_negative_order_total,
    ),
    HandoffRule("valid_event_ts", "event_ts must not be null", valid_event_ts),
    HandoffRule(
        "rescued_data_empty",
        "_rescued_data must be null (no schema drift)",
        rescued_data_empty,
    ),
    HandoffRule(
        "not_duplicate_batch",
        "row must not come from a replayed batch (non-first file)",
        not_duplicate_batch,
    ),
]


def evaluate_row(row: dict[str, Any]) -> list[str]:
    """Return list of failed rule names for a single row (empty = pass)."""
    return [rule.name for rule in HANDOFF_RULES if not rule.check(row)]


def evaluate_batch(rows: list[dict[str, Any]]) -> dict[int, list[str]]:
    """Evaluate all rows, returning {row_index: [failed_rules]} for failures only."""
    failures: dict[int, list[str]] = {}
    for i, row in enumerate(rows):
        failed = evaluate_row(row)
        if failed:
            failures[i] = failed
    return failures
