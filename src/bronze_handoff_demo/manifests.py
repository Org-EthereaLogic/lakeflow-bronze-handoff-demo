"""
manifests — Batch manifest utilities for tracking landed files.

In production, a manifest file or catalog table would accompany each
landed batch to declare expected row counts, schema version, and
business metadata. This module provides helpers for generating and
validating those manifests in the demo context.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


@dataclass
class BatchManifest:
    """Metadata record for a single landed batch."""

    batch_id: str
    source_system: str = "demo_orders"
    schema_version: str = "1.0.0"
    expected_row_count: int = 0
    landed_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    )

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, raw: str) -> "BatchManifest":
        return cls(**json.loads(raw))


def validate_manifest(manifest: BatchManifest, actual_row_count: int) -> list[str]:
    """Return a list of validation failures (empty list = pass)."""
    issues: list[str] = []
    if not manifest.batch_id:
        issues.append("batch_id is missing")
    if manifest.expected_row_count > 0 and actual_row_count != manifest.expected_row_count:
        issues.append(
            f"row count mismatch: expected {manifest.expected_row_count}, "
            f"got {actual_row_count}"
        )
    return issues
