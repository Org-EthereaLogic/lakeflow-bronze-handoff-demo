"""
sample_data — Generate sample order-event JSON batches for the Bronze handoff demo.

Each batch exercises a specific operational scenario:
  batch_001_good             — clean baseline rows
  batch_002_schema_drift     — new column, type mismatch, case mismatch
  batch_003_duplicate_replay — same batch_id resent under a new file name
  batch_004_partial_payload  — missing required business fields
"""

from __future__ import annotations

import json
import pathlib
from datetime import datetime, timedelta
from typing import Any


def _ts(base: str, offset_hours: int = 0) -> str:
    """Return an ISO-8601 timestamp string offset from a base date."""
    dt = datetime.fromisoformat(base) + timedelta(hours=offset_hours)
    return dt.isoformat() + "Z"


BASE_DATE = "2026-03-01T08:00:00"


def batch_001_good() -> list[dict[str, Any]]:
    """Clean baseline rows — all required fields present, valid values."""
    return [
        {
            "batch_id": "B-001",
            "order_id": f"ORD-{i:04d}",
            "customer_id": f"CUST-{100 + i}",
            "order_total": round(49.99 + i * 12.50, 2),
            "event_ts": _ts(BASE_DATE, i),
            "region": "US-WEST",
        }
        for i in range(1, 11)
    ]


def batch_002_schema_drift() -> list[dict[str, Any]]:
    """Schema drift: new column, type mismatch, case mismatch on key."""
    rows: list[dict[str, Any]] = []
    for i in range(1, 8):
        row: dict[str, Any] = {
            "batch_id": "B-002",
            "order_id": f"ORD-{100 + i:04d}",
            "customer_id": f"CUST-{200 + i}",
            "order_total": round(25.00 + i * 7.75, 2),
            "event_ts": _ts("2026-03-02T09:00:00", i),
            "region": "US-EAST",
        }
        if i == 2:
            # New column the schema has never seen
            row["loyalty_tier"] = "GOLD"
        if i == 4:
            # Type mismatch: order_total as string instead of number
            row["order_total"] = "not_a_number"
        if i == 6:
            # Case mismatch on a key field name (Order_ID vs order_id)
            del row["order_id"]
            row["Order_ID"] = f"ORD-{100 + i:04d}"
        rows.append(row)
    return rows


def batch_003_duplicate_replay() -> list[dict[str, Any]]:
    """Duplicate replay: same batch_id as batch_001, sent under a new file name."""
    return [
        {
            "batch_id": "B-001",
            "order_id": f"ORD-{i:04d}",
            "customer_id": f"CUST-{100 + i}",
            "order_total": round(49.99 + i * 12.50, 2),
            "event_ts": _ts(BASE_DATE, i),
            "region": "US-WEST",
        }
        for i in range(1, 11)
    ]


def batch_004_partial_payload() -> list[dict[str, Any]]:
    """Partial payload: missing required fields on multiple rows."""
    return [
        {
            "batch_id": "B-004",
            "order_id": "ORD-2001",
            "customer_id": None,
            "order_total": 89.99,
            "event_ts": _ts("2026-03-03T10:00:00", 0),
        },
        {
            "batch_id": "B-004",
            "order_id": None,
            "customer_id": "CUST-401",
            "order_total": 120.00,
            "event_ts": _ts("2026-03-03T10:00:00", 1),
        },
        {
            "batch_id": "B-004",
            "order_id": "ORD-2003",
            "customer_id": "CUST-402",
            "order_total": -15.00,
            "event_ts": _ts("2026-03-03T10:00:00", 2),
        },
        {
            "batch_id": "B-004",
            "order_id": "ORD-2004",
            "customer_id": "CUST-403",
            "order_total": 55.00,
            "event_ts": None,
        },
        {
            "batch_id": None,
            "order_id": "ORD-2005",
            "customer_id": "CUST-404",
            "order_total": 33.50,
            "event_ts": _ts("2026-03-03T10:00:00", 4),
        },
    ]


ALL_BATCHES = {
    "batch_001_good/orders_2026-03-01.json": batch_001_good,
    "batch_002_schema_drift/orders_2026-03-02.json": batch_002_schema_drift,
    "batch_003_duplicate_replay/orders_2026-03-02-replay.json": batch_003_duplicate_replay,
    "batch_004_partial_payload/orders_2026-03-03.json": batch_004_partial_payload,
}


def write_sample_batches(target_dir: str | pathlib.Path) -> dict[str, int]:
    """Write all sample batches as newline-delimited JSON files.

    Returns a dict of {relative_path: row_count} for verification.
    """
    target = pathlib.Path(target_dir)
    results: dict[str, int] = {}
    for rel_path, generator in ALL_BATCHES.items():
        out_path = target / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        rows = generator()
        with open(out_path, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        results[rel_path] = len(rows)
    return results


if __name__ == "__main__":
    import sys

    dest = sys.argv[1] if len(sys.argv) > 1 else "data/sample"
    counts = write_sample_batches(dest)
    for path, count in counts.items():
        print(f"  {path}: {count} rows")
    print(f"\n  Total: {sum(counts.values())} rows across {len(counts)} files")
