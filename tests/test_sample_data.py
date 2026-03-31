"""Tests for the sample_data module."""

from bronze_handoff_demo.sample_data import (
    ALL_BATCHES,
    batch_001_good,
    batch_002_schema_drift,
    batch_003_duplicate_replay,
    batch_004_partial_payload,
)


def test_batch_001_has_10_rows():
    rows = batch_001_good()
    assert len(rows) == 10


def test_batch_001_all_fields_present():
    for row in batch_001_good():
        assert row["batch_id"] == "B-001"
        assert row["order_id"] is not None
        assert row["customer_id"] is not None
        assert row["order_total"] >= 0
        assert row["event_ts"] is not None


def test_batch_002_has_7_rows():
    rows = batch_002_schema_drift()
    assert len(rows) == 7


def test_batch_002_has_drift_scenarios():
    rows = batch_002_schema_drift()
    # At least one row should have loyalty_tier (new column)
    assert any("loyalty_tier" in r for r in rows)
    # At least one row should have order_total as string (type mismatch)
    assert any(isinstance(r.get("order_total"), str) for r in rows)
    # At least one row should have Order_ID instead of order_id (case mismatch)
    assert any("Order_ID" in r for r in rows)


def test_batch_003_is_replay_of_001():
    replay = batch_003_duplicate_replay()
    original = batch_001_good()
    assert len(replay) == len(original)
    assert replay[0]["batch_id"] == "B-001"


def test_batch_004_has_5_rows():
    rows = batch_004_partial_payload()
    assert len(rows) == 5


def test_batch_004_has_missing_fields():
    rows = batch_004_partial_payload()
    # At least one null customer_id
    assert any(r.get("customer_id") is None for r in rows)
    # At least one null order_id
    assert any(r.get("order_id") is None for r in rows)
    # At least one negative order_total
    assert any((r.get("order_total") or 0) < 0 for r in rows)
    # At least one null event_ts
    assert any(r.get("event_ts") is None for r in rows)
    # At least one null batch_id
    assert any(r.get("batch_id") is None for r in rows)


def test_all_batches_registry_has_four_entries():
    assert len(ALL_BATCHES) == 4
