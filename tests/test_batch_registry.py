"""Tests for the manifests (batch registry) module."""

import pytest

from bronze_handoff_demo.manifests import BatchManifest, validate_manifest


def test_manifest_round_trip():
    m = BatchManifest(batch_id="B-001", expected_row_count=10)
    raw = m.to_json()
    m2 = BatchManifest.from_json(raw)
    assert m2.batch_id == "B-001"
    assert m2.expected_row_count == 10


def test_validate_manifest_pass():
    m = BatchManifest(batch_id="B-001", expected_row_count=10)
    issues = validate_manifest(m, actual_row_count=10)
    assert issues == []


def test_validate_manifest_row_count_mismatch():
    m = BatchManifest(batch_id="B-001", expected_row_count=10)
    issues = validate_manifest(m, actual_row_count=8)
    assert len(issues) == 1
    assert "row count mismatch" in issues[0]


def test_validate_manifest_missing_batch_id():
    m = BatchManifest(batch_id="", expected_row_count=0)
    issues = validate_manifest(m, actual_row_count=5)
    assert any("batch_id" in i for i in issues)


def test_validate_manifest_zero_expected_skips_count_check():
    """When expected_row_count is 0, count check is skipped."""
    m = BatchManifest(batch_id="B-999", expected_row_count=0)
    issues = validate_manifest(m, actual_row_count=42)
    assert issues == []


def test_validate_manifest_zero_actual_with_expected():
    """Empty batch landed when rows were expected."""
    m = BatchManifest(batch_id="B-010", expected_row_count=10)
    issues = validate_manifest(m, actual_row_count=0)
    assert len(issues) == 1
    assert "row count mismatch" in issues[0]


def test_manifest_round_trip_preserves_landed_at():
    m = BatchManifest(batch_id="B-001", expected_row_count=5)
    raw = m.to_json()
    m2 = BatchManifest.from_json(raw)
    assert m2.landed_at == m.landed_at


def test_manifest_from_json_rejects_extra_keys():
    import json

    data = {"batch_id": "B-001", "expected_row_count": 5, "unknown_field": "oops"}
    raw = json.dumps(data)
    with pytest.raises(TypeError):
        BatchManifest.from_json(raw)
