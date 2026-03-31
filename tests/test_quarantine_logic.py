"""Tests for the handoff rules (quarantine logic) module."""

from bronze_handoff_demo.rules import HANDOFF_RULES, evaluate_batch, evaluate_row


def _good_row():
    return {
        "batch_id": "B-001",
        "order_id": "ORD-0001",
        "customer_id": "CUST-101",
        "order_total": 62.49,
        "event_ts": "2026-03-01T09:00:00Z",
        "_rescued_data": None,
    }


def test_good_row_passes_all():
    assert evaluate_row(_good_row()) == []


def test_missing_batch_id():
    row = _good_row()
    row["batch_id"] = None
    failed = evaluate_row(row)
    assert "required_batch_id" in failed


def test_missing_order_id():
    row = _good_row()
    row["order_id"] = None
    failed = evaluate_row(row)
    assert "required_order_id" in failed


def test_missing_customer_id():
    row = _good_row()
    row["customer_id"] = None
    failed = evaluate_row(row)
    assert "required_customer_id" in failed


def test_negative_order_total():
    row = _good_row()
    row["order_total"] = -15.00
    failed = evaluate_row(row)
    assert "non_negative_order_total" in failed


def test_null_order_total():
    row = _good_row()
    row["order_total"] = None
    failed = evaluate_row(row)
    assert "non_negative_order_total" in failed


def test_string_order_total():
    row = _good_row()
    row["order_total"] = "not_a_number"
    failed = evaluate_row(row)
    assert "non_negative_order_total" in failed


def test_missing_event_ts():
    row = _good_row()
    row["event_ts"] = None
    failed = evaluate_row(row)
    assert "valid_event_ts" in failed


def test_rescued_data_present():
    row = _good_row()
    row["_rescued_data"] = '{"loyalty_tier": "GOLD"}'
    failed = evaluate_row(row)
    assert "rescued_data_empty" in failed


def test_not_duplicate_batch_passes_by_default():
    row = _good_row()
    assert "not_duplicate_batch" not in evaluate_row(row)


def test_not_duplicate_batch_fails_when_replay_flagged():
    row = _good_row()
    row["_is_replay_file"] = True
    failed = evaluate_row(row)
    assert "not_duplicate_batch" in failed


def test_multiple_failures():
    row = _good_row()
    row["batch_id"] = None
    row["order_total"] = -5.00
    row["_rescued_data"] = "{}"
    failed = evaluate_row(row)
    assert len(failed) == 3
    assert "required_batch_id" in failed
    assert "non_negative_order_total" in failed
    assert "rescued_data_empty" in failed


def test_evaluate_batch_returns_only_failures():
    rows = [_good_row(), _good_row()]
    rows[1]["customer_id"] = None
    failures = evaluate_batch(rows)
    assert 0 not in failures
    assert 1 in failures
    assert "required_customer_id" in failures[1]


def test_rule_registry_has_seven_rules():
    assert len(HANDOFF_RULES) == 7
