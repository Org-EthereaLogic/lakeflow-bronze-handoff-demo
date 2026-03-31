"""Tests for derived demo metrics used by docs visuals."""

from bronze_handoff_demo.demo_metrics import (
    build_demo_pipeline_rows,
    compute_demo_summary,
    compute_quarantine_funnel,
)


def test_build_demo_pipeline_rows_models_rescue_and_replay_behavior():
    rows = build_demo_pipeline_rows()

    assert len(rows) == 32
    assert sum("rescued_data_empty" in row["failed_rules"] for row in rows) == 3
    assert sum("not_duplicate_batch" in row["failed_rules"] for row in rows) == 10
    assert sum(row["_is_replay_file"] for row in rows) == 10


def test_compute_quarantine_funnel_is_derived_from_demo_data():
    assert compute_quarantine_funnel() == [
        ("Landed", 32),
        ("Passed Required\nFields", 27),
        ("Passed Order\nTotal Check", 25),
        ("Passed Drift\nCheck", 24),
        ("Passed Replay\nCheck", 14),
        ("Ready Output", 14),
    ]


def test_compute_demo_summary_matches_demo_contract():
    assert compute_demo_summary() == {
        "total_landed": 32,
        "total_ready": 14,
        "total_quarantined": 18,
        "total_rescued": 3,
        "total_duplicate": 10,
    }
