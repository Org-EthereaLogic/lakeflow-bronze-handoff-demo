"""Tests for the docs visual generation module.

These tests verify that the generate_visuals script runs without error
and produces the expected image files. They require matplotlib.
"""

import importlib
import pathlib

import pytest


@pytest.fixture
def visuals_module():
    """Import the generate_visuals module from the docs directory."""
    import sys
    docs_dir = str(pathlib.Path(__file__).resolve().parent.parent / "docs")
    if docs_dir not in sys.path:
        sys.path.insert(0, docs_dir)
    return importlib.import_module("generate_visuals")


@pytest.fixture
def images_dir():
    return pathlib.Path(__file__).resolve().parent.parent / "docs" / "images"


def test_architecture_diagram_generates(visuals_module, images_dir):
    visuals_module.generate_architecture_diagram()
    assert (images_dir / "bronze_handoff_architecture.png").exists()


def test_quarantine_funnel_generates(visuals_module, images_dir):
    visuals_module.generate_quarantine_funnel()
    assert (images_dir / "quarantine_funnel.png").exists()


def test_quarantine_funnel_uses_derived_counts(visuals_module):
    stages, counts = visuals_module.get_quarantine_funnel_data()
    assert stages == [
        "Landed",
        "Passed Required\nFields",
        "Passed Order\nTotal Check",
        "Passed Drift\nCheck",
        "Passed Replay\nCheck",
        "Ready Output",
    ]
    assert counts == [32, 27, 25, 24, 14, 14]


def test_replay_protection_flow_generates(visuals_module, images_dir):
    visuals_module.generate_replay_protection_flow()
    assert (images_dir / "replay_protection_flow.png").exists()
