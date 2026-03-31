"""
generate_visuals - Create executive-style architecture diagrams and
operational exhibits for the docs/images/ directory.

Usage:
    python docs/generate_visuals.py

Requires matplotlib (install via `pip install -e ".[docs]"`).
"""

from __future__ import annotations

import pathlib

import matplotlib

matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

from bronze_handoff_demo.demo_metrics import compute_quarantine_funnel

OUTPUT_DIR = pathlib.Path(__file__).resolve().parent / "images"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Color palette ────────────────────────────────────────────────────────────

TEAL = "#0D7377"
DARK = "#1A1A2E"
AMBER = "#F59E0B"
RED = "#EF4444"
GREEN = "#10B981"
GRAY = "#6B7280"
WHITE = "#FFFFFF"
LIGHT_BG = "#F8FAFC"


def _save(fig: plt.Figure, name: str) -> None:
    path = OUTPUT_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=WHITE)
    plt.close(fig)
    print(f"  Saved: {path}")


# ── 1. Architecture diagram ─────────────────────────────────────────────────

def generate_architecture_diagram() -> None:
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 7)
    ax.axis("off")
    fig.patch.set_facecolor(WHITE)

    boxes = [
        (1.0, 5.5, 3.0, 0.8, "Landing Volume\n(JSON files)", GRAY),
        (1.0, 4.0, 3.0, 0.8, "Auto Loader\n(STREAM read_files)", TEAL),
        (1.0, 2.5, 3.0, 0.8, "bronze_orders_raw\n(streaming table)", DARK),
        (5.5, 4.0, 3.5, 0.8, "ops_batch_registry\n(replay detection)", AMBER),
        (5.5, 2.5, 3.5, 0.8, "ops_quarantine_rows\n(failed checks)", RED),
        (5.5, 1.0, 3.5, 0.8, "bronze_orders_ready\n(contract-compliant)", GREEN),
        (1.0, 0.5, 3.0, 0.8, "ops_handoff_summary\n(dashboard metrics)", TEAL),
    ]

    for x, y, w, h, label, color in boxes:
        rect = mpatches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.1",
            facecolor=color,
            edgecolor=DARK,
            linewidth=1.5,
            alpha=0.85,
        )
        ax.add_patch(rect)
        ax.text(x + w / 2, y + h / 2, label, ha="center", va="center",
                fontsize=8, fontweight="bold", color=WHITE)

    # Arrows
    arrow_kw = dict(arrowstyle="->", color=DARK, lw=1.5)
    ax.annotate("", xy=(2.5, 4.8), xytext=(2.5, 5.5), arrowprops=arrow_kw)
    ax.annotate("", xy=(2.5, 3.3), xytext=(2.5, 4.0), arrowprops=arrow_kw)
    ax.annotate("", xy=(5.5, 4.4), xytext=(4.0, 2.9), arrowprops=arrow_kw)
    ax.annotate("", xy=(5.5, 2.9), xytext=(4.0, 2.9), arrowprops=arrow_kw)
    ax.annotate("", xy=(5.5, 1.4), xytext=(4.0, 2.9), arrowprops=arrow_kw)
    ax.annotate("", xy=(4.0, 0.9), xytext=(5.5, 1.4), arrowprops=arrow_kw)

    ax.set_title(
        "Governed Bronze Handoff Before Downstream Trust",
        fontsize=14,
        fontweight="bold",
        color=DARK,
        pad=15,
    )
    _save(fig, "bronze_handoff_architecture.png")


# ── 2. Quarantine funnel ────────────────────────────────────────────────────

def get_quarantine_funnel_data() -> tuple[list[str], list[int]]:
    """Return traceable funnel data derived from the sample batches."""
    funnel = compute_quarantine_funnel()
    stages = [stage for stage, _ in funnel]
    counts = [count for _, count in funnel]
    return stages, counts


def generate_quarantine_funnel() -> None:
    fig, ax = plt.subplots(figsize=(8, 5))

    stages, counts = get_quarantine_funnel_data()
    colors = [GRAY, AMBER, AMBER, RED, RED, GREEN]

    bars = ax.barh(range(len(stages)), counts, color=colors, edgecolor=DARK, linewidth=0.5)
    ax.set_yticks(range(len(stages)))
    ax.set_yticklabels(stages, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Row Count", fontsize=10)
    ax.set_title(
        "32 Landed Rows Narrow to 14 Trusted Rows",
        fontsize=13,
        fontweight="bold",
        color=DARK,
    )

    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                str(count), va="center", fontsize=9, fontweight="bold", color=DARK)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.patch.set_facecolor(WHITE)
    _save(fig, "quarantine_funnel.png")


# ── 3. Replay protection flow ───────────────────────────────────────────────

def generate_replay_protection_flow() -> None:
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 4)
    ax.axis("off")
    fig.patch.set_facecolor(WHITE)

    flow_boxes = [
        (0.5, 1.5, 2.2, 1.0, "File Landed\n(new file name)", GRAY),
        (3.5, 1.5, 2.2, 1.0, "Auto Loader\n(processes file)", TEAL),
        (6.5, 1.5, 2.5, 1.0, "Batch Registry\n(check batch_id)", AMBER),
        (9.8, 2.5, 1.8, 0.7, "Ready\n(first seen)", GREEN),
        (9.8, 0.8, 1.8, 0.7, "Quarantine\n(replay)", RED),
    ]

    for x, y, w, h, label, color in flow_boxes:
        rect = mpatches.FancyBboxPatch(
            (x, y), w, h,
            boxstyle="round,pad=0.08",
            facecolor=color,
            edgecolor=DARK,
            linewidth=1.5,
            alpha=0.85,
        )
        ax.add_patch(rect)
        ax.text(x + w / 2, y + h / 2, label, ha="center", va="center",
                fontsize=8, fontweight="bold", color=WHITE)

    arrow_kw = dict(arrowstyle="->", color=DARK, lw=1.5)
    ax.annotate("", xy=(3.5, 2.0), xytext=(2.7, 2.0), arrowprops=arrow_kw)
    ax.annotate("", xy=(6.5, 2.0), xytext=(5.7, 2.0), arrowprops=arrow_kw)
    ax.annotate("", xy=(9.8, 2.85), xytext=(9.0, 2.3), arrowprops=arrow_kw)
    ax.annotate("", xy=(9.8, 1.15), xytext=(9.0, 1.7), arrowprops=arrow_kw)

    ax.text(9.3, 2.8, "1st", fontsize=7, color=GREEN, fontweight="bold")
    ax.text(9.3, 1.3, "2nd+", fontsize=7, color=RED, fontweight="bold")

    ax.set_title(
        "Replay Protection Blocks a Re-Sent Business Batch",
        fontsize=13,
        fontweight="bold",
        color=DARK,
        pad=12,
    )
    _save(fig, "replay_protection_flow.png")


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Generating documentation visuals...\n")
    generate_architecture_diagram()
    generate_quarantine_funnel()
    generate_replay_protection_flow()
    print("\nDone.")
