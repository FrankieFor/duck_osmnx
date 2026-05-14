"""Compare latest benchmark JSON against baseline; exit non-zero on regression."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def latest_sha() -> str:
    """Return short git SHA for the current commit."""
    return subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"],
    ).decode().strip()


def main() -> None:
    """Print before/after and emit Phase A gate / Phase B trigger verdict."""
    root = Path(__file__).parent / "results"
    base = json.loads((root / "baseline.json").read_text())
    if base.get("source") == "spec_estimate":
        print(
            "WARNING: benchmarks/results/baseline.json is seeded from spec "
            "estimates, not a measured pre-Phase-A run. Re-baseline after "
            "Phase B lands for trustworthy comparisons.",
            file=sys.stderr,
        )
    new_path = root / f"{latest_sha()}.json"
    if not new_path.exists():
        print(f"No benchmark JSON at {new_path}", file=sys.stderr)
        sys.exit(2)
    new = json.loads(new_path.read_text())

    s_old = base["large"]["graph_simplify"]["time_s"]
    s_new = new["large"]["graph_simplify"]["time_s"]
    b_old = base["large"]["graph_build"]["time_s"]
    b_new = new["large"]["graph_build"]["time_s"]
    print(f"simplify large: {s_old:.2f}s -> {s_new:.2f}s")
    print(f"build    large: {b_old:.2f}s -> {b_new:.2f}s")

    # Phase A gate
    if s_new > 18.0 or b_new > 8.0:  # noqa: PLR2004
        print("FAIL: Phase A gate (simplify <=18s, build <=8s) missed")
        sys.exit(1)

    # Phase B trigger
    if s_new > 10.0 or b_new > 6.0:  # noqa: PLR2004
        print("PHASE_B_REQUIRED")
    else:
        print("PHASE_B_NOT_REQUIRED")


if __name__ == "__main__":
    main()
