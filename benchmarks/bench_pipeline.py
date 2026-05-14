"""Benchmark each stage of the ducknx pipeline at multiple scales."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any
from typing import Callable

import ducknx as dx
from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import distance
from ducknx import features
from ducknx import graph
from ducknx import settings
from ducknx import simplification
from ducknx import utils_geo

# Berlin PBF — adjust path if needed
PBF_PATH = Path("berlin-latest.osm.pbf")

# Scale tiers: (name, left, bottom, right, top)
SCALES = [
    ("small", 13.38, 52.51, 13.39, 52.52),
    ("medium", 13.3, 52.45, 13.4, 52.55),
    ("large", 13.1, 52.3, 13.8, 52.7),
]


def _timed(
    label: str,
    func: Callable[..., Any],
    *args: Any,
    timings: dict[str, dict[str, float]] | None = None,
    **kwargs: Any,
) -> Any:
    """Run func, return result; optionally record timings."""
    tracemalloc.start()
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / 1024 / 1024
    print(f"  {label:25s}  {elapsed:8.2f}s  {peak_mb:8.1f} MB")
    if timings is not None:
        timings[label] = {"time_s": float(elapsed), "peak_mb": float(peak_mb)}
    return result


def bench_scale_collect(
    name: str, bbox: tuple[float, float, float, float],
) -> dict[str, dict[str, float]]:
    """Benchmark all stages for one scale tier; return per-stage timings."""
    print(f"\n{'=' * 60}")
    print(f"Scale: {name}  bbox={bbox}")
    print(f"{'=' * 60}")
    print(f"  {'Stage':25s}  {'Time':>8s}  {'Peak Mem':>8s}")
    print(f"  {'-' * 25}  {'-' * 8}  {'-' * 8}")

    timings: dict[str, dict[str, float]] = {}
    polygon = utils_geo.bbox_to_poly(bbox)

    nodes_df, ways_df = _timed(
        "duckdb_query_network",
        _pbf_reader._read_pbf_network_duckdb,
        polygon, "drive", None, PBF_PATH,
        timings=timings,
    )

    G = _timed(
        "graph_build",
        graph._create_graph_from_dfs,
        nodes_df, ways_df, False,
        timings=timings,
    )

    if len(G.edges) > 0:
        _timed("graph_simplify", simplification.simplify_graph, G, timings=timings)

    if len(G.edges) > 0:
        _timed("distance_calc", distance.add_edge_lengths, G, timings=timings)

    tags = {"building": True}
    try:
        nodes_f, ways_f, rels_f = _timed(
            "duckdb_query_features",
            _pbf_reader._read_pbf_features_duckdb,
            polygon, tags, PBF_PATH,
            timings=timings,
        )
        _timed(
            "features_table",
            features._build_table,
            nodes_f, ways_f, rels_f, polygon, tags,
            timings=timings,
        )
    except Exception as e:  # noqa: BLE001
        print(f"  features skipped: {e}")

    _duckdb.close()
    _timed(
        "end_to_end_graph",
        dx.graph_from_bbox,
        bbox, network_type="drive", simplify=True,
        timings=timings,
    )
    return timings


def _git_sha() -> str:
    """Return short git SHA for the current commit."""
    return subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=Path(__file__).parent.parent,
    ).decode().strip()


def main() -> None:
    """Run benchmarks, optionally dumping JSON for tracking."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--track", action="store_true",
        help="Emit JSON to benchmarks/results/<sha>.json",
    )
    args = parser.parse_args()

    if not PBF_PATH.exists():
        print(f"PBF file not found: {PBF_PATH}", file=sys.stderr)
        sys.exit(1)

    settings.pbf_file_path = str(PBF_PATH)
    settings.log_console = False

    results: dict[str, dict[str, dict[str, float]]] = {}
    for name, *bbox_coords in SCALES:
        _duckdb.close()
        results[name] = bench_scale_collect(name, tuple(bbox_coords))  # type: ignore[arg-type]

    _duckdb.close()

    if args.track:
        out_dir = Path(__file__).parent / "results"
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f"{_git_sha()}.json"
        out_path.write_text(json.dumps(results, indent=2))
        print(f"\nWrote {out_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
