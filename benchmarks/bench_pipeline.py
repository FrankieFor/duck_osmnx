"""Benchmark each stage of the ducknx pipeline at multiple scales."""

from __future__ import annotations

import sys
import time
import tracemalloc
from pathlib import Path

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


def _timed(label: str, func, *args, **kwargs):
    """Run func, return (result, wall_seconds, peak_mb)."""
    tracemalloc.start()
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / 1024 / 1024
    print(f"  {label:25s}  {elapsed:8.2f}s  {peak_mb:8.1f} MB")
    return result


def bench_scale(name: str, bbox: tuple[float, float, float, float]) -> None:
    """Benchmark all stages for one scale tier."""
    print(f"\n{'='*60}")
    print(f"Scale: {name}  bbox={bbox}")
    print(f"{'='*60}")
    print(f"  {'Stage':25s}  {'Time':>8s}  {'Peak Mem':>8s}")
    print(f"  {'-'*25}  {'-'*8}  {'-'*8}")

    polygon = utils_geo.bbox_to_poly(bbox)

    # Stage: DuckDB query (network)
    nodes_df, ways_df = _timed(
        "duckdb_query_network",
        _pbf_reader._read_pbf_network_duckdb,
        polygon, "drive", None, PBF_PATH,
    )

    # Stage: Graph build
    G = _timed(
        "graph_build",
        graph._create_graph_from_dfs,
        nodes_df, ways_df, False,
    )

    # Stage: Graph simplify
    if len(G.edges) > 0:
        _timed("graph_simplify", simplification.simplify_graph, G)

    # Stage: Distance calc
    if len(G.edges) > 0:
        _timed("distance_calc", distance.add_edge_lengths, G)

    # Stage: DuckDB query (features)
    tags = {"building": True}
    try:
        nodes_f, ways_f, rels_f = _timed(
            "duckdb_query_features",
            _pbf_reader._read_pbf_features_duckdb,
            polygon, tags, PBF_PATH,
        )

        # Stage: Features GDF
        _timed(
            "features_gdf",
            features._create_gdf_from_dfs,
            nodes_f, ways_f, rels_f, polygon, tags,
        )
    except Exception as e:
        print(f"  features skipped: {e}")

    # Stage: End-to-end graph
    _duckdb.close()
    _timed(
        "end_to_end_graph",
        dx.graph_from_bbox,
        bbox, network_type="drive", simplify=True,
    )


def main() -> None:
    """Run benchmarks."""
    if not PBF_PATH.exists():
        print(f"PBF file not found: {PBF_PATH}", file=sys.stderr)
        sys.exit(1)

    settings.pbf_file_path = str(PBF_PATH)
    settings.log_console = False

    for name, *bbox_coords in SCALES:
        _duckdb.close()
        bench_scale(name, tuple(bbox_coords))

    _duckdb.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
