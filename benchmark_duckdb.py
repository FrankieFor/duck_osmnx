"""Benchmark script comparing DuckDB-optimized vs legacy PBF reading paths."""

import time

import ducknx as dx
from ducknx._errors import InsufficientResponseError

# Configure PBF file path
dx.settings.pbf_file_path = "berlin-latest.osm.pbf"

# Test area: central Berlin (Mitte area)
BBOX = (13.38, 52.51, 13.42, 52.53)


def benchmark_graph():
    """Compare graph_from_bbox performance between optimized and legacy paths."""
    print("=" * 60)
    print("Benchmarking graph_from_bbox")
    print("=" * 60)

    # DuckDB-optimized path
    dx.settings.use_duckdb_optimized = True
    t0 = time.perf_counter()
    G_opt = dx.graph_from_bbox(BBOX, network_type="drive")
    t_opt = time.perf_counter() - t0

    # Legacy path
    dx.settings.use_duckdb_optimized = False
    t0 = time.perf_counter()
    G_leg = dx.graph_from_bbox(BBOX, network_type="drive")
    t_leg = time.perf_counter() - t0

    # Compare results
    nodes_opt = set(G_opt.nodes)
    nodes_leg = set(G_leg.nodes)
    edges_opt = len(G_opt.edges)
    edges_leg = len(G_leg.edges)

    print(f"\n{'Metric':<25} {'Optimized':>12} {'Legacy':>12} {'Match':>8}")
    print("-" * 60)
    print(f"{'Nodes':.<25} {len(nodes_opt):>12,} {len(nodes_leg):>12,} {'YES' if nodes_opt == nodes_leg else 'NO':>8}")
    print(f"{'Edges':.<25} {edges_opt:>12,} {edges_leg:>12,} {'YES' if edges_opt == edges_leg else 'NO':>8}")
    print(f"{'Time (seconds)':.<25} {t_opt:>12.3f} {t_leg:>12.3f} {'':>8}")
    print(f"{'Speedup':.<25} {t_leg / t_opt:>12.2f}x {'':>12} {'':>8}")

    if nodes_opt != nodes_leg:
        only_opt = nodes_opt - nodes_leg
        only_leg = nodes_leg - nodes_opt
        print(f"\n  Nodes only in optimized: {len(only_opt)}")
        print(f"  Nodes only in legacy:    {len(only_leg)}")

    return G_opt, G_leg


def benchmark_features():
    """Compare features_from_bbox performance between optimized and legacy paths."""
    print("\n" + "=" * 60)
    print("Benchmarking features_from_bbox")
    print("=" * 60)

    tags = {"building": True}

    # DuckDB-optimized path
    dx.settings.use_duckdb_optimized = True
    t0 = time.perf_counter()
    gdf_opt = dx.features_from_bbox(BBOX, tags=tags)
    t_opt = time.perf_counter() - t0
    print(f"\nOptimized: {len(gdf_opt):,} features in {t_opt:.3f}s")

    # Legacy path
    dx.settings.use_duckdb_optimized = False
    gdf_leg = None
    t_leg = None
    try:
        t0 = time.perf_counter()
        gdf_leg = dx.features_from_bbox(BBOX, tags=tags)
        t_leg = time.perf_counter() - t0
        print(f"Legacy:    {len(gdf_leg):,} features in {t_leg:.3f}s")
    except InsufficientResponseError:
        print("Legacy:    FAILED (InsufficientResponseError — pre-existing issue)")
        print("           The legacy features path does not spatially filter ways,")
        print("           causing it to miss features in some cases.")

    # Compare results
    print(f"\n{'Metric':<25} {'Optimized':>12} {'Legacy':>12} {'Match':>8}")
    print("-" * 60)
    n_opt = len(gdf_opt)
    if gdf_leg is not None:
        n_leg = len(gdf_leg)
        print(f"{'Features':.<25} {n_opt:>12,} {n_leg:>12,} {'YES' if n_opt == n_leg else 'NO':>8}")
        print(f"{'Time (seconds)':.<25} {t_opt:>12.3f} {t_leg:>12.3f} {'':>8}")
        if t_opt > 0:
            print(f"{'Speedup':.<25} {t_leg / t_opt:>12.2f}x {'':>12} {'':>8}")
    else:
        print(f"{'Features':.<25} {n_opt:>12,} {'N/A':>12} {'N/A':>8}")
        print(f"{'Time (seconds)':.<25} {t_opt:>12.3f} {'N/A':>12} {'':>8}")

    return gdf_opt, gdf_leg


if __name__ == "__main__":
    print("DuckDB-Optimized vs Legacy PBF Reading Benchmark")
    print(f"PBF file: {dx.settings.pbf_file_path}")
    print(f"Test bbox: {BBOX}\n")

    G_opt, G_leg = benchmark_graph()
    gdf_opt, gdf_leg = benchmark_features()

    print("\n" + "=" * 60)
    print("Benchmark complete")
    print("=" * 60)
