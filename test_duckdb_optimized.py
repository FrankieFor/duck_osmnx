import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo
    import time
    import ducknx as dx
    from ducknx._errors import InsufficientResponseError

    dx.settings.pbf_file_path = "berlin-latest.osm.pbf"

    BBOX = (13.38, 52.51, 13.42, 52.53)

    mo.md(
        f"""
        # DuckDB-Optimized PBF Reading Test

        **PBF file**: `{dx.settings.pbf_file_path}`
        **Test bbox**: `{BBOX}` (central Berlin / Mitte)
        """
    )


@app.cell
def test_graph_optimized():
    mo.md("## Graph: Optimized Path")
    dx.settings.use_duckdb_optimized = True
    _t0 = time.perf_counter()
    G_opt = dx.graph_from_bbox(BBOX, network_type="drive")
    t_opt = time.perf_counter() - _t0
    mo.md(
        f"""
        **Optimized path** completed in **{t_opt:.3f}s**

        - Nodes: **{len(G_opt):,}**
        - Edges: **{len(G_opt.edges):,}**
        """
    )
    return G_opt, t_opt


@app.cell
def test_graph_legacy():
    mo.md("## Graph: Legacy Path")
    dx.settings.use_duckdb_optimized = False
    _t0 = time.perf_counter()
    G_leg = dx.graph_from_bbox(BBOX, network_type="drive")
    t_leg = time.perf_counter() - _t0
    mo.md(
        f"""
        **Legacy path** completed in **{t_leg:.3f}s**

        - Nodes: **{len(G_leg):,}**
        - Edges: **{len(G_leg.edges):,}**
        """
    )
    return G_leg, t_leg


@app.cell
def compare_graphs(G_leg, G_opt, t_leg, t_opt):
    _nodes_match = set(G_opt.nodes) == set(G_leg.nodes)
    _edges_match = len(G_opt.edges) == len(G_leg.edges)
    _speedup = t_leg / t_opt if t_opt > 0 else 0

    _status_nodes = "pass" if _nodes_match else "FAIL"
    _status_edges = "pass" if _edges_match else "FAIL"

    mo.md(
        f"""
        ## Graph Comparison

        | Metric | Optimized | Legacy | Match |
        |--------|-----------|--------|-------|
        | Nodes | {len(G_opt):,} | {len(G_leg):,} | {_status_nodes} |
        | Edges | {len(G_opt.edges):,} | {len(G_leg.edges):,} | {_status_edges} |
        | Time (s) | {t_opt:.3f} | {t_leg:.3f} | |
        | **Speedup** | **{_speedup:.2f}x** | | |
        """
    )
    return


@app.cell
def test_features_optimized():
    mo.md("## Features: Optimized Path")
    dx.settings.use_duckdb_optimized = True
    _tags = {"building": True}
    _t0 = time.perf_counter()
    gdf_opt = dx.features_from_bbox(BBOX, tags=_tags)
    t_feat_opt = time.perf_counter() - _t0
    mo.md(
        f"""
        **Optimized path** completed in **{t_feat_opt:.3f}s**

        - Features: **{len(gdf_opt):,}**
        - Geometry types: `{dict(gdf_opt.geom_type.value_counts())}`
        - Columns: `{list(gdf_opt.columns)}`
        """
    )
    return gdf_opt, t_feat_opt


@app.cell
def test_features_legacy():
    mo.md("## Features: Legacy Path")
    dx.settings.use_duckdb_optimized = False
    _tags = {"building": True}
    gdf_leg = None
    t_feat_leg = None
    try:
        _t0 = time.perf_counter()
        gdf_leg = dx.features_from_bbox(BBOX, tags=_tags)
        t_feat_leg = time.perf_counter() - _t0
        mo.md(
            f"""
            **Legacy path** completed in **{t_feat_leg:.3f}s**

            - Features: **{len(gdf_leg):,}**
            """
        )
    except InsufficientResponseError:
        mo.md(
            """
            **Legacy path**: InsufficientResponseError

            This is a **pre-existing issue** in the legacy features path -- it does
            not spatially filter ways, so it misses building features that are
            represented as ways (the vast majority). The optimized path fixes this.
            """
        )
    return gdf_leg, t_feat_leg


@app.cell
def compare_features(gdf_leg, gdf_opt, t_feat_leg, t_feat_opt):
    if gdf_leg is not None and t_feat_leg is not None:
        _match = len(gdf_opt) == len(gdf_leg)
        _status = "pass" if _match else "FAIL"
        _speedup = t_feat_leg / t_feat_opt if t_feat_opt > 0 else 0
        mo.md(
            f"""
            ## Features Comparison

            | Metric | Optimized | Legacy | Match |
            |--------|-----------|--------|-------|
            | Features | {len(gdf_opt):,} | {len(gdf_leg):,} | {_status} |
            | Time (s) | {t_feat_opt:.3f} | {t_feat_leg:.3f} | |
            | **Speedup** | **{_speedup:.2f}x** | | |
            """
        )
    else:
        mo.md(
            f"""
            ## Features Comparison

            | Metric | Optimized | Legacy |
            |--------|-----------|--------|
            | Features | {len(gdf_opt):,} | N/A (legacy failed) |
            | Time (s) | {t_feat_opt:.3f} | N/A |

            Legacy path could not produce results for comparison (pre-existing bug).
            The optimized path is the only working features path.
            """
        )
    return


@app.cell
def sample_data():
    mo.md("""
    ## Sample Feature Data (first 10 rows)
    """)
    return


if __name__ == "__main__":
    app.run()
