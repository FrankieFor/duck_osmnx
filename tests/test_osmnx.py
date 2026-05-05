"""
Smoke tests for the public ducknx API.

The original ducknx test suite required a populated `.osm.pbf` cache,
matplotlib for plot tests, and the legacy GeoDataFrame API. After the
3.0 Arrow redesign:

- core graph + features tests live in ``test_arrow_pipeline``,
  ``test_features_vectorized``, ``test_arrow_convert``, ``test_io_arrow``,
  ``test_consolidate_arrow``, and ``test_vectorized_graph``;
- plot/UI tests are gated on the ``plot`` extras and a real PBF file,
  and skip cleanly when those are unavailable.

This module retains the ``test_osmnx`` filename for backwards
compatibility with CI and provides a focused public-API smoke test.
"""

from __future__ import annotations

import logging as lg
from pathlib import Path

import pyarrow as pa
import pytest

import ducknx as dx


def test_logging_smoke() -> None:
    """utils.log accepts the legacy log levels without error."""
    dx.utils.log("test default")
    dx.utils.log("test debug", level=lg.DEBUG)
    dx.utils.log("test info", level=lg.INFO)
    dx.utils.log("test warning", level=lg.WARNING)
    dx.utils.log("test error", level=lg.ERROR)


def test_exceptions_smoke() -> None:
    """All custom exception classes are importable and raisable."""
    msg = "testing exception"
    with pytest.raises(dx._errors.ResponseStatusCodeError):
        raise dx._errors.ResponseStatusCodeError(msg)
    with pytest.raises(dx._errors.InsufficientResponseError):
        raise dx._errors.InsufficientResponseError(msg)
    with pytest.raises(dx._errors.GraphSimplificationError):
        raise dx._errors.GraphSimplificationError(msg)


def test_top_level_api_surface() -> None:
    """Confirm the 3.0 Arrow public API names are reachable on `dx`."""
    expected = {
        "graph_from_arrow",
        "graph_to_arrow",
        "graph_from_bbox",
        "graph_from_polygon",
        "geocode",
        "geocode_to_arrow",
        "features_from_bbox",
        "features_from_polygon",
        "save_graph_geopackage",
        "save_graphml",
        "load_graphml",
        "save_graph_xml",
        "project_graph",
        "consolidate_intersections",
        "simplify_graph",
        "shortest_path",
        "k_shortest_paths",
        "add_edge_speeds",
        "add_edge_travel_times",
    }
    missing = [name for name in expected if not hasattr(dx, name)]
    assert not missing, f"missing public API names: {missing}"


def test_legacy_gdf_api_removed() -> None:
    """Legacy GeoDataFrame functions removed in 3.0 are no longer exposed."""
    for removed in ("graph_to_gdfs", "graph_from_gdfs", "geocode_to_gdf"):
        assert not hasattr(dx, removed), (
            f"{removed} should be removed from the public API in 3.0"
        )


def test_plot_module_lazy_imports() -> None:
    """`dx.plot` only imports matplotlib/pandas on first access (lazy)."""
    # Just confirm that referencing the attribute doesn't blow up at import time.
    # If matplotlib is installed we get the module; if not, an ImportError fires
    # only when the user actually tries to use plot.
    try:
        plot = dx.plot
    except ImportError:
        pytest.skip("matplotlib/plot extras not installed")
    else:
        assert hasattr(plot, "plot_graph")


@pytest.mark.skipif(
    not Path("berlin-latest.osm.pbf").exists(),
    reason="berlin-latest.osm.pbf not available",
)
def test_features_from_bbox_returns_arrow_table() -> None:
    """End-to-end: features_from_bbox returns a pa.Table on a real PBF."""
    dx.settings.pbf_file_path = "berlin-latest.osm.pbf"
    tbl = dx.features_from_bbox(
        (13.38, 52.51, 13.39, 52.52), {"building": True},
    )
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows > 0
    assert set(tbl.column_names) == {"element", "id", "tags", "geometry"}


@pytest.mark.skipif(
    not Path("berlin-latest.osm.pbf").exists(),
    reason="berlin-latest.osm.pbf not available",
)
def test_graph_from_bbox_round_trip_via_arrow() -> None:
    """End-to-end: graph_from_bbox → graph_to_arrow → graph_from_arrow."""
    dx.settings.pbf_file_path = "berlin-latest.osm.pbf"
    G = dx.graph_from_bbox(
        (13.38, 52.51, 13.39, 52.52), network_type="drive", simplify=True,
    )
    nodes_tbl, edges_tbl = dx.graph_to_arrow(G)
    G2 = dx.graph_from_arrow(nodes_tbl, edges_tbl)
    assert set(G2.nodes) == set(G.nodes)
    assert set(G2.edges(keys=True)) == set(G.edges(keys=True))
