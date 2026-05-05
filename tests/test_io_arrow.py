"""Tests for io.save_graph_geopackage Arrow path."""

from __future__ import annotations

from pathlib import Path

import networkx as nx
import pyogrio
import pytest
from shapely import LineString

from ducknx import io


@pytest.fixture
def sample_graph() -> nx.MultiDiGraph:
    """Tiny MultiDiGraph with mixed-type edge attributes (incl. list-typed osmid)."""
    G = nx.MultiDiGraph(crs="EPSG:4326")
    G.add_node(1, x=13.38, y=52.51, highway="traffic_signals", street_count=4)
    G.add_node(2, x=13.39, y=52.51, street_count=3)
    G.add_node(3, x=13.39, y=52.52, street_count=4)
    # edges with mixed scalar / list / geometry attributes
    G.add_edge(1, 2, key=0, osmid=10, length=80.5, name="A", oneway=True)
    G.add_edge(2, 3, key=0, osmid=[11, 12], length=120.0, name=["B1", "B2"],
               geometry=LineString([(13.39, 52.51), (13.39, 52.52)]))
    return G


def test_save_graph_geopackage_writes_two_layers(
    sample_graph: nx.MultiDiGraph, tmp_path: Path
) -> None:
    """Both nodes and edges layers exist and contain the expected row counts."""
    out = tmp_path / "graph.gpkg"
    io.save_graph_geopackage(sample_graph, filepath=out, directed=True)

    assert out.exists()
    layers = {name for name, _ in pyogrio.list_layers(out)}
    assert {"nodes", "edges"}.issubset(layers)

    nodes_info = pyogrio.read_info(out, layer="nodes")
    edges_info = pyogrio.read_info(out, layer="edges")
    assert nodes_info["features"] == sample_graph.number_of_nodes()
    assert edges_info["features"] == sample_graph.number_of_edges()


def test_save_graph_geopackage_lists_become_strings(
    sample_graph: nx.MultiDiGraph, tmp_path: Path
) -> None:
    """List-valued attrs (e.g. simplified osmid) get JSON-stringified for GPKG."""
    out = tmp_path / "graph.gpkg"
    io.save_graph_geopackage(sample_graph, filepath=out, directed=True)

    edges_tbl = pyogrio.read_arrow(out, layer="edges")[1]
    osmid_col = edges_tbl.column("osmid").to_pylist()
    # one row had osmid=[11, 12] which must serialize, not throw
    assert any(isinstance(v, str) and v.startswith("[") for v in osmid_col)


def test_save_graph_geopackage_undirected_path(
    sample_graph: nx.MultiDiGraph, tmp_path: Path
) -> None:
    """directed=False routes through to_undirected without errors."""
    out = tmp_path / "graph_u.gpkg"
    io.save_graph_geopackage(sample_graph, filepath=out, directed=False)
    assert out.exists()
    layers = {name for name, _ in pyogrio.list_layers(out)}
    assert {"nodes", "edges"}.issubset(layers)
