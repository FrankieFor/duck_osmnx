"""Tests for vectorized graph construction and distance vectorization."""

from __future__ import annotations

from pathlib import Path

import networkx as nx
import numpy as np
import pytest
from shapely import Polygon

import ducknx as dx
from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import distance
from ducknx import graph
from ducknx import settings

PBF_PATH = Path("berlin-latest.osm.pbf")
BBOX_POLYGON = Polygon([
    (13.38, 52.51), (13.39, 52.51), (13.39, 52.52), (13.38, 52.52), (13.38, 52.51)
])


@pytest.fixture(autouse=True)
def _setup_pbf():
    """Configure PBF path and reset connection."""
    if not PBF_PATH.exists():
        pytest.skip("Berlin PBF not available")
    settings.pbf_file_path = str(PBF_PATH)
    _duckdb.close()
    yield
    _duckdb.close()


def test_graph_has_nodes_and_edges() -> None:
    """Test that graph construction produces a valid graph."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    assert isinstance(G, nx.MultiDiGraph)
    assert len(G.nodes) > 0
    assert len(G.edges) > 0


def test_graph_nodes_have_coordinates() -> None:
    """Test that all graph nodes have x and y attributes."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    for node, data in G.nodes(data=True):
        assert "x" in data, f"Node {node} missing x"
        assert "y" in data, f"Node {node} missing y"


def test_graph_edges_have_length() -> None:
    """Test that all edges have length attribute."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    for u, v, data in G.edges(data=True):
        assert "length" in data, f"Edge ({u},{v}) missing length"
        assert data["length"] >= 0


def test_graph_edges_have_osmid() -> None:
    """Test that all edges have osmid attribute."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    for u, v, data in G.edges(data=True):
        assert "osmid" in data, f"Edge ({u},{v}) missing osmid"


def test_bidirectional_doubles_edges() -> None:
    """Test that bidirectional mode creates reverse edges."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G_uni = graph._create_graph_from_dfs(nodes, ways, False)
    _duckdb.close()
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G_bi = graph._create_graph_from_dfs(nodes, ways, True)
    # Bidirectional should have more edges (all non-oneway edges are doubled)
    assert len(G_bi.edges) >= len(G_uni.edges)


# --- Distance vectorization tests ---


def _make_simple_graph() -> nx.MultiDiGraph:
    """Create a simple graph with known coordinates for unit testing."""
    G = nx.MultiDiGraph(crs="EPSG:4326")
    G.add_node(1, x=13.377, y=52.516)
    G.add_node(2, x=13.379, y=52.518)
    G.add_node(3, x=13.381, y=52.514)
    G.add_edge(1, 2, key=0)
    G.add_edge(2, 3, key=0)
    G.add_edge(3, 1, key=0)
    return G


def test_edge_lengths_are_positive() -> None:
    """Test that all computed edge lengths are positive."""
    G = _make_simple_graph()
    G = distance.add_edge_lengths(G)
    for u, v, data in G.edges(data=True):
        assert "length" in data
        assert data["length"] > 0


def test_edge_lengths_are_finite() -> None:
    """Test that all computed edge lengths are finite numbers."""
    G = _make_simple_graph()
    G = distance.add_edge_lengths(G)
    for u, v, data in G.edges(data=True):
        assert np.isfinite(data["length"])


def test_edge_lengths_subset() -> None:
    """Test adding lengths to a subset of edges."""
    G = _make_simple_graph()
    subset = [(1, 2, 0)]
    G = distance.add_edge_lengths(G, edges=subset)
    assert "length" in G[1][2][0]
    assert "length" not in G[2][3][0]


def test_vectorized_matches_great_circle() -> None:
    """Test that vectorized result matches direct great_circle computation."""
    G = _make_simple_graph()
    G = distance.add_edge_lengths(G)
    expected = distance.great_circle(52.516, 13.377, 52.518, 13.379)
    actual = G[1][2][0]["length"]
    assert abs(actual - expected) < 1e-10


def test_edge_lengths_real_graph() -> None:
    """Test edge lengths on a real graph from Berlin PBF data."""
    G = dx.graph_from_bbox(
        bbox=(13.375, 52.515, 13.385, 52.520),
        network_type="drive",
    )
    for u, v, data in G.edges(data=True):
        assert data.get("length", 0) > 0
