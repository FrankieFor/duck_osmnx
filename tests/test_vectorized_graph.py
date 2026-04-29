"""Tests for vectorized graph construction."""

from __future__ import annotations

from pathlib import Path

import networkx as nx
import pytest
from shapely import Polygon

from ducknx import _duckdb
from ducknx import _pbf_reader
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
