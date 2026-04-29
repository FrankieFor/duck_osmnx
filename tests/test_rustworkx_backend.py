"""Tests for rustworkx opt-in graph backend."""

from __future__ import annotations

import unittest.mock
from pathlib import Path

import networkx as nx
import pytest
from shapely import Polygon

from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import convert as dx_convert
from ducknx import graph
from ducknx import settings

try:
    import rustworkx as rx

    HAS_RUSTWORKX = True
except ImportError:
    HAS_RUSTWORKX = False

PBF_PATH = Path("berlin-latest.osm.pbf")
BBOX_POLYGON = Polygon(
    [
        (13.38, 52.51),
        (13.39, 52.51),
        (13.39, 52.52),
        (13.38, 52.52),
        (13.38, 52.51),
    ]
)


@pytest.fixture(autouse=True)
def _setup_pbf():
    """Configure PBF path and reset connection."""
    if not PBF_PATH.exists():
        pytest.skip("Berlin PBF not available")
    settings.pbf_file_path = str(PBF_PATH)
    _duckdb.close()
    yield
    _duckdb.close()


def _read_test_data() -> tuple:
    """Read network data for tests."""
    return _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON,
        "drive",
        None,
        PBF_PATH,
    )


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_graph_creation() -> None:
    """Test that backend='rustworkx' returns a PyDiGraph."""
    nodes, ways = _read_test_data()
    G = graph._create_graph_from_dfs(nodes, ways, bidirectional=False, backend="rustworkx")
    assert isinstance(G, rx.PyDiGraph)
    assert G.num_nodes() > 0
    assert G.num_edges() > 0


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_node_payloads() -> None:
    """Test that rustworkx nodes have osmid, x, y."""
    nodes, ways = _read_test_data()
    G = graph._create_graph_from_dfs(nodes, ways, bidirectional=False, backend="rustworkx")
    for idx in G.node_indices():
        data = G.get_node_data(idx)
        assert "osmid" in data, f"Node {idx} missing osmid"
        assert "x" in data, f"Node {idx} missing x"
        assert "y" in data, f"Node {idx} missing y"


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_edge_payloads() -> None:
    """Test that rustworkx edges have osmid attribute."""
    nodes, ways = _read_test_data()
    G = graph._create_graph_from_dfs(nodes, ways, bidirectional=False, backend="rustworkx")
    for idx in G.edge_indices():
        data = G.get_edge_data_by_index(idx)
        assert "osmid" in data, f"Edge {idx} missing osmid"


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_node_id_map() -> None:
    """Test that node_id_map is present and maps to valid OSM IDs."""
    nodes, ways = _read_test_data()
    G = graph._create_graph_from_dfs(nodes, ways, bidirectional=False, backend="rustworkx")
    node_id_map = G.attrs["node_id_map"]
    assert len(node_id_map) == G.num_nodes()
    # All values should be positive integers (OSM IDs)
    for osm_id in node_id_map.values():
        assert isinstance(osm_id, int)
        assert osm_id > 0


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_to_networkx_roundtrip() -> None:
    """Test converting rustworkx graph to NetworkX."""
    nodes, ways = _read_test_data()
    G_rx = graph._create_graph_from_dfs(nodes, ways, bidirectional=False, backend="rustworkx")
    G_nx = dx_convert.rustworkx_to_networkx(G_rx)
    assert isinstance(G_nx, nx.MultiDiGraph)
    assert len(G_nx.nodes) == G_rx.num_nodes()
    assert len(G_nx.edges) == G_rx.num_edges()


def test_rustworkx_import_error() -> None:
    """Test clear error when rustworkx not installed and backend requested."""
    nodes, ways = _read_test_data()
    with (
        unittest.mock.patch.dict("sys.modules", {"rustworkx": None}),
        pytest.raises(ImportError, match="rustworkx"),
    ):
        graph._create_graph_from_dfs(nodes, ways, bidirectional=False, backend="rustworkx")
