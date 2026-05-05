"""Tests for graph_to_arrow / graph_from_arrow round-trip."""

from __future__ import annotations

import networkx as nx
import pyarrow as pa
import pytest
import shapely
from shapely import LineString
from shapely import Point

from ducknx import convert


def _sample_graph() -> nx.MultiDiGraph:
    """Build a small MultiDiGraph with mixed-type node/edge attributes."""
    G = nx.MultiDiGraph(crs="EPSG:4326")
    G.add_node(1, x=13.38, y=52.51, highway="traffic_signals")
    G.add_node(2, x=13.39, y=52.51)
    G.add_node(3, x=13.39, y=52.52, street_count=4)
    G.add_edge(1, 2, key=0, osmid=10, length=80.5, oneway=True, name="A")
    G.add_edge(2, 3, key=0, osmid=11, length=120.0, name="B",
               geometry=LineString([(13.39, 52.51), (13.39, 52.52)]))
    G.add_edge(1, 3, key=0, osmid=12, length=200.0, name="C")
    return G


def test_graph_to_arrow_returns_two_tables() -> None:
    """graph_to_arrow returns Arrow tables for nodes and edges."""
    G = _sample_graph()
    nodes_tbl, edges_tbl = convert.graph_to_arrow(G)

    assert isinstance(nodes_tbl, pa.Table)
    assert isinstance(edges_tbl, pa.Table)
    assert nodes_tbl.num_rows == 3
    assert edges_tbl.num_rows == 3
    for required in ("osmid", "x", "y", "geometry"):
        assert required in nodes_tbl.column_names
    for required in ("u", "v", "key", "geometry"):
        assert required in edges_tbl.column_names


def test_graph_to_arrow_geometry_carries_geoarrow_metadata() -> None:
    """geometry columns advertise the geoarrow.wkb extension."""
    G = _sample_graph()
    nodes_tbl, edges_tbl = convert.graph_to_arrow(G)

    for tbl in (nodes_tbl, edges_tbl):
        meta = tbl.schema.field("geometry").metadata or {}
        assert meta.get(b"ARROW:extension:name") == b"geoarrow.wkb"
        assert b"EPSG:4326" in meta.get(b"ARROW:extension:metadata", b"")


def test_graph_to_arrow_synthesizes_missing_edge_geometry() -> None:
    """Edges without an explicit geometry get a synthesized LineString."""
    G = _sample_graph()
    _nodes_tbl, edges_tbl = convert.graph_to_arrow(G)

    geoms = [shapely.from_wkb(b) for b in edges_tbl.column("geometry").to_pylist()]
    for geom in geoms:
        assert isinstance(geom, LineString)


def test_graph_to_arrow_node_geometry_is_point_wkb() -> None:
    """node geometry is decodable as a Point at the (x, y) coordinate."""
    G = _sample_graph()
    nodes_tbl = convert.graph_to_arrow(G, edges=False)

    geoms = [shapely.from_wkb(b) for b in nodes_tbl.column("geometry").to_pylist()]
    xs = nodes_tbl.column("x").to_pylist()
    ys = nodes_tbl.column("y").to_pylist()
    for geom, x, y in zip(geoms, xs, ys):
        assert isinstance(geom, Point)
        assert geom.x == x
        assert geom.y == y


def test_arrow_round_trip_preserves_topology() -> None:
    """graph_to_arrow → graph_from_arrow round-trips nodes, edges, and CRS."""
    G = _sample_graph()
    nodes_tbl, edges_tbl = convert.graph_to_arrow(G)
    G2 = convert.graph_from_arrow(nodes_tbl, edges_tbl)

    assert isinstance(G2, nx.MultiDiGraph)
    assert set(G2.nodes) == set(G.nodes)
    assert set(G2.edges(keys=True)) == set(G.edges(keys=True))
    assert G2.graph.get("crs") == "EPSG:4326"

    # node attrs preserved
    assert G2.nodes[1]["highway"] == "traffic_signals"
    assert G2.nodes[3]["street_count"] == 4

    # edge attrs preserved
    assert G2.edges[1, 2, 0]["osmid"] == 10
    assert G2.edges[1, 2, 0]["length"] == pytest.approx(80.5)
    assert G2.edges[2, 3, 0]["name"] == "B"
    assert isinstance(G2.edges[2, 3, 0]["geometry"], LineString)


def test_graph_from_arrow_rejects_missing_required_columns() -> None:
    """graph_from_arrow surfaces missing required columns with ValueError."""
    bad_nodes = pa.table({"osmid": pa.array([1], type=pa.int64())})
    bad_edges = pa.table(
        {
            "u": pa.array([1], type=pa.int64()),
            "v": pa.array([1], type=pa.int64()),
            "key": pa.array([0], type=pa.int64()),
        },
    )
    with pytest.raises(ValueError, match="x"):
        convert.graph_from_arrow(bad_nodes, bad_edges)
