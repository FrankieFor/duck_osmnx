"""Tests for the Arrow/polars rewrite of simplification + truncate."""

from __future__ import annotations

import networkx as nx
import pyarrow as pa
import pytest
import shapely
from shapely import LineString
from shapely import Polygon

from ducknx import simplification
from ducknx import truncate


def _grid_graph() -> nx.MultiDiGraph:
    """Tiny projected graph: two nodes 0.5m apart, one farther node."""
    G = nx.MultiDiGraph(crs="EPSG:32633")
    # cluster A: nodes 1 & 2 within 0.5m
    G.add_node(1, x=100.0, y=100.0, street_count=2, elevation=10.0)
    G.add_node(2, x=100.5, y=100.0, street_count=2, elevation=12.0)
    # cluster B: lone node 3 50m away
    G.add_node(3, x=150.0, y=100.0, street_count=2, elevation=14.0)
    G.add_edge(1, 2, key=0, length=0.5, name="A", osmid=1,
               geometry=LineString([(100.0, 100.0), (100.5, 100.0)]))
    G.add_edge(2, 3, key=0, length=49.5, name="B", osmid=2,
               geometry=LineString([(100.5, 100.0), (150.0, 100.0)]))
    G.add_edge(3, 1, key=0, length=50.0, name="C", osmid=3,
               geometry=LineString([(150.0, 100.0), (100.0, 100.0)]))
    return G


def test_consolidate_geom_only_returns_arrow_table() -> None:
    """rebuild_graph=False returns a pa.Table with geoarrow.wkb geometry."""
    G = _grid_graph()
    tbl = simplification.consolidate_intersections(
        G, tolerance=1.0, rebuild_graph=False, dead_ends=True,
    )
    assert isinstance(tbl, pa.Table)
    assert tbl.column_names == ["geometry"]
    meta = tbl.schema.field("geometry").metadata or {}
    assert meta.get(b"ARROW:extension:name") == b"geoarrow.wkb"
    # nodes 1 & 2 collapse into one cluster; node 3 stands alone → 2 centroids
    assert tbl.num_rows == 2

    geoms = [shapely.from_wkb(b) for b in tbl.column("geometry").to_pylist()]
    assert all(g.geom_type == "Point" for g in geoms)


def test_consolidate_rebuild_merges_close_nodes() -> None:
    """rebuild_graph=True collapses near-duplicate nodes into one cluster."""
    G = _grid_graph()
    Gc = simplification.consolidate_intersections(
        G, tolerance=1.0, rebuild_graph=True, dead_ends=True,
    )
    assert isinstance(Gc, nx.MultiDiGraph)
    # nodes 1 & 2 should merge → 2 consolidated nodes total
    assert len(Gc.nodes) == 2
    merged_nodes = [n for n, d in Gc.nodes(data=True)
                    if isinstance(d.get("osmid_original"), list)]
    assert len(merged_nodes) == 1
    merged = Gc.nodes[merged_nodes[0]]
    assert sorted(merged["osmid_original"]) == [1, 2]
    # elevation is averaged via node_attr_aggs default
    assert merged["elevation"] == pytest.approx(11.0)


def test_consolidate_empty_graph_returns_empty_table() -> None:
    """Empty graph + rebuild_graph=False → empty Arrow table."""
    G = nx.MultiDiGraph(crs="EPSG:32633")
    tbl = simplification.consolidate_intersections(
        G, tolerance=1.0, rebuild_graph=False, dead_ends=True,
    )
    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 0
    assert tbl.column_names == ["geometry"]


def test_truncate_graph_polygon_keeps_inside_nodes() -> None:
    """truncate_graph_polygon keeps nodes inside the polygon, drops the rest."""
    G = nx.MultiDiGraph(crs="EPSG:4326")
    G.add_node(1, x=0.0, y=0.0)
    G.add_node(2, x=0.5, y=0.5)
    G.add_node(3, x=10.0, y=10.0)
    G.add_edge(1, 2, key=0)
    G.add_edge(2, 3, key=0)
    keep_box = Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)])

    Gt = truncate.truncate_graph_polygon(G, keep_box)

    assert set(Gt.nodes) == {1, 2}


def test_truncate_graph_polygon_truncate_by_edge_keeps_neighbors() -> None:
    """truncate_by_edge=True retains exterior nodes adjacent to interior nodes."""
    G = nx.MultiDiGraph(crs="EPSG:4326")
    G.add_node(1, x=0.0, y=0.0)
    G.add_node(2, x=0.5, y=0.5)
    G.add_node(3, x=10.0, y=10.0)  # outside but connected to 2
    G.add_node(4, x=20.0, y=20.0)  # outside and isolated from inside
    G.add_edge(1, 2, key=0)
    G.add_edge(2, 3, key=0)
    G.add_edge(3, 4, key=0)
    keep_box = Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)])

    Gt = truncate.truncate_graph_polygon(G, keep_box, truncate_by_edge=True)

    # node 3 is kept because it neighbors interior node 2; node 4 is dropped
    assert 3 in Gt.nodes
    assert 4 not in Gt.nodes


def test_truncate_graph_polygon_raises_when_empty() -> None:
    """Empty intersection raises ValueError."""
    G = nx.MultiDiGraph(crs="EPSG:4326")
    G.add_node(1, x=100.0, y=100.0)
    G.add_node(2, x=101.0, y=101.0)
    G.add_edge(1, 2, key=0)
    keep_box = Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)])
    with pytest.raises(ValueError, match="no graph nodes"):
        truncate.truncate_graph_polygon(G, keep_box)
