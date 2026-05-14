"""Tests that _create_graph_from_dfs produces equivalent graphs after attr dedup."""

from __future__ import annotations

import networkx as nx
import pyarrow as pa

from ducknx import graph as g


def _make_minimal_inputs() -> tuple[pa.Table, pa.Table]:
    """Return tiny ``(nodes, ways)`` Arrow tables for build-path tests."""
    nodes = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "x": pa.array([0.0, 1.0, 2.0]),
        "y": pa.array([0.0, 0.0, 0.0]),
        "highway": pa.array([None, None, None], type=pa.string()),
    })
    ways = pa.table({
        "osmid": pa.array([10, 11], type=pa.int64()),
        "refs": pa.array([[1, 2], [2, 3]], type=pa.list_(pa.int64())),
        "highway": pa.array(["residential", "residential"]),
        "oneway": pa.array([None, None], type=pa.string()),
        "name": pa.array(["A", "B"]),
    })
    return nodes, ways


def test_graph_build_basic_topology() -> None:
    """Basic build produces expected nodes and bidirectional edges."""
    nodes, ways = _make_minimal_inputs()
    G = g._create_graph_from_dfs(nodes, ways, bidirectional=False)
    assert isinstance(G, nx.MultiDiGraph)
    assert set(G.nodes) == {1, 2, 3}
    # oneway not set => default both directions
    assert set(G.edges()) == {(1, 2), (2, 1), (2, 3), (3, 2)}
    fwd = next(iter(G[1][2].values()))
    rev = next(iter(G[2][1].values()))
    assert fwd["osmid"] == 10
    assert fwd["reversed"] is False
    assert rev["reversed"] is True
    assert fwd["name"] == rev["name"] == "A"


def test_shared_template_does_not_alias_per_edge_length() -> None:
    """add_edge_lengths writes per-edge length; templates must not alias it."""
    from ducknx import distance

    ways = pa.table({
        "osmid": pa.array([10], type=pa.int64()),
        "refs": pa.array([[1, 2, 3]], type=pa.list_(pa.int64())),
        "highway": pa.array(["residential"]),
        "oneway": pa.array([None], type=pa.string()),
        "name": pa.array(["A"]),
    })
    nodes = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "x": pa.array([0.0, 1.0, 2.0]),
        "y": pa.array([0.0, 0.0, 0.0]),
        "highway": pa.array([None, None, None], type=pa.string()),
    })
    G = g._create_graph_from_dfs(nodes, ways, bidirectional=False)
    G = distance.add_edge_lengths(G)
    e12 = next(iter(G[1][2].values()))
    e23 = next(iter(G[2][3].values()))
    assert id(e12) != id(e23), "edge attr dicts must not be aliased"
