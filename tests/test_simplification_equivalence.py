"""Equivalence and unit tests for the Phase A simplification rewrite."""

from __future__ import annotations

from collections import Counter

import networkx as nx
import numpy as np
import pytest
from hypothesis import given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from ducknx import simplification as simp


def _toy_graph() -> nx.MultiDiGraph:
    """Build a tiny deterministic graph for adjacency tests.

    Topology:  1 -> 2 -> 3 -> 4
                         ^
                         5 -> 3 (so 3 has 2 predecessors)
    """
    G = nx.MultiDiGraph()
    for nid, x, y in [
        (1, 0.0, 0.0), (2, 1.0, 0.0), (3, 2.0, 0.0),
        (4, 3.0, 0.0), (5, 2.0, 1.0),
    ]:
        G.add_node(nid, x=x, y=y)
    G.add_edge(1, 2)
    G.add_edge(2, 3)
    G.add_edge(3, 4)
    G.add_edge(5, 3)
    return G


def test_build_adjacency_roundtrip() -> None:
    """Adjacency view correctly captures graph structure."""
    G = _toy_graph()
    adj = simp._build_adjacency(G)

    assert list(adj.node_ids) == sorted(G.nodes)
    idx3 = adj.osmid_to_idx[3]
    succ3 = adj.succ_indices[adj.succ_indptr[idx3]: adj.succ_indptr[idx3 + 1]]
    assert {int(adj.node_ids[s]) for s in succ3} == {4}
    pred3 = adj.pred_indices[adj.pred_indptr[idx3]: adj.pred_indptr[idx3 + 1]]
    assert {int(adj.node_ids[p]) for p in pred3} == {2, 5}
    assert adj.xs[adj.osmid_to_idx[3]] == 2.0
    assert adj.ys[adj.osmid_to_idx[5]] == 1.0


def test_endpoints_match_legacy_on_toy() -> None:
    """Vectorized endpoint detection matches legacy on toy graph."""
    G = _toy_graph()
    adj = simp._build_adjacency(G)
    legacy = simp._identify_endpoints(G, None, None)
    fast = simp._identify_endpoints_vectorized(G, adj, None, None)
    assert legacy == fast


def test_endpoints_match_legacy_node_attrs_include() -> None:
    """node_attrs_include path matches legacy."""
    G = _toy_graph()
    G.nodes[2]["highway"] = "traffic_signals"
    adj = simp._build_adjacency(G)
    legacy = simp._identify_endpoints(G, ["highway"], None)
    fast = simp._identify_endpoints_vectorized(G, adj, ["highway"], None)
    assert legacy == fast


def test_endpoints_match_legacy_edge_attrs_differ() -> None:
    """edge_attrs_differ path matches legacy."""
    G = _toy_graph()
    next(iter(G[2][3].values()))["osmid"] = 100
    next(iter(G[3][4].values()))["osmid"] = 200
    adj = simp._build_adjacency(G)
    legacy = simp._identify_endpoints(G, None, ["osmid"])
    fast = simp._identify_endpoints_vectorized(G, adj, None, ["osmid"])
    assert legacy == fast


def test_trace_paths_match_legacy_on_toy() -> None:
    """Trace paths produces same path set as legacy _get_paths_to_simplify."""
    G = _toy_graph()
    adj = simp._build_adjacency(G)
    legacy_paths, legacy_endpoints = simp._get_paths_to_simplify(G, None, None)
    endpoints = simp._identify_endpoints_vectorized(G, adj, None, None)
    offsets, nodes_flat = simp._trace_paths(adj, endpoints)

    new_paths = [
        [int(adj.node_ids[idx]) for idx in nodes_flat[offsets[i]: offsets[i + 1]]]
        for i in range(offsets.size - 1)
    ]
    assert {tuple(p) for p in new_paths} == {tuple(p) for p in legacy_paths}
    assert legacy_endpoints == endpoints


def test_path_geometries_bulk_matches_legacy() -> None:
    """Bulk-built geometries match per-path coordinates."""
    G = _toy_graph()
    adj = simp._build_adjacency(G)
    endpoints = simp._identify_endpoints_vectorized(G, adj, None, None)
    offsets, nodes_flat = simp._trace_paths(adj, endpoints)
    geoms = simp._build_path_geometries(offsets, nodes_flat, adj.xs, adj.ys)

    for i in range(offsets.size - 1):
        path_idxs = nodes_flat[offsets[i]: offsets[i + 1]]
        coords = [(adj.xs[idx], adj.ys[idx]) for idx in path_idxs]
        assert list(geoms[i].coords) == coords


@hyp_settings(max_examples=200, deadline=None)
@given(
    n_nodes=st.integers(min_value=2, max_value=20),
    edge_seed=st.integers(min_value=0, max_value=2**31 - 1),
)
def test_endpoint_kernel_matches_legacy_on_random_graphs(
    n_nodes: int, edge_seed: int,
) -> None:
    """Property test: vectorized endpoints == legacy endpoints on random graphs."""
    rng = np.random.default_rng(edge_seed)
    G = nx.MultiDiGraph()
    for i in range(n_nodes):
        G.add_node(i, x=float(i), y=float(rng.integers(0, 5)))
    n_edges = int(rng.integers(0, n_nodes * 3))
    for _ in range(n_edges):
        u = int(rng.integers(0, n_nodes))
        v = int(rng.integers(0, n_nodes))
        G.add_edge(u, v)

    adj = simp._build_adjacency(G)
    legacy = simp._identify_endpoints(G, None, None)
    fast = simp._identify_endpoints_vectorized(G, adj, None, None)
    assert legacy == fast


def _build_realistic_graph() -> nx.MultiDiGraph:
    """A graph rich enough to exercise list-valued attrs, geometry, and rings."""
    G = nx.MultiDiGraph()
    coords = {
        1: (0, 0), 2: (1, 0), 3: (2, 0), 4: (3, 0), 5: (4, 0),
        6: (4, 1), 7: (3, 1), 8: (2, 1), 9: (1, 1), 10: (0, 1),
    }
    for nid, (x, y) in coords.items():
        G.add_node(nid, x=float(x), y=float(y))
    edges = [
        (1, 2, {"osmid": 100, "highway": "residential", "length": 1.0}),
        (2, 3, {"osmid": 100, "highway": "residential", "length": 1.0}),
        (3, 4, {"osmid": 200, "highway": "residential", "length": 1.0}),
        (4, 5, {"osmid": 200, "highway": "residential", "length": 1.0}),
        (5, 6, {"osmid": 300, "highway": "residential", "length": 1.0}),
        (6, 7, {"osmid": 300, "highway": "residential", "length": 1.0}),
        (7, 8, {"osmid": 300, "highway": "residential", "length": 1.0}),
        (8, 9, {"osmid": 400, "highway": "residential", "length": 1.0}),
        (9, 10, {"osmid": 400, "highway": "residential", "length": 1.0}),
        (10, 1, {"osmid": 400, "highway": "residential", "length": 1.0}),
    ]
    for u, v, d in edges:
        G.add_edge(u, v, **d)
        G.add_edge(v, u, **{**d, "reversed": True})
    G.graph["crs"] = "EPSG:4326"
    return G


def test_simplify_graph_matches_legacy_on_realistic() -> None:
    """End-to-end equivalence on a richer fixture."""
    G = _build_realistic_graph()
    Glegacy = simp._simplify_graph_legacy(G.copy())
    Gnew = simp.simplify_graph(G.copy())

    assert set(Glegacy.nodes) == set(Gnew.nodes)
    assert Counter(Glegacy.edges()) == Counter(Gnew.edges())

    for u, v in set(Glegacy.edges()):
        legacy_attrs = next(iter(Glegacy[u][v].values()))
        new_attrs = next(iter(Gnew[u][v].values()))
        assert legacy_attrs["geometry"].equals_exact(new_attrs["geometry"], 1e-9)
        assert legacy_attrs["length"] == pytest.approx(new_attrs["length"], rel=1e-9)
        for key in ("osmid", "highway"):
            lv = legacy_attrs.get(key)
            nv = new_attrs.get(key)
            if isinstance(lv, list) or isinstance(nv, list):
                lset = set(lv) if isinstance(lv, list) else {lv}
                nset = set(nv) if isinstance(nv, list) else {nv}
                assert lset == nset
            else:
                assert lv == nv


def test_simplify_graph_handles_list_valued_attrs() -> None:
    """Pre-merged list attrs (e.g. osmid=[100,200]) must not crash aggregation."""
    G = _build_realistic_graph()
    # Make node 1 an endpoint so the ring removal does not delete everything.
    G.nodes[1]["highway"] = "traffic_signals"
    next(iter(G[1][2].values()))["osmid"] = [100, 100]
    Gs = simp.simplify_graph(G.copy(), node_attrs_include=["highway"])
    assert Gs.number_of_nodes() > 0


def test_consolidate_intersections_smoke() -> None:
    """consolidate_intersections still works after polars refactor."""
    G = _build_realistic_graph()
    Gc = simp.consolidate_intersections(G.copy(), tolerance=2.0, rebuild_graph=True)
    assert isinstance(Gc, nx.MultiDiGraph)
    assert len(Gc.nodes) <= len(G.nodes)
