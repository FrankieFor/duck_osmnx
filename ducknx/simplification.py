"""Simplify, correct, and consolidate spatial graph nodes and edges."""

from __future__ import annotations

import logging as lg
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any

import networkx as nx
import numpy as np
import polars as pl
import pyarrow as pa
import shapely
from shapely import LineString
from shapely import Point
from shapely import STRtree

from . import stats
from . import utils
from ._errors import GraphSimplificationError

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True)
class AdjacencyView:
    """
    Read-only CSR snapshot of a MultiDiGraph for vectorized topology work.

    Parameters
    ----------
    node_ids
        Sorted int64 array of OSM node IDs (the row order for all per-node arrays).
    osmid_to_idx
        Mapping from OSM node ID to row index in ``node_ids``.
    succ_indptr
        CSR successor pointer array (length ``n + 1``).
    succ_indices
        CSR successor row-index array.
    pred_indptr
        CSR predecessor pointer array (length ``n + 1``).
    pred_indices
        CSR predecessor row-index array.
    xs
        Per-node x coordinate array.
    ys
        Per-node y coordinate array.
    out_degree
        Per-node out-degree (parallel edges counted).
    in_degree
        Per-node in-degree (parallel edges counted).
    """

    node_ids: np.ndarray
    osmid_to_idx: dict[int, int]
    succ_indptr: np.ndarray
    succ_indices: np.ndarray
    pred_indptr: np.ndarray
    pred_indices: np.ndarray
    xs: np.ndarray
    ys: np.ndarray
    out_degree: np.ndarray
    in_degree: np.ndarray


def _build_adjacency(G: nx.MultiDiGraph) -> AdjacencyView:
    """
    Build a CSR adjacency snapshot from a MultiDiGraph.

    Uses ``G.adj`` and ``G.pred`` directly to avoid per-node Python method
    calls. Multi-edges between the same ``(u, v)`` contribute one CSR entry
    per parallel edge so endpoint rule 3 (degree 4 = parallel-edge case)
    sees the same shape as NetworkX would report.

    Parameters
    ----------
    G
        Input MultiDiGraph.

    Returns
    -------
    adj
        Frozen ``AdjacencyView`` snapshot.
    """
    n = G.number_of_nodes()
    node_ids = np.fromiter(sorted(G.nodes), dtype=np.int64, count=n)
    osmid_to_idx = {int(nid): i for i, nid in enumerate(node_ids)}

    succ_counts = np.zeros(n, dtype=np.int64)
    pred_counts = np.zeros(n, dtype=np.int64)

    adj = G.adj
    pred = G.pred
    for u, succs in adj.items():
        ui = osmid_to_idx[u]
        for _v, ekeys in succs.items():
            succ_counts[ui] += len(ekeys)
    for v, preds in pred.items():
        vi = osmid_to_idx[v]
        for _u, ekeys in preds.items():
            pred_counts[vi] += len(ekeys)

    succ_indptr = np.empty(n + 1, dtype=np.int64)
    pred_indptr = np.empty(n + 1, dtype=np.int64)
    succ_indptr[0] = 0
    pred_indptr[0] = 0
    np.cumsum(succ_counts, out=succ_indptr[1:])
    np.cumsum(pred_counts, out=pred_indptr[1:])

    succ_indices = np.empty(int(succ_indptr[-1]), dtype=np.int64)
    pred_indices = np.empty(int(pred_indptr[-1]), dtype=np.int64)

    succ_cursor = succ_indptr[:-1].copy()
    pred_cursor = pred_indptr[:-1].copy()
    for u, succs in adj.items():
        ui = osmid_to_idx[u]
        for v, ekeys in succs.items():
            vi = osmid_to_idx[v]
            for _ in ekeys:
                succ_indices[succ_cursor[ui]] = vi
                succ_cursor[ui] += 1
    for v, preds in pred.items():
        vi = osmid_to_idx[v]
        for u, ekeys in preds.items():
            ui = osmid_to_idx[u]
            for _ in ekeys:
                pred_indices[pred_cursor[vi]] = ui
                pred_cursor[vi] += 1

    xs = np.fromiter(
        (G.nodes[int(nid)]["x"] for nid in node_ids), dtype=np.float64, count=n,
    )
    ys = np.fromiter(
        (G.nodes[int(nid)]["y"] for nid in node_ids), dtype=np.float64, count=n,
    )

    return AdjacencyView(
        node_ids=node_ids,
        osmid_to_idx=osmid_to_idx,
        succ_indptr=succ_indptr,
        succ_indices=succ_indices,
        pred_indptr=pred_indptr,
        pred_indices=pred_indices,
        xs=xs,
        ys=ys,
        out_degree=succ_counts,
        in_degree=pred_counts,
    )


def _is_endpoint(
    G: nx.MultiDiGraph,
    node: int,
    node_attrs_include: Iterable[str] | None,
    edge_attrs_differ: Iterable[str] | None,
) -> bool:
    """
    Determine if a node is a true endpoint of an edge.

    Return True if the node is a "true" endpoint of an edge in the network,
    otherwise False. OpenStreetMap data includes many nodes that exist only as
    geometric vertices to allow ways to curve. `node` is a true edge endpoint
    if it satisfies at least 1 of the following 5 rules:

    1) It is its own neighbor (ie, it self-loops).

    2) Or, it has no incoming edges or no outgoing edges (ie, all its incident
    edges are inbound or all its incident edges are outbound).

    3) Or, it does not have exactly two neighbors and degree of 2 or 4.

    4) Or, if `node_attrs_include` is not None and it has one or more of the
    attributes in `node_attrs_include`.

    5) Or, if `edge_attrs_differ` is not None and its incident edges have
    different values than each other for any of the edge attributes in
    `edge_attrs_differ`.

    Parameters
    ----------
    G
        Input graph.
    node
        The ID of the node to check.
    node_attrs_include
        Node attribute names for relaxing the strictness of endpoint
        determination. A node is always an endpoint if it possesses one or
        more of the attributes in `node_attrs_include`.
    edge_attrs_differ
        Edge attribute names for relaxing the strictness of endpoint
        determination. A node is always an endpoint if its incident edges have
        different values than each other for any attribute in
        `edge_attrs_differ`.

    Returns
    -------
    endpoint
        True if node is an endpoint, otherwise False.
    """
    neighbors = set(list(G.predecessors(node)) + list(G.successors(node)))
    n = len(neighbors)
    d = G.degree(node)

    # RULE 1
    # if the node appears in its list of neighbors, it self-loops: this is
    # always an endpoint
    if node in neighbors:
        return True

    # RULE 2
    # if node has no incoming edges or no outgoing edges, it is an endpoint
    if G.out_degree(node) == 0 or G.in_degree(node) == 0:
        return True

    # RULE 3
    # else, if it does NOT have 2 neighbors AND either 2 or 4 directed edges,
    # it is an endpoint. either it has 1 or 3+ neighbors, in which case it is
    # a dead-end or an intersection of multiple streets or it has 2 neighbors
    # but 3 degree (indicating a change from oneway to twoway) or more than 4
    # degree (indicating a parallel edge) and thus is an endpoint
    if not ((n == 2) and (d in {2, 4})):  # noqa: PLR2004
        return True

    # RULE 4
    # non-strict mode: does it contain an attr denoting that it is an endpoint
    if node_attrs_include is not None and len(set(node_attrs_include) & G.nodes[node].keys()) > 0:
        return True

    # RULE 5
    # non-strict mode: do its incident edges have different attr values? for
    # each attribute to check, collect the attribute's values in all inbound
    # and outbound edges. if there is more than 1 unique value then this node
    # is an endpoint
    if edge_attrs_differ is not None:
        for attr in edge_attrs_differ:
            in_values = {v for _, _, v in G.in_edges(node, data=attr, keys=False)}
            out_values = {v for _, _, v in G.out_edges(node, data=attr, keys=False)}
            if len(in_values | out_values) > 1:
                return True

    # if none of the preceding rules passed, then it is not an endpoint
    return False


def _identify_endpoints(
    G: nx.MultiDiGraph,
    node_attrs_include: Iterable[str] | None,
    edge_attrs_differ: Iterable[str] | None,
) -> set[int]:
    """
    Identify all endpoint nodes in the graph.

    Parameters
    ----------
    G
        Input graph.
    node_attrs_include
        Node attribute names for relaxing the strictness of endpoint
        determination. A node is always an endpoint if it possesses one or
        more of the attributes in `node_attrs_include`.
    edge_attrs_differ
        Edge attribute names for relaxing the strictness of endpoint
        determination. A node is always an endpoint if its incident edges have
        different values than each other for any attribute in
        `edge_attrs_differ`.

    Returns
    -------
    endpoints
        The set of all nodes in the graph that are endpoints.
    """
    return {n for n in G.nodes if _is_endpoint(G, n, node_attrs_include, edge_attrs_differ)}


def _build_path(
    G: nx.MultiDiGraph,
    endpoint: int,
    endpoint_successor: int,
    endpoints: set[int],
) -> list[int]:
    """
    Build a path of nodes from one endpoint node to next endpoint node.

    Parameters
    ----------
    G
        Input graph.
    endpoint
        The endpoint node from which to start the path.
    endpoint_successor
        The successor of endpoint through which the path to the next endpoint
        will be built.
    endpoints
        The set of all nodes in the graph that are endpoints.

    Returns
    -------
    path
        The first and last items in the resulting path list are endpoint
        nodes, and all other items are interstitial nodes that can be removed
        subsequently.
    """
    # start building path from endpoint node through its successor
    path = [endpoint, endpoint_successor]
    path_set = {endpoint, endpoint_successor}

    # for each successor of the endpoint's successor
    for this_successor in G.successors(endpoint_successor):
        successor = this_successor
        if successor not in path_set:
            # if this successor is already in the path, ignore it, otherwise add
            # it to the path
            path.append(successor)
            path_set.add(successor)
            while successor not in endpoints:
                # find successors (of current successor) not in path
                successors = [n for n in G.successors(successor) if n not in path_set]

                # 99%+ of the time there will be only 1 successor: add to path
                if len(successors) == 1:
                    successor = successors[0]
                    path.append(successor)
                    path_set.add(successor)

                # handle relatively rare cases or OSM digitization quirks
                elif len(successors) == 0:
                    if endpoint in G.successors(successor):
                        # we have come to the end of a self-looping edge, so
                        # add first node to end of path to close it and return
                        return [*path, endpoint]

                    # otherwise, this can happen due to OSM digitization error
                    # where a one-way street turns into a two-way here, but
                    # duplicate incoming one-way edges are present
                    msg = f"Unexpected simplify pattern handled near {successor}"
                    utils.log(msg, level=lg.WARNING)
                    return path
                else:  # pragma: no cover
                    # if successor has >1 successors, then successor must have
                    # been an endpoint because you can go in 2 new directions.
                    # this should never occur in practice
                    msg = f"Impossible simplify pattern failed near {successor}."
                    raise GraphSimplificationError(msg)

            # if this successor is an endpoint, we've completed the path
            return path

    # if endpoint_successor has no successors not already in the path, return
    # the current path: this is usually due to a digitization quirk on OSM
    return path


def _get_paths_to_simplify(
    G: nx.MultiDiGraph,
    node_attrs_include: Iterable[str] | None,
    edge_attrs_differ: Iterable[str] | None,
) -> tuple[list[list[int]], set[int]]:
    """
    Get all the paths to be simplified between endpoint nodes.

    The path is ordered from the first endpoint, through the interstitial nodes,
    to the second endpoint.

    Parameters
    ----------
    G
        Input graph.
    node_attrs_include
        Node attribute names for relaxing the strictness of endpoint
        determination. A node is always an endpoint if it possesses one or
        more of the attributes in `node_attrs_include`.
    edge_attrs_differ
        Edge attribute names for relaxing the strictness of endpoint
        determination. A node is always an endpoint if its incident edges have
        different values than each other for any attribute in
        `edge_attrs_differ`.

    Returns
    -------
    paths_endpoints
        A tuple of (paths, endpoints) where paths is a list of paths to
        simplify and endpoints is the set of all endpoint nodes.
    """
    # first identify all the nodes that are endpoints
    endpoints = _identify_endpoints(G, node_attrs_include, edge_attrs_differ)
    msg = f"Identified {len(endpoints):,} edge endpoints"
    utils.log(msg, level=lg.INFO)

    # for each endpoint node, look at each of its successor nodes
    paths = [
        _build_path(G, endpoint, successor, endpoints)
        for endpoint in endpoints
        for successor in G.successors(endpoint)
        if successor not in endpoints
    ]

    return paths, endpoints


def _remove_rings(
    G: nx.MultiDiGraph,
    endpoints: set[int],
) -> nx.MultiDiGraph:
    """
    Remove all graph components that consist only of a single chordless cycle.

    This identifies all connected components in the graph that consist only of
    a single isolated self-contained ring, and removes them from the graph.

    Parameters
    ----------
    G
        Input graph.
    endpoints
        The set of all nodes in the graph that are endpoints.

    Returns
    -------
    G
        Graph with all chordless cycle components removed.
    """
    to_remove = set()
    for wcc in nx.weakly_connected_components(G):
        if not any(n in endpoints for n in wcc):
            to_remove.update(wcc)
    G.remove_nodes_from(to_remove)
    return G


def _identify_endpoints_vectorized(
    G: nx.MultiDiGraph,
    adj: AdjacencyView,
    node_attrs_include: Iterable[str] | None,
    edge_attrs_differ: Iterable[str] | None,
) -> set[int]:
    """
    Vectorized endpoint detection over a CSR adjacency snapshot.

    Rules 1-3 (self-loop, dangling, neighbor/degree mismatch) are evaluated
    as numpy operations on flat CSR arrays. Rules 4-5 fall back to per-node
    checks but only for nodes that survived rules 1-3.

    Parameters
    ----------
    G
        Input graph (used for attribute access in rules 4-5).
    adj
        CSR adjacency snapshot.
    node_attrs_include
        Same semantics as ``_identify_endpoints``.
    edge_attrs_differ
        Same semantics as ``_identify_endpoints``.

    Returns
    -------
    endpoints
        Set of OSM node IDs that are endpoints.
    """
    n = adj.node_ids.size
    is_endpoint = np.zeros(n, dtype=bool)

    # Per-edge source index for each CSR successor / predecessor entry.
    src_succ = np.repeat(np.arange(n, dtype=np.int64), np.diff(adj.succ_indptr))
    src_pred = np.repeat(np.arange(n, dtype=np.int64), np.diff(adj.pred_indptr))

    # Rule 1: self-loops
    self_loop_mask = src_succ == adj.succ_indices
    is_endpoint[src_succ[self_loop_mask]] = True

    # Rule 2: zero in/out degree
    is_endpoint |= (adj.in_degree == 0) | (adj.out_degree == 0)

    # Rule 3: unique-neighbor count + degree check (vectorized)
    all_src = np.concatenate([src_succ, src_pred])
    all_nbr = np.concatenate([adj.succ_indices, adj.pred_indices])
    if all_src.size > 0:
        order = np.lexsort((all_nbr, all_src))
        s_sorted = all_src[order]
        n_sorted = all_nbr[order]
        first = np.empty(s_sorted.size, dtype=bool)
        first[0] = True
        first[1:] = (s_sorted[1:] != s_sorted[:-1]) | (n_sorted[1:] != n_sorted[:-1])
        unique_counts = np.bincount(s_sorted[first], minlength=n).astype(np.int64)
    else:
        unique_counts = np.zeros(n, dtype=np.int64)
    degree = adj.in_degree + adj.out_degree
    rule3_pass = (unique_counts == 2) & ((degree == 2) | (degree == 4))  # noqa: PLR2004
    is_endpoint |= ~rule3_pass

    # Rules 4-5: per-node fallback only for survivors
    if node_attrs_include is not None or edge_attrs_differ is not None:
        attrs_set = set(node_attrs_include) if node_attrs_include is not None else None
        edge_attrs_list = list(edge_attrs_differ) if edge_attrs_differ is not None else None
        survivors = np.where(~is_endpoint)[0]
        for i in survivors:
            osmid = int(adj.node_ids[i])
            if attrs_set is not None and len(attrs_set & G.nodes[osmid].keys()) > 0:
                is_endpoint[i] = True
                continue
            if edge_attrs_list is not None:
                hit = False
                for attr in edge_attrs_list:
                    in_values = {v for _, _, v in G.in_edges(osmid, data=attr, keys=False)}
                    out_values = {v for _, _, v in G.out_edges(osmid, data=attr, keys=False)}
                    if len(in_values | out_values) > 1:
                        hit = True
                        break
                if hit:
                    is_endpoint[i] = True

    return {int(adj.node_ids[i]) for i in np.where(is_endpoint)[0]}


def _trace_paths(
    adj: AdjacencyView,
    endpoints: set[int],
) -> tuple[np.ndarray, np.ndarray]:
    """
    Trace simplification paths between endpoint nodes via CSR DFS.

    Mirrors the semantics of ``_build_path`` but operates on integer indices
    into ``adj.node_ids`` and emits flat ``(offsets, nodes_flat)`` arrays.

    Parameters
    ----------
    adj
        CSR adjacency snapshot.
    endpoints
        Set of endpoint OSM node IDs.

    Returns
    -------
    offsets
        Int64 array; path ``i`` spans ``nodes_flat[offsets[i]:offsets[i+1]]``.
    nodes_flat
        Int64 array of row indices into ``adj.node_ids``.
    """
    is_endpoint = np.zeros(adj.node_ids.size, dtype=bool)
    for e in endpoints:
        is_endpoint[adj.osmid_to_idx[e]] = True

    endpoint_idxs = sorted(adj.osmid_to_idx[e] for e in endpoints)

    paths_offsets: list[int] = [0]
    paths_nodes: list[int] = []

    for ei in endpoint_idxs:
        s, e = adj.succ_indptr[ei], adj.succ_indptr[ei + 1]
        # collect unique successors only (parallel multi-edges duplicate)
        seen_succs: set[int] = set()
        for succ_val in adj.succ_indices[s:e]:
            succ = int(succ_val)
            if succ in seen_succs:
                continue
            seen_succs.add(succ)
            if is_endpoint[succ]:
                continue

            path = [ei, succ]
            path_set = {ei, succ}

            ss, se = adj.succ_indptr[succ], adj.succ_indptr[succ + 1]
            picked: int | None = None
            for nxt in adj.succ_indices[ss:se]:
                nxti = int(nxt)
                if nxti not in path_set:
                    picked = nxti
                    break
            if picked is None:
                paths_nodes.extend(path)
                paths_offsets.append(len(paths_nodes))
                continue

            path.append(picked)
            path_set.add(picked)

            current = picked
            while not is_endpoint[current]:
                cs, ce = adj.succ_indptr[current], adj.succ_indptr[current + 1]
                onward = [int(x) for x in adj.succ_indices[cs:ce] if int(x) not in path_set]
                if len(onward) == 1:
                    current = onward[0]
                    path.append(current)
                    path_set.add(current)
                elif not onward:
                    cs2, ce2 = adj.succ_indptr[current], adj.succ_indptr[current + 1]
                    if ei in adj.succ_indices[cs2:ce2]:
                        path.append(ei)
                    break
                else:  # pragma: no cover
                    msg = f"Impossible simplify pattern at node idx {current}"
                    raise GraphSimplificationError(msg)

            paths_nodes.extend(path)
            paths_offsets.append(len(paths_nodes))

    return (
        np.asarray(paths_offsets, dtype=np.int64),
        np.asarray(paths_nodes, dtype=np.int64),
    )


def _build_path_geometries(
    offsets: np.ndarray,
    nodes_flat: np.ndarray,
    xs: np.ndarray,
    ys: np.ndarray,
) -> np.ndarray:
    """
    Bulk-build LineString geometries for every path in one shapely call.

    Parameters
    ----------
    offsets
        Path offsets (int64), length ``num_paths + 1``.
    nodes_flat
        Flat array of node indices into ``xs``/``ys``.
    xs
        X coordinate array aligned with ``adj.node_ids``.
    ys
        Y coordinate array aligned with ``adj.node_ids``.

    Returns
    -------
    geoms
        Numpy object array of shapely LineStrings, one per path.
    """
    if nodes_flat.size == 0:
        return np.empty(0, dtype=object)
    coords = np.empty((nodes_flat.size, 2), dtype=np.float64)
    coords[:, 0] = xs[nodes_flat]
    coords[:, 1] = ys[nodes_flat]
    line_idx = np.repeat(
        np.arange(offsets.size - 1, dtype=np.int64),
        np.diff(offsets),
    )
    return shapely.linestrings(coords, indices=line_idx)


def _aggregate_path_attrs(
    G: nx.MultiDiGraph,
    path_osmids: list[int],
    edge_attr_aggs: dict[str, Any],
    *,
    track_merged: bool,
) -> tuple[dict[str, Any], list[tuple[int, int]]]:
    """
    Aggregate attrs across the edges of a single simplification path.

    Parameters
    ----------
    G
        Input graph.
    path_osmids
        OSM node IDs along the path, in order.
    edge_attr_aggs
        Aggregation function map (matches ``simplify_graph``'s arg).
    track_merged
        Whether to record the per-segment ``(u, v)`` pairs.

    Returns
    -------
    attrs
        Aggregated attribute dict.
    merged
        Per-segment ``(u, v)`` pairs (empty if ``track_merged`` is False).
    """
    path_attrs: dict[str, list[Any]] = {}
    merged: list[tuple[int, int]] = []
    for u, v in zip(path_osmids[:-1], path_osmids[1:], strict=True):
        if track_merged:
            merged.append((u, v))
        edge_count = G.number_of_edges(u, v)
        if edge_count != 1:
            msg = f"Found {edge_count} edges between {u} and {v} when simplifying"
            utils.log(msg, level=lg.WARNING)
        edge_data = next(iter(G.get_edge_data(u, v).values()))
        for key, val in edge_data.items():
            path_attrs.setdefault(key, []).append(val)

    out: dict[str, Any] = {}
    for key, values in path_attrs.items():
        if key in edge_attr_aggs:
            out[key] = edge_attr_aggs[key](values)
            continue
        # Use an order-preserving dedup that tolerates unhashable values
        # (list-valued attrs from prior simplification merges).
        uniq: list[Any] = []
        for v in values:
            if v not in uniq:
                uniq.append(v)
        out[key] = uniq[0] if len(uniq) == 1 else uniq
    return out, merged


def _simplify_graph_legacy(  # noqa: C901, PLR0912
    G: nx.MultiDiGraph,
    *,
    node_attrs_include: Iterable[str] | None = None,
    edge_attrs_differ: Iterable[str] | None = None,
    remove_rings: bool = True,
    track_merged: bool = False,
    edge_attr_aggs: dict[str, Any] | None = None,
) -> nx.MultiDiGraph:
    """
    Reference (legacy) implementation kept for equivalence testing only.

    Identical to the pre-Phase-A ``simplify_graph``. Do not call from
    production code paths; it exists solely as an oracle for the
    equivalence test suite during the Phase A rollout.

    Parameters
    ----------
    G
        Input graph.
    node_attrs_include
        Same semantics as ``simplify_graph``.
    edge_attrs_differ
        Same semantics as ``simplify_graph``.
    remove_rings
        Same semantics as ``simplify_graph``.
    track_merged
        Same semantics as ``simplify_graph``.
    edge_attr_aggs
        Same semantics as ``simplify_graph``.

    Returns
    -------
    Gs
        Topologically simplified graph.
    """
    if G.graph.get("simplified"):  # pragma: no cover
        msg = "This graph has already been simplified, cannot simplify it again."
        raise GraphSimplificationError(msg)

    if edge_attr_aggs is None:
        edge_attr_aggs = {"length": sum, "travel_time": sum}

    G = G.copy()
    initial_node_count = len(G)
    initial_edge_count = len(G.edges)
    all_nodes_to_remove: list[int] = []
    all_edges_to_add: list[dict[str, Any]] = []

    paths, endpoints = _get_paths_to_simplify(G, node_attrs_include, edge_attrs_differ)
    for path in paths:
        merged_edges: list[tuple[int, int]] = []
        path_attributes: dict[str, Any] = {}
        for u, v in zip(path[:-1], path[1:]):
            if track_merged:
                merged_edges.append((u, v))
            edge_count = G.number_of_edges(u, v)
            if edge_count != 1:
                msg = f"Found {edge_count} edges between {u} and {v} when simplifying"
                utils.log(msg, level=lg.WARNING)
            edge_data = next(iter(G.get_edge_data(u, v).values()))
            for attr in edge_data:
                if attr in path_attributes:
                    path_attributes[attr].append(edge_data[attr])
                else:
                    path_attributes[attr] = [edge_data[attr]]

        for attr_name, attr_values in path_attributes.items():
            if attr_name in edge_attr_aggs:
                path_attributes[attr_name] = edge_attr_aggs[attr_name](attr_values)
            elif len({type(x).__name__ + repr(x) for x in attr_values}) == 1:
                path_attributes[attr_name] = attr_values[0]
            else:
                # Use order-preserving dedup tolerating unhashable values.
                uniq: list[Any] = []
                for x in attr_values:
                    if x not in uniq:
                        uniq.append(x)
                path_attributes[attr_name] = uniq[0] if len(uniq) == 1 else uniq

        path_attributes["geometry"] = LineString(
            [Point((G.nodes[node]["x"], G.nodes[node]["y"])) for node in path],
        )

        if track_merged:
            path_attributes["merged_edges"] = merged_edges

        all_nodes_to_remove.extend(path[1:-1])
        all_edges_to_add.append(
            {"origin": path[0], "destination": path[-1], "attr_dict": path_attributes},
        )

    for edge in all_edges_to_add:
        G.add_edge(edge["origin"], edge["destination"], **edge["attr_dict"])

    G.remove_nodes_from(set(all_nodes_to_remove))

    if remove_rings:
        G = _remove_rings(G, endpoints)

    G.graph["simplified"] = True

    msg = (
        f"Simplified graph: {initial_node_count:,} to {len(G):,} nodes, "
        f"{initial_edge_count:,} to {len(G.edges):,} edges"
    )
    utils.log(msg, level=lg.INFO)
    return G


def simplify_graph(  # noqa: C901, PLR0912
    G: nx.MultiDiGraph,
    *,
    node_attrs_include: Iterable[str] | None = None,
    edge_attrs_differ: Iterable[str] | None = None,
    remove_rings: bool = True,
    track_merged: bool = False,
    edge_attr_aggs: dict[str, Any] | None = None,
) -> nx.MultiDiGraph:
    """
    Simplify a graph's topology by removing interstitial nodes.

    This algorithm is described in the journal article: Boeing, G. 2025.
    "Topological Graph Simplification Solutions to the Street Intersection
    Miscount Problem." Transactions in GIS, 29 (3), e70037.
    https://doi.org/10.1111/tgis.70037

    This simplifies the graph's topology by removing all nodes that are not
    intersections or dead-ends, by creating an edge directly between the end
    points that encapsulate them while retaining the full geometry of the
    original edges, saved as a new `geometry` attribute on the new edge.

    Note that only simplified edges receive a `geometry` attribute. Some of
    the resulting consolidated edges may comprise multiple OSM ways, and if
    so, their unique attribute values are stored as a list. Optionally, the
    simplified edges can receive a `merged_edges` attribute that contains a
    list of all the `(u, v)` node pairs that were merged together.

    Use the `node_attrs_include` or `edge_attrs_differ` parameters to relax
    simplification strictness. For example, `edge_attrs_differ=["osmid"]` will
    retain every node whose incident edges have different OSM IDs. This lets
    you keep nodes at elbow two-way intersections (but be aware that sometimes
    individual blocks have multiple OSM IDs within them too). You could also
    use this parameter to retain nodes where sidewalks or bike lanes begin/end
    in the middle of a block. Or for example, `node_attrs_include=["highway"]`
    will retain every node with a "highway" attribute (regardless of its
    value), even if it does not represent a street junction.

    Parameters
    ----------
    G
        Input graph.
    node_attrs_include
        Node attribute names for relaxing the strictness of endpoint
        determination. A node is always an endpoint if it possesses one or
        more of the attributes in `node_attrs_include`.
    edge_attrs_differ
        Edge attribute names for relaxing the strictness of endpoint
        determination. A node is always an endpoint if its incident edges have
        different values than each other for any attribute in
        `edge_attrs_differ`.
    remove_rings
        If True, remove any graph components that consist only of a single
        chordless cycle (i.e., an isolated self-contained ring).
    track_merged
        If True, add `merged_edges` attribute on simplified edges, containing
        a list of all the `(u, v)` node pairs that were merged together.
    edge_attr_aggs
        Allows user to aggregate edge segment attributes when simplifying an
        edge. Keys are edge attribute names and values are aggregation
        functions to apply to these attributes when they exist for a set of
        edges being merged. Edge attributes not in `edge_attr_aggs` will
        contain the unique values across the merged edge segments. If None,
        defaults to `{"length": sum, "travel_time": sum}`.

    Returns
    -------
    Gs
        Topologically simplified graph, with a new `geometry` attribute on
        each simplified edge.
    """
    if G.graph.get("simplified"):  # pragma: no cover
        msg = "This graph has already been simplified, cannot simplify it again."
        raise GraphSimplificationError(msg)

    msg = "Begin topologically simplifying the graph (vectorized)..."
    utils.log(msg, level=lg.INFO)

    if edge_attr_aggs is None:
        edge_attr_aggs = {"length": sum, "travel_time": sum}

    initial_node_count = len(G)
    initial_edge_count = len(G.edges)

    adj = _build_adjacency(G)
    endpoints = _identify_endpoints_vectorized(
        G, adj, node_attrs_include, edge_attrs_differ,
    )
    msg = f"Identified {len(endpoints):,} edge endpoints"
    utils.log(msg, level=lg.INFO)

    offsets, nodes_flat = _trace_paths(adj, endpoints)
    geometries = _build_path_geometries(offsets, nodes_flat, adj.xs, adj.ys)

    nodes_to_remove: list[int] = []
    edges_to_add: list[tuple[int, int, dict[str, Any]]] = []

    for i in range(offsets.size - 1):
        path_idxs = nodes_flat[offsets[i]: offsets[i + 1]]
        path_osmids = [int(adj.node_ids[idx]) for idx in path_idxs]
        attrs, merged = _aggregate_path_attrs(
            G, path_osmids, edge_attr_aggs, track_merged=track_merged,
        )
        attrs["geometry"] = geometries[i]
        if track_merged:
            attrs["merged_edges"] = merged

        nodes_to_remove.extend(path_osmids[1:-1])
        edges_to_add.append((path_osmids[0], path_osmids[-1], attrs))

    for u, v, data in edges_to_add:
        G.add_edge(u, v, **data)
    G.remove_nodes_from(set(nodes_to_remove))

    if remove_rings:
        G = _remove_rings(G, endpoints)

    G.graph["simplified"] = True
    msg = (
        f"Simplified graph: {initial_node_count:,} to {len(G):,} nodes, "
        f"{initial_edge_count:,} to {len(G.edges):,} edges"
    )
    utils.log(msg, level=lg.INFO)
    return G


def consolidate_intersections(
    G: nx.MultiDiGraph,
    *,
    tolerance: float | dict[int, float] = 10,
    rebuild_graph: bool = True,
    dead_ends: bool = False,
    reconnect_edges: bool = True,
    node_attr_aggs: dict[str, Any] | None = None,
) -> nx.MultiDiGraph | pa.Table:
    """
    Consolidate intersections comprising clusters of nearby nodes.

    This algorithm is described in the journal article: Boeing, G. 2025.
    "Topological Graph Simplification Solutions to the Street Intersection
    Miscount Problem." Transactions in GIS, 29 (3), e70037.
    https://doi.org/10.1111/tgis.70037

    Merges nearby nodes and returns either their centroids or a rebuilt graph
    with consolidated intersections and reconnected edge geometries. The
    `tolerance` argument can be a single value applied to all nodes or
    individual per-node values. It should be adjusted to approximately match
    street design standards in the specific street network, and you should use
    a projected graph to work in meaningful and consistent units like meters.
    Note: `tolerance` represents a per-node buffering radius. For example, to
    consolidate nodes within 10 meters of each other, use `tolerance=5`.

    When `rebuild_graph` is False, it uses a purely geometric (and relatively
    fast) algorithm to identify "geometrically close" nodes, merge them, and
    return the merged intersections' centroids. When `rebuild_graph` is True,
    it uses a topological (and slower but more accurate) algorithm to identify
    "topologically close" nodes, merge them, then rebuild/return the graph.
    Returned graph's node IDs represent clusters rather than "osmid" values.
    Refer to nodes' "osmid_original" attributes for original "osmid" values.
    If multiple nodes were merged together, the "osmid_original" attribute is
    a list of merged nodes' "osmid" values.

    Divided roads are often represented by separate centerline edges. The
    intersection of two divided roads thus creates 4 nodes, representing where
    each edge intersects a perpendicular edge. These 4 nodes represent a
    single intersection in the real world. A similar situation occurs with
    roundabouts and traffic circles. This function consolidates nearby nodes
    by buffering them to an arbitrary distance, merging overlapping buffers,
    and taking their centroid.

    Parameters
    ----------
    G
        A projected graph.
    tolerance
        Nodes are buffered to this distance (in graph's geometry's units) and
        subsequent overlaps are dissolved into a single node. If scalar, then
        that single value will be used for all nodes. If dict (mapping node
        IDs to individual values), then those values will be used per node and
        any missing node IDs will not be buffered.
    rebuild_graph
        If True, consolidate the nodes topologically, rebuild the graph, and
        return as MultiDiGraph. Otherwise, consolidate the nodes geometrically
        and return the consolidated node points as GeoSeries.
    dead_ends
        If False, discard dead-end nodes to return only street-intersection
        points.
    reconnect_edges
        If True, reconnect edges (and their geometries) to the consolidated
        nodes in rebuilt graph, and update the edge length attributes. If
        False, the returned graph has no edges (which is faster if you just
        need topologically consolidated intersection counts). Ignored if
        `rebuild_graph` is not True.
    node_attr_aggs
        Allows user to aggregate node attributes values when merging nodes.
        Keys are node attribute names and values are aggregation functions
        (anything accepted as an argument by `pandas.agg`). Node attributes
        not in `node_attr_aggs` will contain the unique values across the
        merged nodes. If None, defaults to `{"elevation": numpy.mean}`.

    Returns
    -------
    Gc or table
        If `rebuild_graph=True`, returns MultiDiGraph with consolidated
        intersections and (optionally) reconnected edge geometries. If
        `rebuild_graph=False`, returns a single-column ``pa.Table``
        (column ``geometry``, geoarrow.wkb) holding the centroid Points
        of the merged intersections.
    """
    # make a copy to not mutate original graph object caller passed in
    G = G.copy()

    # if dead_ends is False, discard dead-ends to retain only intersections
    if not dead_ends:
        spn = stats.streets_per_node(G)
        dead_end_nodes = [node for node, count in spn.items() if count <= 1]
        G.remove_nodes_from(dead_end_nodes)

    if rebuild_graph:
        if len(G.nodes) == 0 or len(G.edges) == 0:
            # cannot rebuild a graph with no nodes or no edges, just return it
            return G

        # otherwise
        return _consolidate_intersections_rebuild_graph(
            G,
            tolerance,
            reconnect_edges,
            node_attr_aggs,
        )

    # otherwise, if we're not rebuilding the graph
    crs = G.graph.get("crs")
    if len(G) == 0:
        # if graph has no nodes, just return empty geoarrow table
        return _empty_geoarrow_table(crs)

    # otherwise, return the centroids of the merged intersection polygons
    cluster_geoms = _merge_nodes_geometric(G, tolerance)
    centroids = shapely.centroid(cluster_geoms)
    return _points_to_arrow(centroids, crs)


def _wkb_geom_field(crs: str | None) -> pa.Field:
    """
    Build a `geometry` field carrying geoarrow.wkb extension metadata.

    Parameters
    ----------
    crs
        CRS authority code string (e.g. ``"EPSG:4326"``) or ``None``.

    Returns
    -------
    field
        Arrow field tagged with the geoarrow.wkb extension type.
    """
    if crs is None:
        crs_token = ""
    elif isinstance(crs, str):
        crs_token = crs.upper()
    else:
        # pyproj.CRS or similar — fall back to its string form
        for attr in ("to_string", "srs"):
            if hasattr(crs, attr):
                value = getattr(crs, attr)
                value = value() if callable(value) else value
                if isinstance(value, str) and value:
                    crs_token = value.upper()
                    break
        else:
            crs_token = str(crs).upper()
    metadata = {
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": (
            b'{"crs":"' + crs_token.encode("ascii") + b'","edges":"planar"}'
        ),
    }
    return pa.field("geometry", pa.binary(), nullable=True, metadata=metadata)


def _points_to_arrow(geoms: np.ndarray, crs: str | None) -> pa.Table:
    """
    Wrap a numpy array of shapely geometries in a single-column Arrow table.

    Parameters
    ----------
    geoms
        Shapely geometries.
    crs
        CRS to embed in the geometry field metadata.

    Returns
    -------
    table
        ``pa.Table`` with a single ``geometry`` column (geoarrow.wkb).
    """
    wkbs = shapely.to_wkb(geoms)
    arr = pa.array(wkbs, type=pa.binary())
    return pa.Table.from_arrays([arr], schema=pa.schema([_wkb_geom_field(crs)]))


def _empty_geoarrow_table(crs: str | None) -> pa.Table:
    """Return an empty geoarrow.wkb-typed table for empty-graph fallbacks."""
    return pa.Table.from_arrays(
        [pa.array([], type=pa.binary())],
        schema=pa.schema([_wkb_geom_field(crs)]),
    )


def _node_arrays(G: nx.MultiDiGraph) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Extract aligned ``(osmids, xs, ys)`` numpy arrays from a graph's nodes.

    Parameters
    ----------
    G
        Input graph.

    Returns
    -------
    osmids, xs, ys
        Parallel arrays in graph node order.
    """
    osmids = np.fromiter(G.nodes, dtype=np.int64, count=len(G.nodes))
    xs = np.fromiter((G.nodes[n]["x"] for n in osmids), dtype=np.float64,
                     count=len(osmids))
    ys = np.fromiter((G.nodes[n]["y"] for n in osmids), dtype=np.float64,
                     count=len(osmids))
    return osmids, xs, ys


def _merge_nodes_geometric(
    G: nx.MultiDiGraph,
    tolerance: float | dict[int, float],
) -> np.ndarray:
    """
    Geometrically merge nodes within some distance of each other.

    Buffers each node by ``tolerance`` (or the per-node value from a dict)
    and unions overlapping buffers into cluster polygons.

    Parameters
    ----------
    G
        A projected graph.
    tolerance
        Per-node buffering radius in graph CRS units; scalar or dict
        keyed by node ID.

    Returns
    -------
    cluster_geoms
        Numpy array of the merged cluster geometries.
    """
    osmids, xs, ys = _node_arrays(G)
    points = shapely.points(xs, ys)

    if isinstance(tolerance, dict):
        tols = np.array([tolerance.get(int(o), np.nan) for o in osmids],
                        dtype=np.float64)
        valid = ~np.isnan(tols)
        # buffer where a tolerance is provided, otherwise keep the bare point
        buffered = np.where(
            valid,
            shapely.buffer(points, np.where(valid, tols, 0.0)),
            points,
        )
    else:
        buffered = shapely.buffer(points, tolerance)

    merged = shapely.union_all(buffered)
    if merged is None or merged.is_empty:
        return np.empty(0, dtype=object)
    if hasattr(merged, "geoms"):
        return np.asarray(list(merged.geoms), dtype=object)
    return np.asarray([merged], dtype=object)


def _split_disconnected_clusters(
    cluster_df: pl.DataFrame,
    G: nx.MultiDiGraph,
) -> pl.DataFrame:
    """
    Split disconnected clusters into connected subclusters.

    If a cluster contains multiple weakly connected components, move each
    component to its own subcluster to avoid connecting nodes that are not
    truly connected (e.g., nearby dead-ends or surface streets with bridge).

    Parameters
    ----------
    gdf
        Node-to-cluster mapping GeoDataFrame.
    node_points
        Original graph's node points GeoDataFrame.
    G
        The original projected graph.

    Returns
    -------
    cluster_df
        Polars DataFrame (`osmid`, `x`, `y`, `cluster`) with disconnected
        clusters split into subclusters and unique integer cluster IDs.
    """
    rows = cluster_df.to_dicts()
    by_cluster: dict[Any, list[dict[str, Any]]] = {}
    for row in rows:
        by_cluster.setdefault(row["cluster"], []).append(row)

    new_label_seq = 0
    relabeled: list[dict[str, Any]] = []
    for nodes_subset in by_cluster.values():
        osmids = [row["osmid"] for row in nodes_subset]
        if len(osmids) > 1:
            wccs = list(nx.weakly_connected_components(G.subgraph(osmids)))
            if len(wccs) > 1:
                # multiple weakly-connected components in one cluster: split
                node_to_wcc = {n: idx for idx, wcc in enumerate(wccs) for n in wcc}
                # group rows by wcc, assign each its own new label + centroid
                wcc_groups: dict[int, list[dict[str, Any]]] = {}
                for row in nodes_subset:
                    wcc_groups.setdefault(node_to_wcc[row["osmid"]], []).append(row)
                for group in wcc_groups.values():
                    pts = shapely.points(
                        np.array([r["x"] for r in group], dtype=np.float64),
                        np.array([r["y"] for r in group], dtype=np.float64),
                    )
                    centroid = shapely.union_all(pts).centroid
                    for r in group:
                        r["x"] = float(centroid.x)
                        r["y"] = float(centroid.y)
                        r["cluster"] = new_label_seq
                    new_label_seq += 1
                    relabeled.extend(group)
                continue
        for row in nodes_subset:
            row["cluster"] = new_label_seq
        new_label_seq += 1
        relabeled.extend(nodes_subset)

    return pl.DataFrame(relabeled, schema=cluster_df.schema)


def _aggregate_cluster_attrs(
    osmids: list[int],
    G: nx.MultiDiGraph,
    cluster_xy: tuple[float, float],
    node_attr_aggs: dict[str, Any],
) -> dict[str, Any]:
    """
    Aggregate attribute values across the OSM nodes in a single cluster.

    Mirrors the legacy GeoDataFrame ``groupby + agg`` semantics without
    requiring pandas: collects per-attribute lists of non-null values from
    ``G.nodes``, then either applies the user's aggregation function,
    keeps the single unique value, or returns a list of unique values.

    Parameters
    ----------
    osmids
        OSM node IDs participating in this cluster.
    G
        The original projected graph.
    cluster_xy
        ``(x, y)`` coordinate to set on the merged node.
    node_attr_aggs
        Mapping from attribute name to aggregation function.

    Returns
    -------
    attrs
        Attribute dict for the new merged node.
    """
    x, y = cluster_xy
    attrs: dict[str, Any] = {"osmid_original": list(osmids), "x": x, "y": y}

    # collect per-attr non-null values across all merged nodes
    per_attr: dict[str, list[Any]] = {}
    for osmid in osmids:
        for key, val in G.nodes[osmid].items():
            if val is None:
                continue
            per_attr.setdefault(key, []).append(val)

    for key, values in per_attr.items():
        if key in {"x", "y", "osmid_original"}:
            continue
        if key in node_attr_aggs:
            agg = node_attr_aggs[key]
            try:
                attrs[key] = agg(values) if callable(agg) else _named_agg(agg, values)
            except (TypeError, ValueError):
                attrs[key] = values
            continue
        if key == "street_count":
            # recomputed downstream from the consolidated graph
            continue
        unique_vals = list(dict.fromkeys(values))
        if len(unique_vals) == 1:
            attrs[key] = unique_vals[0]
        elif len(unique_vals) > 1:
            attrs[key] = unique_vals
    return attrs


def _named_agg(name: str, values: list[Any]) -> Any:  # noqa: ANN401
    """
    Apply a string-named aggregation (``"mean"``, ``"sum"``, ...) to values.

    Parameters
    ----------
    name
        Aggregation name as accepted by ``polars.Series.<agg>``.
    values
        Collected non-null values.

    Returns
    -------
    result
        The aggregated value.
    """
    series = pl.Series(values=values)
    method = getattr(series, name)
    return method()


def _build_consolidated_nodes(
    Gc: nx.MultiDiGraph,
    cluster_df: pl.DataFrame,
    G: nx.MultiDiGraph,
    node_attr_aggs: dict[str, Any],
) -> None:
    """
    Create a new node in ``Gc`` for each cluster in ``cluster_df``.

    Parameters
    ----------
    Gc
        The new consolidated graph to populate.
    cluster_df
        Polars DataFrame with columns ``osmid``, ``x``, ``y``, ``cluster``.
    G
        The original projected graph.
    node_attr_aggs
        Node attribute aggregation functions.
    """
    rows = cluster_df.to_dicts()
    by_cluster: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        by_cluster.setdefault(row["cluster"], []).append(row)

    for cluster_label, nodes_subset in by_cluster.items():
        osmids = [row["osmid"] for row in nodes_subset]
        if len(osmids) == 1:
            Gc.add_node(cluster_label, osmid_original=osmids[0], **G.nodes[osmids[0]])
        else:
            xy = (nodes_subset[0]["x"], nodes_subset[0]["y"])
            attrs = _aggregate_cluster_attrs(osmids, G, xy, node_attr_aggs)
            Gc.add_node(cluster_label, **attrs)


def _reconnect_edges_to_clusters(
    Gc: nx.MultiDiGraph,
    cluster_df: pl.DataFrame,
    G: nx.MultiDiGraph,
) -> None:
    """
    Create edges between clusters and extend geometries to new node points.

    Parameters
    ----------
    Gc
        The new consolidated graph to add edges to.
    cluster_df
        Polars DataFrame with columns ``osmid``, ``cluster``.
    G
        The original projected graph.
    """
    # osmid → cluster id lookup
    osmid_to_cluster = dict(zip(
        cluster_df.get_column("osmid").to_list(),
        cluster_df.get_column("cluster").to_list(),
        strict=True,
    ))

    # create cluster-to-cluster edge for each original edge
    for u, v, _k, data in G.edges(keys=True, data=True):
        u2 = osmid_to_cluster[u]
        v2 = osmid_to_cluster[v]
        if (u2 != v2) or (u == v):
            data["u_original"] = u
            data["v_original"] = v
            if "geometry" not in data:
                data["geometry"] = LineString((
                    (G.nodes[u]["x"], G.nodes[u]["y"]),
                    (G.nodes[v]["x"], G.nodes[v]["y"]),
                ))
            Gc.add_edge(u2, v2, **data)

    # extend incident edge geometries for clusters that swallowed >1 node
    sizes = (
        cluster_df.group_by("cluster").len().rename({"len": "n"}).filter(pl.col("n") > 1)
    )
    for cluster_label in sizes.get_column("cluster").to_list():
        x = Gc.nodes[cluster_label]["x"]
        y = Gc.nodes[cluster_label]["y"]
        xy = [(x, y)]
        in_edges = set(Gc.in_edges(cluster_label, keys=True))
        out_edges = set(Gc.out_edges(cluster_label, keys=True))
        for u, v, k in in_edges | out_edges:
            old_coords = list(Gc.edges[u, v, k]["geometry"].coords)
            new_coords = xy + old_coords if cluster_label == u else old_coords + xy
            new_geom = LineString(new_coords)
            Gc.edges[u, v, k]["geometry"] = new_geom
            Gc.edges[u, v, k]["length"] = new_geom.length


def _consolidate_intersections_rebuild_graph(
    G: nx.MultiDiGraph,
    tolerance: float | dict[int, float],
    reconnect_edges: bool,  # noqa: FBT001
    node_attr_aggs: dict[str, Any] | None,
) -> nx.MultiDiGraph:
    """
    Consolidate intersections comprising clusters of nearby nodes.

    Merge nodes and return a rebuilt graph with consolidated intersections and
    reconnected edge geometries.

    Parameters
    ----------
    G
        A projected graph.
    tolerance
        Nodes are buffered to this distance (in graph's geometry's units) and
        subsequent overlaps are dissolved into a single node. If scalar, then
        that single value will be used for all nodes. If dict (mapping node
        IDs to individual values), then those values will be used per node and
        any missing node IDs will not be buffered.
    reconnect_edges
        If True, reconnect edges (and their geometries) to the consolidated
        nodes in rebuilt graph, and update the edge length attributes. If
        False, the returned graph has no edges (which is faster if you just
        need topologically consolidated intersection counts).
    node_attr_aggs
        Allows user to aggregate node attributes values when merging nodes.
        Keys are node attribute names and values are aggregation functions
        (anything accepted as an argument by `pandas.agg`). Node attributes
        not in `node_attr_aggs` will contain the unique values across the
        merged nodes. If None, defaults to `{"elevation": "mean"}`.

    Returns
    -------
    Gc
        A rebuilt graph with consolidated intersections and (optionally)
        reconnected edge geometries.
    """
    if G.graph.get("consolidated"):  # pragma: no cover
        msg = "This graph has already been consolidated, cannot consolidate it again."
        raise GraphSimplificationError(msg)

    if node_attr_aggs is None:
        node_attr_aggs = {"elevation": "mean"}

    # STEP 1: buffer nodes, dissolve overlaps → cluster polygons
    cluster_geoms = _merge_nodes_geometric(G, tolerance)
    cluster_centroids = shapely.centroid(cluster_geoms)
    cluster_xs = shapely.get_x(cluster_centroids)
    cluster_ys = shapely.get_y(cluster_centroids)

    # STEP 2: assign each original node to the cluster polygon containing it
    osmids, node_xs, node_ys = _node_arrays(G)
    node_points = shapely.points(node_xs, node_ys)
    tree = STRtree(cluster_geoms)
    # query returns 2 x N array: [tree_idx, geom_idx]; nearest=False means
    # we get all overlap pairs.
    pairs = tree.query(node_points, predicate="within")
    # pairs[0] = input (node) indices, pairs[1] = tree (cluster) indices
    cluster_assignment = np.full(len(osmids), -1, dtype=np.int64)
    cluster_assignment[pairs[0]] = pairs[1]
    if (cluster_assignment < 0).any():
        # handle nodes that fell on cluster boundaries (rare): nearest-cluster fallback
        for missing_idx in np.where(cluster_assignment < 0)[0]:
            nearest = tree.nearest(node_points[missing_idx])
            cluster_assignment[missing_idx] = int(nearest)

    cluster_df = pl.DataFrame({
        "osmid": osmids,
        "x": cluster_xs[cluster_assignment],
        "y": cluster_ys[cluster_assignment],
        "cluster": cluster_assignment,
    })

    # STEP 3: split disconnected clusters into connected subclusters
    cluster_df = _split_disconnected_clusters(cluster_df, G)

    # STEP 4: new empty graph carrying over G.graph
    Gc = nx.MultiDiGraph()
    Gc.graph = G.graph

    # STEP 5: one new node per cluster, with attribute aggregation
    _build_consolidated_nodes(Gc, cluster_df, G, node_attr_aggs)

    G.graph["consolidated"] = True

    if len(G.edges) == 0 or not reconnect_edges:
        return Gc

    # STEPS 6+7: cluster-to-cluster edges + geometry extension
    _reconnect_edges_to_clusters(Gc, cluster_df, G)

    # recompute street_count for nodes that lack it
    null_nodes = [n for n, sc in Gc.nodes(data="street_count") if sc is None]
    street_counts = stats.count_streets_per_node(Gc, nodes=null_nodes)
    nx.set_node_attributes(Gc, street_counts, name="street_count")

    return Gc
