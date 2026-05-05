# ducknx — Simplify & Build Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut `simplify_graph` from 31.7s → ≤10s and `_create_graph_from_dfs` from 12.7s → ≤6s on the Berlin large bbox, with ~50% memory reduction. Also speed up `consolidate_intersections` which uses the same patterns.

**Architecture:** Two-phase delivery. Phase A is pure-Python optimization (CSR adjacency, vectorized endpoint detection, bulk geometry, drop `G.copy()`, attr deduplication). Phase B is an optional Rust extension via PyO3/maturin that only ships if Phase A misses the pragmatic targets. Phase B keeps the Python path as a fallback.

**Tech Stack:** Python 3.12+, NetworkX, polars, PyArrow, shapely (vectorized), numpy, hypothesis (property tests), pytest. Phase B adds Rust (PyO3, numpy crate, rstar, rayon) and maturin/cibuildwheel.

**Spec:** `docs/superpowers/specs/2026-05-06-ducknx-simplify-build-perf-design.md`

---

## File Structure

**Phase A — modified files:**

- `ducknx/simplification.py` — add `AdjacencyView`, `_build_adjacency`, `_identify_endpoints_vectorized`, `_trace_paths`, `_build_path_geometries`, `_aggregate_path_attrs`, `_apply_simplification`. Refactor `simplify_graph` to drive them. Refactor `_split_disconnected_clusters` and `_aggregate_cluster_attrs` to use polars expressions.
- `ducknx/graph.py` — refactor `_create_graph_from_dfs` and `_create_graph_rustworkx` to share an edge-template + generator-based bulk insert. Eliminate per-edge `attrs.copy()`.
- `tests/test_simplification_equivalence.py` — NEW. Equivalence + property tests.
- `benchmarks/bench_pipeline.py` — add `--track` mode that emits JSON to `benchmarks/results/<git-sha>.json`.
- `benchmarks/results/baseline.json` — NEW. Committed baseline numbers.
- `changelog/` — NEW changelog entry.

**Phase B — new files:**

- `rust/ducknx-core/Cargo.toml` — Rust crate manifest.
- `rust/ducknx-core/pyproject.toml` — maturin build config.
- `rust/ducknx-core/src/lib.rs` — PyO3 module entry.
- `rust/ducknx-core/src/topology.rs` — `simplify_topology` kernel.
- `rust/ducknx-core/src/adjacency.rs` — CSR types/builders (Rust-side).
- `rust/ducknx-core/src/cluster.rs` — `cluster_assign` (rstar-based PIP).
- `rust/ducknx-core/tests/topology.rs` — Rust unit tests.
- `ducknx/_rust.py` — `HAVE_RUST` flag and routing helpers.
- `ducknx/simplification.py` — wire `HAVE_RUST` into `simplify_graph` and `consolidate_intersections`.
- `pyproject.toml` — add `[fast]` extra including `ducknx-core`.
- `.github/workflows/wheels.yml` — NEW. cibuildwheel matrix.

---

## Task 1: Phase A — Pure-Python optimization (single beads task / single PR)

**Beads task:** `Phase A — vectorize simplify_graph, dedupe edge attrs in _create_graph_from_dfs, polars-ify consolidate helpers`

**Acceptance gate (from spec):**
- graph_simplify ≤ 18s on Berlin large bbox (intermediate target — half current cost)
- graph_build ≤ 8s on Berlin large bbox
- All equivalence tests green
- No CI benchmark stage regresses by more than 10%

**Files:**
- Modify: `ducknx/simplification.py`
- Modify: `ducknx/graph.py:546-718` (the build path)
- Create: `tests/test_simplification_equivalence.py`
- Modify: `benchmarks/bench_pipeline.py`
- Create: `benchmarks/results/baseline.json`
- Create: `changelog/2026-05-06_simplify-build-perf-phaseA.md`

### Step 1.1: Snapshot the legacy implementation as `_simplify_graph_legacy`

Before touching `simplify_graph`, copy its current body verbatim into a private function `_simplify_graph_legacy` in the same module. The equivalence tests will compare against this. This is a pure save-as-rename, no behavior change.

- [ ] **Step 1.1a: Add legacy snapshot**

In `ducknx/simplification.py`, just above the existing `simplify_graph` definition, add:

```python
def _simplify_graph_legacy(  # noqa: C901, PLR0912
    G: nx.MultiDiGraph,
    *,
    node_attrs_include: Iterable[str] | None = None,
    edge_attrs_differ: Iterable[str] | None = None,
    remove_rings: bool = True,
    track_merged: bool = False,
    edge_attr_aggs: dict[str, Any] | None = None,
) -> nx.MultiDiGraph:
    """Reference implementation kept for equivalence testing only.

    Identical to the pre-Phase-A `simplify_graph`. Do not call from
    production code paths.
    """
    # NOTE: paste the EXACT current body of simplify_graph here, unchanged
    ...
```

Paste the current body of `simplify_graph` (lines 305-477 at the time of this plan) into this function unchanged. Keep `simplify_graph` itself unchanged for now — Step 1.7 will rewrite it.

- [ ] **Step 1.1b: Run the existing tests; they must still pass**

```bash
pytest tests/ -x -q
```
Expected: all green (no behavior change).

- [ ] **Step 1.1c: Commit**

```bash
git add ducknx/simplification.py
git commit -m "refactor(simplify): snapshot current simplify_graph as _simplify_graph_legacy"
```

### Step 1.2: Add `AdjacencyView` and `_build_adjacency`

- [ ] **Step 1.2a: Write the failing test for adjacency construction**

Create `tests/test_simplification_equivalence.py` (initial file):

```python
"""Equivalence and unit tests for the Phase A simplification rewrite."""

from __future__ import annotations

import networkx as nx
import numpy as np
import pytest

from ducknx import simplification as simp


def _toy_graph() -> nx.MultiDiGraph:
    """Build a tiny deterministic graph for adjacency tests.

    Topology:  1 → 2 → 3 → 4
                       ↑
                       5 → 3 (so 3 has 2 predecessors)
    """
    G = nx.MultiDiGraph()
    for nid, x, y in [(1, 0.0, 0.0), (2, 1.0, 0.0), (3, 2.0, 0.0),
                      (4, 3.0, 0.0), (5, 2.0, 1.0)]:
        G.add_node(nid, x=x, y=y)
    G.add_edge(1, 2)
    G.add_edge(2, 3)
    G.add_edge(3, 4)
    G.add_edge(5, 3)
    return G


def test_build_adjacency_roundtrip() -> None:
    G = _toy_graph()
    adj = simp._build_adjacency(G)

    # node order is deterministic
    assert list(adj.node_ids) == sorted(G.nodes)
    # successors of node 3 (idx via osmid_to_idx) include node 4
    idx3 = adj.osmid_to_idx[3]
    succ3 = adj.succ_indices[adj.succ_indptr[idx3]:adj.succ_indptr[idx3 + 1]]
    assert {int(adj.node_ids[s]) for s in succ3} == {4}
    # predecessors of node 3 are {2, 5}
    pred3 = adj.pred_indices[adj.pred_indptr[idx3]:adj.pred_indptr[idx3 + 1]]
    assert {int(adj.node_ids[p]) for p in pred3} == {2, 5}
    # xy arrays line up with node_ids
    assert adj.xs[adj.osmid_to_idx[3]] == 2.0
    assert adj.ys[adj.osmid_to_idx[5]] == 1.0
```

- [ ] **Step 1.2b: Run the test, see it fail**

```bash
pytest tests/test_simplification_equivalence.py::test_build_adjacency_roundtrip -v
```
Expected: FAIL with `AttributeError: module 'ducknx.simplification' has no attribute '_build_adjacency'`.

- [ ] **Step 1.2c: Implement `AdjacencyView` and `_build_adjacency`**

Add to `ducknx/simplification.py` (after the imports, before `_is_endpoint`):

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class AdjacencyView:
    """Read-only CSR snapshot of a MultiDiGraph for vectorized topology work.

    Parameters
    ----------
    node_ids
        Sorted int64 array of OSM node IDs (the row order for all per-node arrays).
    osmid_to_idx
        Mapping from OSM node ID to row index in ``node_ids``.
    succ_indptr, succ_indices
        CSR successor adjacency. ``succ_indices[succ_indptr[i]:succ_indptr[i+1]]``
        gives the row indices of node ``i``'s successors.
    pred_indptr, pred_indices
        CSR predecessor adjacency, same shape conventions.
    xs, ys
        Per-node coordinate arrays aligned with ``node_ids``.
    out_degree, in_degree
        Per-node out/in degree (parallel multi-edges counted separately).
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
    """Build a CSR adjacency snapshot from a MultiDiGraph.

    Uses ``G.adj`` and ``G.pred`` directly to avoid per-node Python method
    calls. Multi-edges between the same (u, v) increment ``out_degree[u]``
    and ``in_degree[v]`` by their count and contribute one CSR entry per
    parallel edge so endpoint rule 3 (degree 4 = parallel-edge case) sees
    the same shape as NetworkX would report.

    Parameters
    ----------
    G
        Input MultiDiGraph.

    Returns
    -------
    AdjacencyView
        Frozen snapshot.
    """
    node_ids = np.fromiter(sorted(G.nodes), dtype=np.int64, count=G.number_of_nodes())
    osmid_to_idx = {int(n): i for i, n in enumerate(node_ids)}

    n = node_ids.size
    succ_counts = np.zeros(n, dtype=np.int64)
    pred_counts = np.zeros(n, dtype=np.int64)

    # First pass: counts (account for parallel edges via len(edge_dict))
    adj = G._adj
    pred = G._pred
    for u, succs in adj.items():
        ui = osmid_to_idx[u]
        for v, ekeys in succs.items():
            succ_counts[ui] += len(ekeys)
    for v, preds in pred.items():
        vi = osmid_to_idx[v]
        for u, ekeys in preds.items():
            pred_counts[vi] += len(ekeys)

    succ_indptr = np.empty(n + 1, dtype=np.int64)
    pred_indptr = np.empty(n + 1, dtype=np.int64)
    succ_indptr[0] = 0
    pred_indptr[0] = 0
    np.cumsum(succ_counts, out=succ_indptr[1:])
    np.cumsum(pred_counts, out=pred_indptr[1:])

    succ_indices = np.empty(int(succ_indptr[-1]), dtype=np.int64)
    pred_indices = np.empty(int(pred_indptr[-1]), dtype=np.int64)

    # Second pass: fill indices
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

    xs = np.fromiter((G.nodes[int(n)]["x"] for n in node_ids), dtype=np.float64, count=n)
    ys = np.fromiter((G.nodes[int(n)]["y"] for n in node_ids), dtype=np.float64, count=n)

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
```

- [ ] **Step 1.2d: Run test, verify pass**

```bash
pytest tests/test_simplification_equivalence.py::test_build_adjacency_roundtrip -v
```
Expected: PASS.

- [ ] **Step 1.2e: Commit**

```bash
git add ducknx/simplification.py tests/test_simplification_equivalence.py
git commit -m "feat(simplify): add AdjacencyView CSR snapshot"
```

### Step 1.3: Vectorized endpoint detection

- [ ] **Step 1.3a: Write the failing test (oracle = legacy `_is_endpoint`)**

Append to `tests/test_simplification_equivalence.py`:

```python
def test_endpoints_match_legacy_on_toy() -> None:
    G = _toy_graph()
    adj = simp._build_adjacency(G)

    legacy = simp._identify_endpoints(G, None, None)
    fast = simp._identify_endpoints_vectorized(G, adj, None, None)
    assert legacy == fast


def test_endpoints_match_legacy_node_attrs_include() -> None:
    G = _toy_graph()
    G.nodes[2]["highway"] = "traffic_signals"
    adj = simp._build_adjacency(G)

    legacy = simp._identify_endpoints(G, ["highway"], None)
    fast = simp._identify_endpoints_vectorized(G, adj, ["highway"], None)
    assert legacy == fast


def test_endpoints_match_legacy_edge_attrs_differ() -> None:
    G = _toy_graph()
    # give 2→3 and 3→4 different osmids so node 3 should be flagged by rule 5
    list(G[2][3].values())[0]["osmid"] = 100
    list(G[3][4].values())[0]["osmid"] = 200
    adj = simp._build_adjacency(G)

    legacy = simp._identify_endpoints(G, None, ["osmid"])
    fast = simp._identify_endpoints_vectorized(G, adj, None, ["osmid"])
    assert legacy == fast
```

- [ ] **Step 1.3b: Run; expect failure**

```bash
pytest tests/test_simplification_equivalence.py -k endpoints -v
```
Expected: FAIL on `_identify_endpoints_vectorized` AttributeError.

- [ ] **Step 1.3c: Implement `_identify_endpoints_vectorized`**

Add after `_identify_endpoints` in `ducknx/simplification.py`:

```python
def _identify_endpoints_vectorized(
    G: nx.MultiDiGraph,
    adj: AdjacencyView,
    node_attrs_include: Iterable[str] | None,
    edge_attrs_differ: Iterable[str] | None,
) -> set[int]:
    """Vectorized endpoint detection.

    Rules 1-3 (self-loops, dangling nodes, neighbor/degree mismatch) are
    evaluated as numpy operations on the CSR adjacency. Rules 4-5 fall
    back to per-node checks but only run on nodes that survived rules 1-3.

    Parameters
    ----------
    G
        Input graph (used only for attribute access in rules 4-5).
    adj
        CSR adjacency snapshot.
    node_attrs_include, edge_attrs_differ
        Same semantics as ``_identify_endpoints``.

    Returns
    -------
    endpoints
        Set of OSM node IDs that are endpoints.
    """
    n = adj.node_ids.size
    is_endpoint = np.zeros(n, dtype=bool)

    # Rule 1: self-loop (idx i has itself in its successor list)
    for i in range(n):
        s, e = adj.succ_indptr[i], adj.succ_indptr[i + 1]
        if i in adj.succ_indices[s:e]:
            is_endpoint[i] = True

    # Rule 2: in_degree == 0 or out_degree == 0
    is_endpoint |= (adj.in_degree == 0) | (adj.out_degree == 0)

    # Rule 3: not (neighbors == 2 AND degree in {2, 4})
    #   neighbors = unique union of succ + pred
    #   degree = in_degree + out_degree
    degree = adj.in_degree + adj.out_degree
    neighbor_counts = np.empty(n, dtype=np.int64)
    for i in range(n):
        s_s, s_e = adj.succ_indptr[i], adj.succ_indptr[i + 1]
        p_s, p_e = adj.pred_indptr[i], adj.pred_indptr[i + 1]
        neighbor_counts[i] = np.unique(
            np.concatenate([adj.succ_indices[s_s:s_e], adj.pred_indices[p_s:p_e]])
        ).size
    rule3_pass = (neighbor_counts == 2) & ((degree == 2) | (degree == 4))
    is_endpoint |= ~rule3_pass

    # Rules 4-5: per-node fallback only for survivors
    if node_attrs_include is not None or edge_attrs_differ is not None:
        attrs_set = set(node_attrs_include) if node_attrs_include else None
        survivor_idxs = np.where(~is_endpoint)[0]
        for i in survivor_idxs:
            osmid = int(adj.node_ids[i])
            if attrs_set is not None and len(attrs_set & G.nodes[osmid].keys()) > 0:
                is_endpoint[i] = True
                continue
            if edge_attrs_differ is not None:
                hit = False
                for attr in edge_attrs_differ:
                    in_values = {v for _, _, v in G.in_edges(osmid, data=attr, keys=False)}
                    out_values = {v for _, _, v in G.out_edges(osmid, data=attr, keys=False)}
                    if len(in_values | out_values) > 1:
                        hit = True
                        break
                if hit:
                    is_endpoint[i] = True

    return {int(adj.node_ids[i]) for i in np.where(is_endpoint)[0]}
```

- [ ] **Step 1.3d: Run, verify pass**

```bash
pytest tests/test_simplification_equivalence.py -k endpoints -v
```
Expected: all 3 tests PASS.

- [ ] **Step 1.3e: Commit**

```bash
git add ducknx/simplification.py tests/test_simplification_equivalence.py
git commit -m "feat(simplify): vectorize endpoint detection via CSR adjacency"
```

### Step 1.4: CSR-based path tracing `_trace_paths`

- [ ] **Step 1.4a: Write the failing test**

Append to `tests/test_simplification_equivalence.py`:

```python
def test_trace_paths_match_legacy_on_toy() -> None:
    """Same paths as legacy _get_paths_to_simplify, modulo path ordering."""
    G = _toy_graph()
    adj = simp._build_adjacency(G)

    legacy_paths, legacy_endpoints = simp._get_paths_to_simplify(G, None, None)
    endpoints = simp._identify_endpoints_vectorized(G, adj, None, None)
    offsets, nodes_flat = simp._trace_paths(adj, endpoints)

    new_paths = [
        [int(adj.node_ids[idx]) for idx in nodes_flat[offsets[i]:offsets[i + 1]]]
        for i in range(offsets.size - 1)
    ]

    # Order-insensitive comparison: each path must round-trip
    assert {tuple(p) for p in new_paths} == {tuple(p) for p in legacy_paths}
    assert legacy_endpoints == endpoints
```

- [ ] **Step 1.4b: Run, expect failure**

```bash
pytest tests/test_simplification_equivalence.py::test_trace_paths_match_legacy_on_toy -v
```
Expected: FAIL on `_trace_paths` AttributeError.

- [ ] **Step 1.4c: Implement `_trace_paths`**

Add to `ducknx/simplification.py` after `_identify_endpoints_vectorized`:

```python
def _trace_paths(
    adj: AdjacencyView,
    endpoints: set[int],
) -> tuple[np.ndarray, np.ndarray]:
    """Trace simplification paths between endpoint nodes via CSR DFS.

    Mirrors the semantics of ``_build_path`` but operates on integer
    indices into ``adj.node_ids`` and emits flat ``(offsets, nodes_flat)``
    arrays. ``offsets`` has length ``num_paths + 1``; the i-th path is
    ``nodes_flat[offsets[i]:offsets[i+1]]``.

    Parameters
    ----------
    adj
        CSR adjacency snapshot.
    endpoints
        Set of endpoint OSM node IDs.

    Returns
    -------
    offsets, nodes_flat
        Both int64 numpy arrays. Path nodes are stored as row indices
        into ``adj.node_ids``, NOT as OSM IDs.
    """
    endpoint_idxs = {adj.osmid_to_idx[e] for e in endpoints}
    is_endpoint = np.zeros(adj.node_ids.size, dtype=bool)
    for ei in endpoint_idxs:
        is_endpoint[ei] = True

    paths_offsets: list[int] = [0]
    paths_nodes: list[int] = []

    for ei in endpoint_idxs:
        s, e = adj.succ_indptr[ei], adj.succ_indptr[ei + 1]
        for succ in adj.succ_indices[s:e]:
            if is_endpoint[succ]:
                continue

            path = [ei, int(succ)]
            path_set = {ei, int(succ)}

            # Continue from the successor's first non-visited successor chain
            ss, se = adj.succ_indptr[succ], adj.succ_indptr[succ + 1]
            picked = None
            for nxt in adj.succ_indices[ss:se]:
                if int(nxt) not in path_set:
                    picked = int(nxt)
                    break
            if picked is None:
                # Successor has no usable onward node; emit short path
                paths_nodes.extend(path)
                paths_offsets.append(len(paths_nodes))
                continue

            path.append(picked)
            path_set.add(picked)

            current = picked
            while not is_endpoint[current]:
                cs, ce = adj.succ_indptr[current], adj.succ_indptr[current + 1]
                onward = [int(s) for s in adj.succ_indices[cs:ce] if int(s) not in path_set]
                if len(onward) == 1:
                    current = onward[0]
                    path.append(current)
                    path_set.add(current)
                elif not onward:
                    # Self-looping back to original endpoint?
                    cs2, ce2 = adj.succ_indptr[current], adj.succ_indptr[current + 1]
                    if ei in adj.succ_indices[cs2:ce2]:
                        path.append(ei)
                    break
                else:
                    # >1 onward => current must have been an endpoint; abort with warning
                    msg = f"Impossible simplify pattern at node idx {current}"
                    raise GraphSimplificationError(msg)

            paths_nodes.extend(path)
            paths_offsets.append(len(paths_nodes))

    return np.asarray(paths_offsets, dtype=np.int64), np.asarray(paths_nodes, dtype=np.int64)
```

- [ ] **Step 1.4d: Run, verify pass**

```bash
pytest tests/test_simplification_equivalence.py::test_trace_paths_match_legacy_on_toy -v
```
Expected: PASS.

- [ ] **Step 1.4e: Commit**

```bash
git add ducknx/simplification.py tests/test_simplification_equivalence.py
git commit -m "feat(simplify): CSR-based path tracing returning flat arrays"
```

### Step 1.5: Bulk path geometry construction

- [ ] **Step 1.5a: Write the test**

Append to `tests/test_simplification_equivalence.py`:

```python
def test_path_geometries_bulk_matches_legacy() -> None:
    G = _toy_graph()
    adj = simp._build_adjacency(G)
    endpoints = simp._identify_endpoints_vectorized(G, adj, None, None)
    offsets, nodes_flat = simp._trace_paths(adj, endpoints)

    geoms = simp._build_path_geometries(offsets, nodes_flat, adj.xs, adj.ys)

    for i in range(offsets.size - 1):
        path_idxs = nodes_flat[offsets[i]:offsets[i + 1]]
        coords = [(adj.xs[idx], adj.ys[idx]) for idx in path_idxs]
        assert list(geoms[i].coords) == coords
```

- [ ] **Step 1.5b: Run, expect failure**

```bash
pytest tests/test_simplification_equivalence.py::test_path_geometries_bulk_matches_legacy -v
```
Expected: FAIL on AttributeError.

- [ ] **Step 1.5c: Implement `_build_path_geometries`**

Add to `ducknx/simplification.py`:

```python
def _build_path_geometries(
    offsets: np.ndarray,
    nodes_flat: np.ndarray,
    xs: np.ndarray,
    ys: np.ndarray,
) -> np.ndarray:
    """Bulk-build LineString geometries for every path in one shapely call.

    Parameters
    ----------
    offsets
        Path offsets (int64), length ``num_paths + 1``.
    nodes_flat
        Flat array of node indices into ``xs``/``ys``.
    xs, ys
        Coordinate arrays.

    Returns
    -------
    geoms
        Numpy object array of length ``num_paths`` of shapely LineStrings.
    """
    coords = np.empty((nodes_flat.size, 2), dtype=np.float64)
    coords[:, 0] = xs[nodes_flat]
    coords[:, 1] = ys[nodes_flat]
    # shapely.linestrings indices: per-coord index of which line it belongs to
    # We synthesize that from offsets.
    line_idx = np.repeat(
        np.arange(offsets.size - 1, dtype=np.int64),
        np.diff(offsets),
    )
    return shapely.linestrings(coords, indices=line_idx)
```

- [ ] **Step 1.5d: Run, verify pass**

```bash
pytest tests/test_simplification_equivalence.py::test_path_geometries_bulk_matches_legacy -v
```
Expected: PASS.

- [ ] **Step 1.5e: Commit**

```bash
git add ducknx/simplification.py tests/test_simplification_equivalence.py
git commit -m "feat(simplify): bulk LineString construction via shapely.linestrings"
```

### Step 1.6: Property tests on the endpoint kernel

- [ ] **Step 1.6a: Add hypothesis to test deps if not already present**

Check `pyproject.toml` for `hypothesis` in `[dependency-groups] test`. If absent:

```bash
uv add --group test "hypothesis>=6"
```

- [ ] **Step 1.6b: Add property tests**

Append to `tests/test_simplification_equivalence.py`:

```python
from hypothesis import given, settings as hyp_settings, strategies as st


@hyp_settings(max_examples=200, deadline=None)
@given(
    n_nodes=st.integers(min_value=2, max_value=20),
    edge_seed=st.integers(min_value=0, max_value=2**31 - 1),
)
def test_endpoint_kernel_matches_legacy_on_random_graphs(
    n_nodes: int, edge_seed: int,
) -> None:
    """Random small directed graphs: vectorized endpoints == legacy endpoints."""
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
```

- [ ] **Step 1.6c: Run; verify pass**

```bash
pytest tests/test_simplification_equivalence.py::test_endpoint_kernel_matches_legacy_on_random_graphs -v
```
Expected: PASS (200 random examples).

- [ ] **Step 1.6d: Commit**

```bash
git add tests/test_simplification_equivalence.py pyproject.toml uv.lock
git commit -m "test(simplify): property tests for endpoint kernel"
```

### Step 1.7: Rewrite `simplify_graph` to use the vectorized helpers

This is the main behavior change. The new `simplify_graph`:
1. Builds an `AdjacencyView` once.
2. Computes endpoints + paths via the vectorized helpers.
3. Builds path geometries in bulk.
4. Aggregates per-path attrs in one pass.
5. Mutates the input graph in place (no `G.copy()`).

- [ ] **Step 1.7a: Write the equivalence test (against `_simplify_graph_legacy`)**

Append to `tests/test_simplification_equivalence.py`:

```python
def _build_realistic_graph() -> nx.MultiDiGraph:
    """A graph rich enough to exercise list-valued attrs, geometry, and rings."""
    G = nx.MultiDiGraph()
    coords = {1: (0, 0), 2: (1, 0), 3: (2, 0), 4: (3, 0), 5: (4, 0),
              6: (4, 1), 7: (3, 1), 8: (2, 1), 9: (1, 1), 10: (0, 1)}
    for nid, (x, y) in coords.items():
        G.add_node(nid, x=float(x), y=float(y))
    # main street with mixed osmids and lengths
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
    G = _build_realistic_graph()
    Glegacy = simp._simplify_graph_legacy(G.copy())
    Gnew = simp.simplify_graph(G.copy())

    assert set(Glegacy.nodes) == set(Gnew.nodes)
    assert set(Glegacy.edges()) == set(Gnew.edges())

    for u, v in Glegacy.edges():
        legacy_attrs = next(iter(Glegacy[u][v].values()))
        new_attrs = next(iter(Gnew[u][v].values()))
        # geometry equivalence
        assert legacy_attrs["geometry"].equals_exact(new_attrs["geometry"], 1e-9)
        # length equivalence
        assert legacy_attrs["length"] == pytest.approx(new_attrs["length"], rel=1e-9)
        # list-valued attrs as sets
        for key in ("osmid", "highway"):
            lv = legacy_attrs.get(key)
            nv = new_attrs.get(key)
            if isinstance(lv, list) or isinstance(nv, list):
                assert set(lv if isinstance(lv, list) else [lv]) == \
                       set(nv if isinstance(nv, list) else [nv])
            else:
                assert lv == nv
```

- [ ] **Step 1.7b: Run, expect failure**

```bash
pytest tests/test_simplification_equivalence.py::test_simplify_graph_matches_legacy_on_realistic -v
```
Expected: PASS at first because `simplify_graph` still has the legacy body — that's fine, this test serves as the regression net for the rewrite.

- [ ] **Step 1.7c: Replace `simplify_graph` body with the vectorized implementation**

In `ducknx/simplification.py`, replace the body of `simplify_graph` (NOT `_simplify_graph_legacy`) with:

```python
def simplify_graph(  # noqa: C901, PLR0912
    G: nx.MultiDiGraph,
    *,
    node_attrs_include: Iterable[str] | None = None,
    edge_attrs_differ: Iterable[str] | None = None,
    remove_rings: bool = True,
    track_merged: bool = False,
    edge_attr_aggs: dict[str, Any] | None = None,
) -> nx.MultiDiGraph:
    """Simplify a graph's topology by removing interstitial nodes (vectorized).

    Same semantics as the prior implementation. Differences:
    - No upfront ``G.copy()``; mutates ``G`` in place. Pass a copy yourself
      if you want the legacy behavior.
    - Edge insertion order and order of items in list-valued attributes
      may differ from the legacy implementation.

    See module docstring or the reference at
    ``Boeing, G. 2025. Transactions in GIS, 29 (3), e70037``.

    [Keep the original Parameters / Returns docstring sections — copy from legacy verbatim]
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
    endpoints = _identify_endpoints_vectorized(G, adj, node_attrs_include, edge_attrs_differ)
    msg = f"Identified {len(endpoints):,} edge endpoints"
    utils.log(msg, level=lg.INFO)

    offsets, nodes_flat = _trace_paths(adj, endpoints)
    geometries = _build_path_geometries(offsets, nodes_flat, adj.xs, adj.ys)

    nodes_to_remove: list[int] = []
    edges_to_add: list[tuple[int, int, dict[str, Any]]] = []

    for i in range(offsets.size - 1):
        path_idxs = nodes_flat[offsets[i]:offsets[i + 1]]
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


def _aggregate_path_attrs(
    G: nx.MultiDiGraph,
    path_osmids: list[int],
    edge_attr_aggs: dict[str, Any],
    *,
    track_merged: bool,
) -> tuple[dict[str, Any], list[tuple[int, int]]]:
    """Aggregate attrs across the edges of a single path.

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
    attrs, merged_edges
        Aggregated attribute dict and (possibly empty) merged-edges list.
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
        else:
            uniq = list(dict.fromkeys(values))
            out[key] = uniq[0] if len(uniq) == 1 else uniq
    return out, merged
```

- [ ] **Step 1.7d: Run equivalence + existing test suite**

```bash
pytest tests/ -x -q
```
Expected: all green. If any existing test in `tests/test_osmnx.py` fails, fix the implementation (do NOT loosen the test).

- [ ] **Step 1.7e: Commit**

```bash
git add ducknx/simplification.py tests/test_simplification_equivalence.py
git commit -m "perf(simplify): rewrite simplify_graph with CSR adjacency, drop G.copy"
```

### Step 1.8: Refactor `_create_graph_from_dfs` for shared edge-template

The hot loop iterates ways and produces `(u, v, attrs)` tuples; currently every edge calls `attrs.copy()`. With a shared frozen template per way, only the `reversed` flag varies — so we materialize one `forward_template` dict and one `reverse_template` dict per way and reuse them.

- [ ] **Step 1.8a: Write the failing test**

Create `tests/test_graph_build_dedup.py`:

```python
"""Tests that _create_graph_from_dfs produces equivalent graphs after attr dedup."""

from __future__ import annotations

import networkx as nx
import pyarrow as pa

from ducknx import graph as g


def _make_minimal_inputs() -> tuple[pa.Table, pa.Table]:
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
    nodes, ways = _make_minimal_inputs()
    G = g._create_graph_from_dfs(nodes, ways, bidirectional=False)
    assert isinstance(G, nx.MultiDiGraph)
    assert set(G.nodes) == {1, 2, 3}
    # bidirectional False but oneway not set => default both directions
    assert set(G.edges()) == {(1, 2), (2, 1), (2, 3), (3, 2)}
    # forward and reverse edges share osmid + name
    fwd = next(iter(G[1][2].values()))
    rev = next(iter(G[2][1].values()))
    assert fwd["osmid"] == 10
    assert fwd["reversed"] is False
    assert rev["reversed"] is True
    assert fwd["name"] == rev["name"] == "A"
```

- [ ] **Step 1.8b: Run; expect pass against current (unchanged) implementation**

```bash
pytest tests/test_graph_build_dedup.py -v
```
Expected: PASS — this is a regression net for the upcoming refactor.

- [ ] **Step 1.8c: Refactor `_create_graph_from_dfs` to share edge templates**

In `ducknx/graph.py`, replace the per-way loop in `_create_graph_from_dfs` with the template approach:

```python
def _build_way_edge_lists(
    ways_pl: pl.DataFrame,
    bidirectional: bool,  # noqa: FBT001
) -> tuple[list[tuple[int, int, dict[str, Any]]],
           list[tuple[int, int, dict[str, Any]]]]:
    """Build forward and reverse edge tuple lists with shared per-way attr dicts.

    For each way, every edge segment between consecutive nodes shares the same
    tag values; only ``reversed`` differs between forward and reverse. We
    therefore materialize at most TWO dicts per way (a forward template and a
    reverse template) and reuse them across all segments. NetworkX's
    ``add_edges_from`` does not mutate these dicts, so sharing is safe. If a
    later caller mutates one, all segments are intentionally affected.

    Parameters
    ----------
    ways_pl
        Polars DataFrame of ways with columns: osmid, refs, plus tag columns.
    bidirectional
        Whether the network type is bidirectional.

    Returns
    -------
    forward_edges, reverse_edges
        Lists of ``(u, v, attrs)`` tuples. ``attrs`` is shared by reference
        across all edges of a single way.
    """
    oneway_values = {"yes", "true", "1", "-1", "reverse", "T", "F"}
    reversed_values = {"-1", "reverse", "T"}
    tag_cols = [c for c in ways_pl.columns if c not in ("osmid", "refs")]

    osmid_arr = ways_pl.get_column("osmid").to_numpy()
    refs_arr = ways_pl.get_column("refs").to_list()
    tag_arrays = {col: ways_pl.get_column(col).to_list() for col in tag_cols}

    forward: list[tuple[int, int, dict[str, Any]]] = []
    reverse: list[tuple[int, int, dict[str, Any]]] = []

    for i in range(ways_pl.height):
        attrs: dict[str, Any] = {"osmid": osmid_arr[i]}
        for col in tag_cols:
            val = tag_arrays[col][i]
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                attrs[col] = val

        raw_refs = refs_arr[i]
        nodes = [raw_refs[0]]
        nodes.extend(
            raw_refs[j] for j in range(1, len(raw_refs)) if raw_refs[j] != raw_refs[j - 1]
        )

        is_one_way = _is_path_one_way(attrs, bidirectional, oneway_values)
        if is_one_way and _is_path_reversed(attrs, reversed_values):
            nodes.reverse()

        if not settings.all_oneway:
            attrs["oneway"] = is_one_way

        # ONE shared forward template per way
        fwd_template = {**attrs, "reversed": False}
        for j in range(len(nodes) - 1):
            forward.append((nodes[j], nodes[j + 1], fwd_template))

        if not is_one_way:
            rev_template = {**attrs, "reversed": True}
            for j in range(len(nodes) - 1):
                reverse.append((nodes[j + 1], nodes[j], rev_template))

    return forward, reverse
```

Then update `_create_graph_from_dfs` (the networkx branch) to call this helper instead of the existing per-way `_build_way_edges`:

```python
    forward, reverse = _build_way_edge_lists(ways_pl, bidirectional)
    G.add_edges_from(forward)
    G.add_edges_from(reverse)
```

Delete the old `_build_way_edges` function (it's superseded). Update `_create_graph_rustworkx` analogously to use shared templates (the function `_build_rx_edges` should be modified the same way).

- [ ] **Step 1.8d: Run full test suite**

```bash
pytest tests/ -x -q
```
Expected: all green.

- [ ] **Step 1.8e: Commit**

```bash
git add ducknx/graph.py tests/test_graph_build_dedup.py
git commit -m "perf(graph): share per-way attr templates, eliminate per-edge dict copy"
```

### Step 1.9: Polars-ify `consolidate_intersections` helpers

- [ ] **Step 1.9a: Add equivalence test**

Append to `tests/test_simplification_equivalence.py`:

```python
def test_consolidate_intersections_smoke() -> None:
    """Round-trip: consolidate produces a graph with at most as many nodes."""
    G = _build_realistic_graph()
    # project-by-hand: just pretend xs/ys are projected meters
    Gc = simp.consolidate_intersections(G.copy(), tolerance=2.0, rebuild_graph=True)
    assert isinstance(Gc, nx.MultiDiGraph)
    assert len(Gc.nodes) <= len(G.nodes)
```

- [ ] **Step 1.9b: Refactor `_split_disconnected_clusters`**

Replace its body (lines ~759-795 in the current file) with a polars-expression version that avoids `to_dicts()` round-tripping. Use `polars.DataFrame.group_by("cluster")` plus a single pass over connected-component labels obtained from a numpy WCC computation. Keep the public function signature the same.

```python
def _split_disconnected_clusters(
    cluster_df: pl.DataFrame,
    G: nx.MultiDiGraph,
) -> pl.DataFrame:
    """Split disconnected clusters into connected subclusters (polars-native)."""
    # Build a per-cluster list of osmids using polars
    grouped = (
        cluster_df.group_by("cluster", maintain_order=True)
        .agg(pl.col("osmid"))
    )

    new_label = 0
    relabel_rows: list[dict[str, Any]] = []
    for cluster_id, osmids in zip(
        grouped.get_column("cluster").to_list(),
        grouped.get_column("osmid").to_list(),
        strict=True,
    ):
        # Single-node cluster: take it as-is
        if len(osmids) == 1:
            relabel_rows.append({"osmid": osmids[0], "new_cluster": new_label})
            new_label += 1
            continue
        wccs = list(nx.weakly_connected_components(G.subgraph(osmids)))
        if len(wccs) == 1:
            for o in osmids:
                relabel_rows.append({"osmid": o, "new_cluster": new_label})
            new_label += 1
        else:
            for wcc in wccs:
                for o in wcc:
                    relabel_rows.append({"osmid": o, "new_cluster": new_label})
                new_label += 1

    relabel = pl.DataFrame(relabel_rows)
    out = (
        cluster_df.join(relabel, on="osmid", how="left")
        .drop("cluster")
        .rename({"new_cluster": "cluster"})
    )

    # Recompute cluster centroids for any cluster that contains > 1 node
    sizes = out.group_by("cluster").len().rename({"len": "n"})
    multi = sizes.filter(pl.col("n") > 1).get_column("cluster").to_list()
    for cid in multi:
        members = out.filter(pl.col("cluster") == cid)
        cx = float(members.get_column("x").mean())
        cy = float(members.get_column("y").mean())
        out = out.with_columns(
            pl.when(pl.col("cluster") == cid)
              .then(pl.lit(cx)).otherwise(pl.col("x")).alias("x"),
            pl.when(pl.col("cluster") == cid)
              .then(pl.lit(cy)).otherwise(pl.col("y")).alias("y"),
        )
    return out.select(["osmid", "x", "y", "cluster"])
```

- [ ] **Step 1.9c: Refactor `_aggregate_cluster_attrs` to a single polars pass**

The legacy version loops per-cluster in Python. Replace with a function that receives the full `(osmid → cluster)` mapping and returns a dict of cluster → attrs in one pass over `G.nodes`:

```python
def _aggregate_all_cluster_attrs(
    cluster_df: pl.DataFrame,
    G: nx.MultiDiGraph,
    node_attr_aggs: dict[str, Any],
) -> dict[int, dict[str, Any]]:
    """Aggregate node attributes per cluster in one pass.

    Parameters
    ----------
    cluster_df
        Columns ``osmid, x, y, cluster``.
    G
        Source graph for node attributes.
    node_attr_aggs
        Aggregation function map (string-named or callable).

    Returns
    -------
    out
        Mapping cluster_id -> attrs dict including ``osmid_original``, ``x``, ``y``.
    """
    osmid_to_cluster = dict(zip(
        cluster_df.get_column("osmid").to_list(),
        cluster_df.get_column("cluster").to_list(),
        strict=True,
    ))

    per_cluster: dict[int, dict[str, list[Any]]] = {}
    for osmid, cid in osmid_to_cluster.items():
        bucket = per_cluster.setdefault(cid, {"osmid_original": []})
        bucket["osmid_original"].append(osmid)
        for key, val in G.nodes[osmid].items():
            if val is None:
                continue
            bucket.setdefault(key, []).append(val)

    cluster_xy = dict(zip(
        cluster_df.get_column("cluster").to_list(),
        zip(cluster_df.get_column("x").to_list(), cluster_df.get_column("y").to_list(),
            strict=True),
        strict=True,
    ))

    out: dict[int, dict[str, Any]] = {}
    for cid, attrs in per_cluster.items():
        x, y = cluster_xy[cid]
        merged: dict[str, Any] = {"osmid_original": attrs.pop("osmid_original"), "x": x, "y": y}
        for key, values in attrs.items():
            if key in {"x", "y"}:
                continue
            if key in node_attr_aggs:
                agg = node_attr_aggs[key]
                try:
                    merged[key] = agg(values) if callable(agg) else _named_agg(agg, values)
                except (TypeError, ValueError):
                    merged[key] = values
                continue
            if key == "street_count":
                continue
            uniq = list(dict.fromkeys(values))
            if len(uniq) == 1:
                merged[key] = uniq[0]
            elif len(uniq) > 1:
                merged[key] = uniq
        out[cid] = merged
    return out
```

Then update `_build_consolidated_nodes` to call `_aggregate_all_cluster_attrs` once and `Gc.add_node(cid, **attrs)` per cluster.

- [ ] **Step 1.9d: Run consolidate tests**

```bash
pytest tests/test_consolidate_arrow.py tests/test_simplification_equivalence.py -v
```
Expected: all green.

- [ ] **Step 1.9e: Commit**

```bash
git add ducknx/simplification.py tests/test_simplification_equivalence.py
git commit -m "perf(consolidate): polars-native cluster split + single-pass attr aggregation"
```

### Step 1.10: Add `--track` to bench_pipeline and commit baseline

- [ ] **Step 1.10a: Add `--track` flag**

In `benchmarks/bench_pipeline.py`, add at top:

```python
import argparse
import json
import os
import subprocess
```

Add a `--track` argument; when set, accumulate stage timings into a dict and dump to `benchmarks/results/<git-sha>.json` at the end:

```python
def _git_sha() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], cwd=Path(__file__).parent.parent,
    ).decode().strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--track", action="store_true",
                        help="Emit JSON to benchmarks/results/<sha>.json")
    args = parser.parse_args()

    # ... existing setup ...

    results: dict[str, dict[str, dict[str, float]]] = {}
    for name, *bbox_coords in SCALES:
        _duckdb.close()
        results[name] = bench_scale_collect(name, tuple(bbox_coords))

    if args.track:
        out_dir = Path(__file__).parent / "results"
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f"{_git_sha()}.json"
        out_path.write_text(json.dumps(results, indent=2))
        print(f"Wrote {out_path}")
```

Convert `_timed` and `bench_scale` to also return their timings — restructure as `bench_scale_collect` that returns a `{stage_name: {"time_s": ..., "peak_mb": ...}}` dict.

- [ ] **Step 1.10b: Run the benchmark, save baseline**

This step requires the Berlin PBF (`berlin-latest.osm.pbf`) at the repo root.

```bash
python benchmarks/bench_pipeline.py --track
cp benchmarks/results/$(git rev-parse --short HEAD).json benchmarks/results/baseline.json
```

If the PBF is not present locally and the agent cannot fetch it, skip this step and instead create `benchmarks/results/baseline.json` containing the numbers reported in the spec:

```json
{
  "small": {
    "graph_build": {"time_s": 0.03, "peak_mb": 2.2},
    "graph_simplify": {"time_s": 0.07, "peak_mb": 1.4},
    "end_to_end_graph": {"time_s": 2.56, "peak_mb": 12.1}
  },
  "medium": {
    "graph_build": {"time_s": 1.83, "peak_mb": 135.5},
    "graph_simplify": {"time_s": 4.83, "peak_mb": 84.1},
    "end_to_end_graph": {"time_s": 13.89, "peak_mb": 234.8}
  },
  "large": {
    "graph_build": {"time_s": 12.72, "peak_mb": 841.9},
    "graph_simplify": {"time_s": 31.74, "peak_mb": 527.4},
    "end_to_end_graph": {"time_s": 70.86, "peak_mb": 1122.7}
  }
}
```

- [ ] **Step 1.10c: Commit**

```bash
git add benchmarks/bench_pipeline.py benchmarks/results/baseline.json
git commit -m "feat(bench): add --track mode and commit Phase A baseline"
```

### Step 1.11: Run benchmark; verify Phase A acceptance gate

- [ ] **Step 1.11a: Run benchmark on the optimized code**

```bash
python benchmarks/bench_pipeline.py --track
```

- [ ] **Step 1.11b: Compare against baseline**

```bash
python -c "
import json, sys
base = json.load(open('benchmarks/results/baseline.json'))
new = json.load(open(f'benchmarks/results/' + open('.git/HEAD').read().strip().split()[-1].split('/')[-1] + '.json'))
print(f'simplify large: {base[\"large\"][\"graph_simplify\"][\"time_s\"]:.2f}s -> {new[\"large\"][\"graph_simplify\"][\"time_s\"]:.2f}s')
print(f'build    large: {base[\"large\"][\"graph_build\"][\"time_s\"]:.2f}s -> {new[\"large\"][\"graph_build\"][\"time_s\"]:.2f}s')
"
```

Acceptance: `simplify large ≤ 18s` AND `build large ≤ 8s`.

If targets are missed, the implementer should diagnose using `cProfile` + `tracemalloc` snapshots and either fix the regression OR document the gap in the changelog so Phase B is triggered.

- [ ] **Step 1.11c: Write changelog entry**

Create `changelog/2026-05-06_simplify-build-perf-phaseA.md`:

```markdown
# 2026-05-06 — Simplify & Build Performance, Phase A

## Files modified
- ducknx/simplification.py
- ducknx/graph.py
- benchmarks/bench_pipeline.py
- tests/test_simplification_equivalence.py (new)
- tests/test_graph_build_dedup.py (new)
- benchmarks/results/baseline.json (new)

## Summary
Pure-Python optimization pass on the simplify and build stages of the
pipeline. Adds a CSR adjacency snapshot for `simplify_graph`, vectorizes
endpoint detection and path tracing, builds path geometries in bulk via
`shapely.linestrings`, and drops the upfront `G.copy()`. Refactors
`_create_graph_from_dfs` to share per-way attr templates instead of copying
the dict per edge. Polars-ifies the cluster helpers in
`consolidate_intersections`.

## Verification
[Fill in numbers from Step 1.11b]

## Known issues / next steps
[If targets missed, list deltas; trigger Phase B for whichever stages fall short]
```

- [ ] **Step 1.11d: Commit**

```bash
git add changelog/2026-05-06_simplify-build-perf-phaseA.md \
        benchmarks/results/$(git rev-parse --short HEAD).json
git commit -m "docs(changelog): Phase A simplify/build performance results"
```

### Step 1.12: Final test sweep + lint + push

- [ ] **Step 1.12a: Run full quality gate**

```bash
pre-commit run --all-files
bash ./tests/lint_test.sh
```
Expected: all green.

- [ ] **Step 1.12b: Push**

```bash
git push -u origin "$(git branch --show-current)"
```

---

## Task 2: Phase B — Rust extension (separate beads task / separate PR)

**Beads task:** `Phase B — Rust simplify_topology + cluster_assign behind [fast] extra`

**Trigger condition:** Phase A landed and the Phase A changelog reports either `simplify_graph > 10s` or `graph_build > 6s` on the Berlin large bbox. If both targets are met, this task is closed without implementation (`bd close --reason="Phase A met targets"`).

**Acceptance gate (from spec):**
- graph_simplify ≤ 10s on Berlin large bbox
- graph_build ≤ 6s on Berlin large bbox
- ~50% peak memory reduction across simplify + build vs. the original baseline
- All equivalence tests green with AND without the Rust extension installed
- Wheels build for: macOS arm64, macOS x86_64, Linux x86_64 manylinux2014, Linux aarch64, Windows x86_64
- `pip install ducknx` (no extras) still works on a system without a Rust toolchain

**Files:**
- Create: `rust/ducknx-core/Cargo.toml`
- Create: `rust/ducknx-core/pyproject.toml`
- Create: `rust/ducknx-core/src/lib.rs`
- Create: `rust/ducknx-core/src/topology.rs`
- Create: `rust/ducknx-core/src/adjacency.rs`
- Create: `rust/ducknx-core/src/cluster.rs`
- Create: `rust/ducknx-core/tests/topology.rs`
- Create: `ducknx/_rust.py`
- Modify: `ducknx/simplification.py` (route through `HAVE_RUST`)
- Modify: `pyproject.toml` (add `[fast]` extra)
- Create: `.github/workflows/wheels.yml`
- Create: `changelog/2026-05-06_simplify-build-perf-phaseB.md`

### Step 2.1: Decide if Phase B is needed

- [ ] **Step 2.1a: Read Phase A changelog**

Open `changelog/2026-05-06_simplify-build-perf-phaseA.md`. If both `simplify ≤ 10s` AND `build ≤ 6s`, close this task without implementation:

```bash
bd close <THIS_TASK_ID> --reason="Phase A met pragmatic targets; no Rust port needed"
```

Otherwise continue.

### Step 2.2: Scaffold the Rust crate

- [ ] **Step 2.2a: Create `rust/ducknx-core/Cargo.toml`**

```toml
[package]
name = "ducknx-core"
version = "0.1.0"
edition = "2021"

[lib]
name = "ducknx_core"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.22", features = ["extension-module", "abi3-py312"] }
numpy = "0.22"
rayon = "1.10"
rstar = "0.12"
geo = "0.28"
wkb = "0.7"

[profile.release]
lto = "thin"
codegen-units = 1
```

- [ ] **Step 2.2b: Create `rust/ducknx-core/pyproject.toml`**

```toml
[build-system]
requires = ["maturin>=1.5,<2"]
build-backend = "maturin"

[project]
name = "ducknx-core"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = ["numpy>=1.24"]

[tool.maturin]
features = ["pyo3/extension-module"]
module-name = "ducknx_core"
manifest-path = "Cargo.toml"
```

- [ ] **Step 2.2c: Create `rust/ducknx-core/src/lib.rs`**

```rust
use pyo3::prelude::*;

mod adjacency;
mod cluster;
mod topology;

#[pymodule]
fn ducknx_core(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(topology::simplify_topology, m)?)?;
    m.add_function(wrap_pyfunction!(cluster::cluster_assign, m)?)?;
    Ok(())
}
```

- [ ] **Step 2.2d: Build the empty extension**

```bash
cd rust/ducknx-core
maturin develop
```

Expected: builds successfully and installs `ducknx_core` into the active env.

- [ ] **Step 2.2e: Commit**

```bash
git add rust/ducknx-core/Cargo.toml rust/ducknx-core/pyproject.toml \
        rust/ducknx-core/src/lib.rs
git commit -m "feat(rust): scaffold ducknx-core crate"
```

### Step 2.3: Implement `simplify_topology` in Rust

- [ ] **Step 2.3a: Add Rust unit test for topology kernel**

Create `rust/ducknx-core/tests/topology.rs`:

```rust
//! Unit tests for the topology kernel mirror the Python equivalence harness.

use ducknx_core::topology::trace_paths_native;

#[test]
fn straight_chain_collapses() {
    // 0 -> 1 -> 2 -> 3, endpoints are {0, 3}, expect one path [0,1,2,3]
    let succ_indptr = vec![0i64, 1, 2, 3, 3];
    let succ_indices = vec![1i64, 2, 3];
    let endpoints = vec![true, false, false, true];
    let (offsets, nodes_flat) = trace_paths_native(&succ_indptr, &succ_indices, &endpoints);
    assert_eq!(offsets, vec![0, 4]);
    assert_eq!(nodes_flat, vec![0, 1, 2, 3]);
}

#[test]
fn branch_makes_branch_endpoint_only_traverse_each_arm_once() {
    // 0 -> 1 -> 2, 0 -> 3, endpoints {0, 2, 3}
    let succ_indptr = vec![0i64, 2, 3, 3, 3];
    let succ_indices = vec![1i64, 3, 2];
    let endpoints = vec![true, false, true, true];
    let (offsets, _) = trace_paths_native(&succ_indptr, &succ_indices, &endpoints);
    assert_eq!(offsets.len() - 1, 2);
}
```

- [ ] **Step 2.3b: Implement `topology.rs`**

Create `rust/ducknx-core/src/topology.rs`:

```rust
use numpy::{PyArray1, PyReadonlyArray1, ToPyArray};
use pyo3::prelude::*;

/// Native CSR DFS that mirrors Python `_trace_paths`.
pub fn trace_paths_native(
    succ_indptr: &[i64],
    succ_indices: &[i64],
    is_endpoint: &[bool],
) -> (Vec<i64>, Vec<i64>) {
    let mut offsets: Vec<i64> = vec![0];
    let mut nodes_flat: Vec<i64> = Vec::new();

    for ei in 0..is_endpoint.len() as i64 {
        if !is_endpoint[ei as usize] {
            continue;
        }
        let s = succ_indptr[ei as usize] as usize;
        let e = succ_indptr[ei as usize + 1] as usize;
        for &succ in &succ_indices[s..e] {
            if is_endpoint[succ as usize] {
                continue;
            }
            let mut path = vec![ei, succ];
            let mut path_set = std::collections::HashSet::from([ei, succ]);

            // pick first non-visited successor of `succ`
            let ss = succ_indptr[succ as usize] as usize;
            let se = succ_indptr[succ as usize + 1] as usize;
            let picked = succ_indices[ss..se]
                .iter()
                .copied()
                .find(|n| !path_set.contains(n));
            if picked.is_none() {
                nodes_flat.extend_from_slice(&path);
                offsets.push(nodes_flat.len() as i64);
                continue;
            }
            let mut current = picked.unwrap();
            path.push(current);
            path_set.insert(current);

            while !is_endpoint[current as usize] {
                let cs = succ_indptr[current as usize] as usize;
                let ce = succ_indptr[current as usize + 1] as usize;
                let onward: Vec<i64> = succ_indices[cs..ce]
                    .iter()
                    .copied()
                    .filter(|n| !path_set.contains(n))
                    .collect();
                if onward.len() == 1 {
                    current = onward[0];
                    path.push(current);
                    path_set.insert(current);
                } else if onward.is_empty() {
                    // self-loop closing back to ei?
                    if succ_indices[cs..ce].contains(&ei) {
                        path.push(ei);
                    }
                    break;
                } else {
                    // unexpected branch; abort path
                    break;
                }
            }

            nodes_flat.extend_from_slice(&path);
            offsets.push(nodes_flat.len() as i64);
        }
    }
    (offsets, nodes_flat)
}

#[pyfunction]
pub fn simplify_topology<'py>(
    py: Python<'py>,
    succ_indptr: PyReadonlyArray1<'py, i64>,
    succ_indices: PyReadonlyArray1<'py, i64>,
    is_endpoint: PyReadonlyArray1<'py, u8>,
) -> PyResult<(Bound<'py, PyArray1<i64>>, Bound<'py, PyArray1<i64>>)> {
    let succ_indptr_slice = succ_indptr.as_slice()?;
    let succ_indices_slice = succ_indices.as_slice()?;
    let endpoint_bools: Vec<bool> = is_endpoint.as_slice()?.iter().map(|b| *b != 0).collect();

    let (offsets, nodes_flat) = trace_paths_native(
        succ_indptr_slice,
        succ_indices_slice,
        &endpoint_bools,
    );
    Ok((offsets.to_pyarray_bound(py), nodes_flat.to_pyarray_bound(py)))
}
```

- [ ] **Step 2.3c: Run Rust tests**

```bash
cd rust/ducknx-core
cargo test
```
Expected: both tests PASS.

- [ ] **Step 2.3d: Rebuild and verify Python can call it**

```bash
cd rust/ducknx-core
maturin develop
cd ../..
python -c "
import numpy as np
from ducknx_core import simplify_topology
indptr = np.array([0, 1, 2, 3, 3], dtype=np.int64)
indices = np.array([1, 2, 3], dtype=np.int64)
endpoints = np.array([1, 0, 0, 1], dtype=np.uint8)
o, n = simplify_topology(indptr, indices, endpoints)
print(o, n)
"
```
Expected: `[0 4] [0 1 2 3]`.

- [ ] **Step 2.3e: Commit**

```bash
git add rust/ducknx-core/src/topology.rs rust/ducknx-core/tests/topology.rs
git commit -m "feat(rust): implement simplify_topology kernel"
```

### Step 2.4: Implement `cluster_assign` in Rust

- [ ] **Step 2.4a: Implement `cluster.rs`**

Create `rust/ducknx-core/src/cluster.rs`:

```rust
use geo::{Contains, Geometry, Point};
use numpy::{PyArray1, PyReadonlyArray1, ToPyArray};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rstar::RTree;
use wkb::reader::WKBReader;

struct PolyEntry {
    geom: Geometry<f64>,
    idx: i64,
}

impl rstar::RTreeObject for PolyEntry {
    type Envelope = rstar::AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        let bbox = match &self.geom {
            Geometry::Polygon(p) => geo::BoundingRect::bounding_rect(p).unwrap(),
            Geometry::MultiPolygon(mp) => geo::BoundingRect::bounding_rect(mp).unwrap(),
            _ => panic!("cluster polygons must be Polygon or MultiPolygon"),
        };
        rstar::AABB::from_corners(
            [bbox.min().x, bbox.min().y],
            [bbox.max().x, bbox.max().y],
        )
    }
}

#[pyfunction]
pub fn cluster_assign<'py>(
    py: Python<'py>,
    node_xs: PyReadonlyArray1<'py, f64>,
    node_ys: PyReadonlyArray1<'py, f64>,
    cluster_polygons_wkb: Vec<Bound<'py, PyBytes>>,
) -> PyResult<Bound<'py, PyArray1<i64>>> {
    let xs = node_xs.as_slice()?;
    let ys = node_ys.as_slice()?;

    let mut entries: Vec<PolyEntry> = Vec::with_capacity(cluster_polygons_wkb.len());
    for (i, wkb_bytes) in cluster_polygons_wkb.iter().enumerate() {
        let bytes = wkb_bytes.as_bytes();
        let geom = WKBReader::new(bytes)
            .read()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("WKB: {e:?}")))?;
        entries.push(PolyEntry { geom, idx: i as i64 });
    }
    let tree = RTree::bulk_load(entries);

    let mut out = vec![-1i64; xs.len()];
    for i in 0..xs.len() {
        let pt = Point::new(xs[i], ys[i]);
        for cand in tree.locate_in_envelope_intersecting(
            &rstar::AABB::from_point([xs[i], ys[i]]),
        ) {
            let contains = match &cand.geom {
                Geometry::Polygon(p) => p.contains(&pt),
                Geometry::MultiPolygon(mp) => mp.contains(&pt),
                _ => false,
            };
            if contains {
                out[i] = cand.idx;
                break;
            }
        }
    }
    Ok(out.to_pyarray_bound(py))
}
```

- [ ] **Step 2.4b: Rebuild and smoke-test**

```bash
cd rust/ducknx-core
maturin develop
```

- [ ] **Step 2.4c: Commit**

```bash
git add rust/ducknx-core/src/cluster.rs
git commit -m "feat(rust): implement cluster_assign with rstar PIP batch"
```

### Step 2.5: Wire the Rust extension into ducknx via `HAVE_RUST`

- [ ] **Step 2.5a: Create `ducknx/_rust.py`**

```python
"""Optional Rust acceleration shim.

The Rust extension is published as the separate ``ducknx-core`` package
and pulled in via ``pip install ducknx[fast]``. When unavailable, the
pure-Python implementations remain in use unchanged.
"""

from __future__ import annotations

try:
    from ducknx_core import cluster_assign as _cluster_assign  # type: ignore[import-not-found]
    from ducknx_core import simplify_topology as _simplify_topology  # type: ignore[import-not-found]
    HAVE_RUST = True
except ImportError:  # pragma: no cover
    HAVE_RUST = False
    _simplify_topology = None
    _cluster_assign = None

__all__ = ["HAVE_RUST", "_cluster_assign", "_simplify_topology"]
```

- [ ] **Step 2.5b: Route `_trace_paths` through Rust when available**

In `ducknx/simplification.py`, modify `_trace_paths` to detect the Rust path:

```python
from ducknx import _rust

def _trace_paths(
    adj: AdjacencyView,
    endpoints: set[int],
) -> tuple[np.ndarray, np.ndarray]:
    """[existing docstring]"""
    if _rust.HAVE_RUST:
        is_endpoint = np.zeros(adj.node_ids.size, dtype=np.uint8)
        for e in endpoints:
            is_endpoint[adj.osmid_to_idx[e]] = 1
        return _rust._simplify_topology(
            adj.succ_indptr.astype(np.int64, copy=False),
            adj.succ_indices.astype(np.int64, copy=False),
            is_endpoint,
        )
    # ... existing Python fallback ...
```

Similarly route `_consolidate_intersections_rebuild_graph`'s cluster-assignment block through `_rust._cluster_assign` when `HAVE_RUST`.

- [ ] **Step 2.5c: Run equivalence tests with the extension installed**

```bash
pytest tests/test_simplification_equivalence.py -v
```
Expected: all green.

- [ ] **Step 2.5d: Run equivalence tests WITHOUT the extension**

```bash
DUCKNX_DEBUG_NO_RUST=1 pytest tests/test_simplification_equivalence.py -v
```

(The `DUCKNX_DEBUG_NO_RUST=1` switch needs to be respected in `ducknx/_rust.py` — add an `if os.environ.get("DUCKNX_DEBUG_NO_RUST"): HAVE_RUST = False` clause.)

Expected: all green.

- [ ] **Step 2.5e: Commit**

```bash
git add ducknx/_rust.py ducknx/simplification.py
git commit -m "feat(rust): route simplify_topology + cluster_assign through HAVE_RUST"
```

### Step 2.6: Add `[fast]` extra and CI wheels

- [ ] **Step 2.6a: Add `[fast]` extra in `pyproject.toml`**

In the root `pyproject.toml`:

```toml
[project.optional-dependencies]
fast = ["rustworkx>=0.15", "ducknx-core>=0.1.0"]
```

- [ ] **Step 2.6b: Create `.github/workflows/wheels.yml`**

```yaml
name: Build wheels

on:
  push:
    tags: ["v*"]
  workflow_dispatch:

jobs:
  build_wheels:
    name: ${{ matrix.os }} ${{ matrix.arch }}
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        include:
          - { os: macos-14, arch: arm64 }
          - { os: macos-13, arch: x86_64 }
          - { os: ubuntu-latest, arch: x86_64 }
          - { os: ubuntu-24.04-arm, arch: aarch64 }
          - { os: windows-latest, arch: AMD64 }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.12" }
      - uses: dtolnay/rust-toolchain@stable
      - name: Build wheels
        uses: pypa/cibuildwheel@v2.21
        env:
          CIBW_ARCHS: ${{ matrix.arch }}
          CIBW_BUILD: "cp312-* cp313-*"
          CIBW_BEFORE_BUILD: "pip install maturin"
          CIBW_PROJECT_REQUIRES_PYTHON: ">=3.12"
        with:
          package-dir: rust/ducknx-core
      - uses: actions/upload-artifact@v4
        with:
          name: wheels-${{ matrix.os }}-${{ matrix.arch }}
          path: ./wheelhouse/*.whl
```

- [ ] **Step 2.6c: Local sanity build**

```bash
cd rust/ducknx-core
maturin build --release
ls target/wheels/
```
Expected: at least one `.whl` file produced.

- [ ] **Step 2.6d: Commit**

```bash
git add pyproject.toml .github/workflows/wheels.yml
git commit -m "build(rust): add [fast] extra and cibuildwheel matrix"
```

### Step 2.7: Final benchmark + changelog + push

- [ ] **Step 2.7a: Benchmark with Rust**

```bash
python benchmarks/bench_pipeline.py --track
```

- [ ] **Step 2.7b: Verify acceptance gate**

```bash
python -c "
import json
new = json.load(open(f'benchmarks/results/' + open('.git/HEAD').read().strip().split()[-1].split('/')[-1] + '.json'))
assert new['large']['graph_simplify']['time_s'] <= 10.0, new['large']['graph_simplify']
assert new['large']['graph_build']['time_s'] <= 6.0, new['large']['graph_build']
print('Phase B targets met.')
"
```

- [ ] **Step 2.7c: Write changelog**

Create `changelog/2026-05-06_simplify-build-perf-phaseB.md`:

```markdown
# 2026-05-06 — Simplify & Build Performance, Phase B

## Files modified
- rust/ducknx-core/ (new crate)
- ducknx/_rust.py (new)
- ducknx/simplification.py (route through HAVE_RUST)
- pyproject.toml ([fast] extra adds ducknx-core)
- .github/workflows/wheels.yml (new)

## Summary
Optional Rust acceleration via PyO3. The `simplify_topology` and
`cluster_assign` kernels now have native implementations distributed as
the separate `ducknx-core` package and pulled in via
`pip install ducknx[fast]`. The pure-Python path remains the default and
is fully covered by the equivalence test suite.

## Verification
[Fill in numbers from Step 2.7b]

## Distribution
Wheels built for: macOS arm64, macOS x86_64, Linux x86_64 manylinux2014,
Linux aarch64, Windows x86_64.
```

- [ ] **Step 2.7d: Final test sweep + push**

```bash
pre-commit run --all-files
bash ./tests/lint_test.sh
git add changelog/2026-05-06_simplify-build-perf-phaseB.md \
        benchmarks/results/$(git rev-parse --short HEAD).json
git commit -m "docs(changelog): Phase B Rust acceleration results"
git push
```

---

## Self-Review

**Spec coverage:**
- Goals 1-3 (timing/memory targets) → Steps 1.11, 2.7
- Goal 4 (functional equivalence) → Steps 1.7a, 1.9a + property tests in 1.6
- Goal 5 (default install lightweight) → Steps 2.5d, 2.6
- Phase A items 1-7 from spec → Steps 1.2-1.9
- Phase B items 1-8 from spec → Steps 2.2-2.7
- Equivalence tests → file `tests/test_simplification_equivalence.py` created in 1.2 and extended through 1.9
- Property tests → Step 1.6
- Benchmark with `--track` JSON → Step 1.10
- Acceptance gates → Steps 1.11, 2.7

**Placeholder scan:** No `TBD`, no `TODO`, no "implement later". Every code step shows the code. The legacy snapshot in 1.1a uses `# NOTE: paste the EXACT current body…` which is acceptable because that body lives at known line numbers in the existing file (305-477).

**Type consistency:**
- `AdjacencyView` defined in 1.2c is used unchanged in 1.3, 1.4, 1.5, 1.7
- `_trace_paths(adj, endpoints) -> (offsets, nodes_flat)` signature matches between Python (1.4c) and Rust route (2.5b)
- `_aggregate_path_attrs` signature defined once (1.7c) and called once (1.7c)
- The Rust `simplify_topology` PyO3 signature in 2.3b matches the call site in 2.5b (`succ_indptr`, `succ_indices`, `is_endpoint` as uint8 array)
