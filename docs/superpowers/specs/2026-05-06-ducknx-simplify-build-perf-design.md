# ducknx — Simplify & Build Performance Design

**Date:** 2026-05-06
**Topic slug:** `ducknx-simplify-build-perf`
**Authors:** Frank Fortunat (with Claude)

## Summary

Optimize `simplify_graph`, `_create_graph_from_dfs`, and `consolidate_intersections` in
`ducknx`. These are the dominant CPU and memory costs end-to-end after the prior
`ducknx-performance` workflow flattened the DuckDB and Arrow paths. The benchmark on
the Berlin large bbox shows graph_simplify at 31.7s / 527 MB and graph_build at
12.7s / 842 MB — together 63% of the end-to-end large-bbox runtime.

This design delivers two phases:

- **Phase A — Pure-Python optimization.** CSR adjacency, vectorized endpoint
  detection, bulk geometry construction, elimination of `G.copy()`, and edge-attr
  deduplication. No new dependencies.
- **Phase B — Rust extension** (`ducknx-core`, exposed via optional `[fast]` extra).
  Topology kernel and cluster-assignment in Rust via PyO3, with prebuilt wheels
  for macOS/Linux/Windows. Falls back transparently to phase-A Python when the
  extension is not installed.

## Goals

1. graph_simplify ≤ 10s on the Berlin large bbox (currently 31.7s)
2. graph_build ≤ 6s on the Berlin large bbox (currently 12.7s)
3. Roughly 50% reduction in peak memory across the simplify + build stages
4. Functionally equivalent output: same set of nodes/edges and same key attribute
   values; insertion order and list-valued attribute ordering may differ
5. The pure-Python install (`pip install ducknx`) keeps working with no Rust
   toolchain required; the Rust extension is opt-in via `pip install ducknx[fast]`

## Non-Goals (Out of Scope)

- Routing optimization (`routing.py`)
- DuckDB query tuning — already flat (~2s on large)
- Replacing NetworkX as the canonical graph return type
- GPU acceleration
- Byte-identical output to the current implementation

## Design Decisions

1. **Hybrid Python-then-Rust strategy.** Phase A lands first as a single PR; if
   pragmatic targets are not met, Phase B follows. This keeps the Rust scope
   bounded — only what is still slow gets ported, not the whole module.
2. **Functional equivalence, not byte equivalence.** Order of items in
   list-valued attributes and edge insertion order in NetworkX are observable
   but not semantic. Treating them as semantic would block batched insertion,
   which is one of the larger Python-side wins.
3. **Optional Rust via `[fast]` extra.** Mirrors the existing `rustworkx` opt-in
   pattern in the repo, keeps the default install lightweight, and lets the
   Rust speedup ship as a documented upgrade rather than a forced dependency.
4. **Narrow Rust surface.** Rust handles topology (CSR traversal, endpoint
   detection, path tracing) and cluster assignment. Geometry construction,
   attribute aggregation, and graph mutation stay in Python where the dict
   polymorphism is awkward to express across the FFI boundary.
5. **Drop `G.copy()` in `simplify_graph`.** Read pass uses a CSR snapshot built
   once; mutation pass writes back to the input graph. Cuts peak memory roughly
   in half on the simplify stage.

## Architecture

### Phase A — Pure-Python optimization

**`ducknx/simplification.py::simplify_graph`:**

```
simplify_graph(G, ...)
  ├── _build_adjacency(G) -> AdjacencyView
  │     # CSR snapshot: numpy indptr/indices for succ + pred, xy arrays
  ├── _identify_endpoints_vectorized(adj, node_attrs_include, edge_attrs_differ)
  │     # rules 1-3 as numpy ops on degree/neighbor arrays
  │     # rules 4-5 fall back to per-node only for nodes that cleared 1-3
  ├── _trace_paths(adj, endpoints) -> (paths_offsets, paths_nodes_flat)
  │     # CSR-based DFS; flat arrays not list-of-lists
  ├── _build_path_geometries(paths_offsets, paths_nodes_flat, xy)
  │     # shapely.linestrings(coords, indices=offsets) — single C call
  ├── _aggregate_path_attrs(paths_offsets, paths_nodes_flat, G, edge_attr_aggs)
  │     # one pass; no Python-level zip
  └── _apply_simplification(G, ...)
        # only mutation step; no upfront G.copy()
```

`AdjacencyView` is a frozen dataclass:

```python
@dataclass(frozen=True)
class AdjacencyView:
    node_ids: np.ndarray            # int64
    osmid_to_idx: dict[int, int]
    succ_indptr: np.ndarray         # int64
    succ_indices: np.ndarray        # int64
    pred_indptr: np.ndarray         # int64
    pred_indices: np.ndarray        # int64
    xs: np.ndarray                  # float64
    ys: np.ndarray                  # float64
```

**`ducknx/graph.py::_create_graph_from_dfs`:**

```
_create_graph_from_dfs(nodes_df, ways_df, bidirectional, backend)
  ├── _add_nodes_bulk(G, nodes_pl)        # generator, no list materialization
  ├── _build_edges_arrow(ways_pl, ...)
  │     # vectorized over Arrow columns; shared attr template per way
  └── G.add_edges_from(edges_iter)        # generator avoids holding all tuples
```

Edge-attr deduplication: for a given way, every edge segment shares the same
tag values; only `reversed` differs between forward and reverse edges. Build
one frozen template dict per way and one `reversed=True` variant; assign by
reference until a path-level mutation happens.

**`ducknx/simplification.py::consolidate_intersections`:**

- `_split_disconnected_clusters` — replace `to_dicts() → loop → DataFrame`
  round-trip with polars `group_by` + numpy WCC labels.
- `_aggregate_cluster_attrs` — pre-pivot node attrs to a polars DataFrame and
  aggregate via expressions instead of per-cluster Python loops.

### Phase B — Rust extension

**Crate layout:**

```
rust/ducknx-core/
├── Cargo.toml              # pyo3, numpy, rayon
├── pyproject.toml          # maturin build config
├── src/
│   ├── lib.rs              # PyO3 module entry
│   ├── topology.rs         # simplify_topology + endpoint rules
│   ├── adjacency.rs        # CSR types + builders
│   └── cluster.rs          # cluster_assign (rstar-backed)
└── tests/                  # Rust-side unit tests on small fixtures
```

**PyO3 surface (narrow by design — endpoint detection stays in Python):**

```rust
#[pyfunction]
fn simplify_topology(
    succ_indptr: PyReadonlyArray1<i64>,
    succ_indices: PyReadonlyArray1<i64>,
    is_endpoint: PyReadonlyArray1<u8>,   // bool mask, Python computes it
) -> PyResult<(PyArray1<i64>, PyArray1<i64>)>
// returns (paths_offsets, paths_nodes_flat)
//
// Releases the GIL via py.allow_threads during the DFS so other Python
// threads can make progress on long-running calls.

#[pyfunction]
fn cluster_assign(
    node_xs: PyReadonlyArray1<f64>,
    node_ys: PyReadonlyArray1<f64>,
    cluster_polygons_wkb: Vec<&[u8]>,
) -> PyResult<PyArray1<i64>>
// Caller is responsible for passing planar (projected) coordinates;
// the kernel treats them as such and does not validate CRS.
```

Predecessor adjacency stays Python-only (used solely for rule 2/3 endpoint
detection, which is fast once vectorized). The Rust kernel only needs the
successor CSR. This keeps the FFI surface minimal and the Rust crate
focused on the actual hot loop (path tracing).

**Python integration:**

```python
# ducknx/_rust.py
try:
    from ducknx_core import simplify_topology, cluster_assign
    HAVE_RUST = True
except ImportError:
    HAVE_RUST = False
```

`simplify_graph` and `consolidate_intersections` route the topology kernel
through `HAVE_RUST`; adjacency building and geometry construction wrappers are
shared between paths.

**Data flow for `simplify_graph` with Rust:**

```
G (nx.MultiDiGraph)
  → _build_adjacency  (Python, numpy)
  → simplify_topology  (Rust, returns flat path arrays)
  → _build_path_geometries  (Python, shapely bulk)
  → _aggregate_path_attrs  (Python)
  → _apply_simplification  (Python, mutates G)
```

**Build & distribution:**

- `maturin` for the build backend
- `cibuildwheel` builds wheels for: macOS arm64, macOS x86_64, Linux x86_64
  (manylinux2014), Linux aarch64, Windows x86_64
- Published to PyPI as `ducknx-core`; ducknx pyproject lists it under
  `[project.optional-dependencies] fast`
- CI matrix builds wheels on tag pushes; `pytest` runs against the built wheel

## Error Handling

- Phase A: existing exception types unchanged (`GraphSimplificationError` for
  impossible simplification patterns). Adds a `_validate_adjacency` debug check
  (off by default) gated by `DUCKNX_DEBUG=1`.
- Rust extension: panics at the FFI boundary convert to Python `RuntimeError`
  with a descriptive message. No `catch_unwind` swallowing — logic bugs surface.
- Import-time fallback: `from ducknx_core import ...` failure is silent by
  default; visible only when `ducknx.utils.config(verbose=True)`.

## Testing

**Equivalence tests** (`tests/test_simplification_equivalence.py`):

- Build a graph from a small fixture PBF
- Run `simplify_graph` under both the legacy implementation and the new Python
  implementation
- Assert:
  - same set of node IDs
  - same set of `(u, v)` edges, ignoring `key` ordering
  - per-edge geometry equal via `equals_exact(tol=1e-9)`
  - per-edge length equal to 6 decimal places
  - list-valued attrs equal as sets (order-insensitive)
- Re-run the same suite with `HAVE_RUST=True` once the Rust extension is in
  place; Rust phase ships only if Python-vs-legacy and Rust-vs-Python
  equivalence both pass.

**Property tests** (`hypothesis`) on synthetic small graphs for the endpoint
detection kernel — guards against overfitting to OSM-shaped inputs.

**Rust unit tests** at `rust/ducknx-core/tests/` exercise the topology kernel
on synthetic CSR fixtures (cycle, lollipop, branch, self-loop, isolated ring).

**Existing test suite** (`tests/test_osmnx.py`) is the canonical behavior
contract — the optimized path must pass it unchanged.

## Benchmarks

- Extend the existing benchmark suite with a `--track` mode emitting JSON to
  `benchmarks/results/<git-sha>.json`.
- CI benchmark job runs small + medium scales (large is too slow for CI),
  diffs against committed baseline, fails if any stage regresses > 10%.
- Manual gate before merging each phase: report time + peak-memory deltas
  across small/medium/large vs. current `main`.

## Acceptance Gates

Memory measurements throughout are `tracemalloc.get_traced_memory()` peak per
stage — same methodology as the existing `benchmarks/bench_pipeline.py`.

**Phase A merge gate (intermediate — controls whether Phase B is required):**
- graph_simplify ≤ 18s on large bbox (≈ half current cost)
- graph_build ≤ 8s on large bbox
- All equivalence and existing tests green
- No CI benchmark stage regresses by more than 10%

**Phase B trigger condition (deterministic, from JSON):**
- After Phase A merges, the latest run's `benchmarks/results/<sha>.json` is
  compared against `benchmarks/results/baseline.json` programmatically. Phase B
  proceeds if and only if `large.graph_simplify.time_s > 10.0` OR
  `large.graph_build.time_s > 6.0`. No human judgment in the loop.

**Phase B merge gate (pragmatic — workflow goal):**
- graph_simplify ≤ 10s on large bbox
- graph_build ≤ 6s on large bbox
- ≥ 50% tracemalloc peak reduction across simplify + build vs. the original
  baseline (sum of `large.graph_simplify.peak_mb + large.graph_build.peak_mb`)
- All equivalence tests green with AND without Rust extension installed
- Wheels build successfully for all five target platforms
- `pip install ducknx` (no extra) still works on a system without Rust
- Cross-version safety: Python wrapper verifies `ducknx_core.__version__`
  matches a pinned compatible range at import time

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Functional-equivalence test passes but a downstream user depends on insertion order | Document explicitly in `CHANGELOG.md` and the `simplify_graph` docstring; bump minor version |
| CSR snapshot for very large graphs uses transient memory | Free numpy arrays immediately after `_apply_simplification` returns; benchmark peak |
| Rust extension hard to debug for users | `DUCKNX_DEBUG=1` env var routes through Python implementation even when Rust is installed; documented in troubleshooting guide |
| `cibuildwheel` matrix breaks on a platform | Phase B can ship with whichever wheels build; missing platforms fall back to source-build (which requires Rust toolchain) — degrades gracefully |
| Rust adds maintenance burden | Narrow surface (two functions); Rust unit tests are fast and isolated; crate is pinned to a single Rust edition |

## Implementation Sketch

Phase A (single PR):

0. **Profile first.** Capture cProfile + tracemalloc breakdown of the current
   simplify and build stages on Berlin large. Commit the profile artifacts so
   later phases can verify the bottlenecks actually moved. This guards
   against optimizing the wrong subroutine.
1. Add `AdjacencyView` and `_build_adjacency` in `simplification.py`
2. Replace `_identify_endpoints` and `_get_paths_to_simplify` with
   vectorized + CSR versions
3. Replace per-path `LineString` construction with `shapely.linestrings` bulk
4. Drop `G.copy()` from `simplify_graph`
5. Refactor `_create_graph_from_dfs` and `_create_graph_rustworkx` to share
   the edge-template / generator-based bulk insertion
6. Refactor `_split_disconnected_clusters` and `_aggregate_cluster_attrs`
7. Add equivalence tests + property tests
8. Run benchmark, attach numbers to the PR
9. After merge, follow-up commit removes `_simplify_graph_legacy` (it exists
   only as a reference oracle for equivalence tests during Phase A and
   becomes dead code once the new implementation is in main)

Phase B (separate PR, only if Phase A misses the pragmatic target):

1. Scaffold `rust/ducknx-core/` (Cargo.toml, pyproject.toml, lib.rs)
2. Port `simplify_topology` to Rust, expose via PyO3
3. Port `cluster_assign` to Rust
4. Add `ducknx/_rust.py` and route through `HAVE_RUST`
5. Set up `cibuildwheel` matrix in CI
6. Add `[fast]` extra to ducknx `pyproject.toml`
7. Run benchmarks with and without `[fast]`; document results in `CHANGELOG.md`

## References

- Prior workflow spec: `docs/superpowers/specs/2026-04-21-ducknx-performance-design.md`
- Boeing, G. 2025. "Topological Graph Simplification Solutions to the Street
  Intersection Miscount Problem." *Transactions in GIS*, 29 (3), e70037.
- PyO3 user guide: https://pyo3.rs
- maturin: https://www.maturin.rs
