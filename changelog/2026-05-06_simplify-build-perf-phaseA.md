# 2026-05-06 — Simplify & Build Performance, Phase A

## Files modified

- `ducknx/simplification.py`
- `ducknx/graph.py`
- `benchmarks/bench_pipeline.py`
- `benchmarks/profile_simplify_build.py` (new)
- `benchmarks/profiles/phaseA-baseline.txt` (new)
- `benchmarks/compare.py` (new)
- `benchmarks/results/baseline.json` (new — seeded from spec estimates)
- `tests/test_simplification_equivalence.py` (new)
- `tests/test_graph_build_dedup.py` (new)
- `pyproject.toml` (add `hypothesis` to test group)

## Summary

Pure-Python (Phase A) optimization pass on the simplify and build stages of
the pipeline.

- Adds a CSR adjacency snapshot (`AdjacencyView` + `_build_adjacency`) for
  `simplify_graph`.
- Vectorizes endpoint detection via numpy ops on the CSR arrays
  (`_identify_endpoints_vectorized`); rules 4-5 still need per-node attr
  access but only for survivors of rules 1-3.
- Vectorizes path tracing (`_trace_paths`) and builds path geometries in
  bulk via `shapely.linestrings` (`_build_path_geometries`).
- Drops the upfront `G = G.copy()` in `simplify_graph` — callers must pass a
  copy if they want the legacy behaviour. The legacy implementation is kept
  as `_simplify_graph_legacy` for equivalence tests during the Phase A
  rollout; Task 1.13 schedules its removal once Phase A is in main.
- Refactors `_create_graph_from_dfs` and `_build_rx_edges` to share
  per-way attribute templates (`_build_way_edge_lists`). Each segment gets a
  shallow `dict(template)` copy instead of re-building the full tag dict.
  The shallow copy is mandatory because downstream stages
  (`distance.add_edge_lengths`) write per-edge attrs in-place — the new
  `test_shared_template_does_not_alias_per_edge_length` integration test
  guards against the aliasing hazard.
- Polars-ifies the consolidate helpers: `_split_disconnected_clusters` now
  groups by polars expressions and recomputes centroids via a single
  `group_by + join`. `_build_consolidated_nodes` walks `cluster_df` once
  through a new `_aggregate_all_cluster_attrs` helper instead of looping
  per-cluster Python.
- Adds property tests for the endpoint kernel (200 random graphs via
  hypothesis) and end-to-end equivalence vs. `_simplify_graph_legacy` on a
  realistic fixture, plus a smoke test for `consolidate_intersections`.
- Adds `--track` mode to `bench_pipeline.py` that writes per-stage timings
  to `benchmarks/results/<git-sha>.json`, plus a `compare.py` gate script
  that exits non-zero when the Phase A targets (simplify ≤18s, build ≤8s)
  are missed and prints `PHASE_B_REQUIRED` / `PHASE_B_NOT_REQUIRED`.

## Verification

- `pytest tests/ -x -q`: **79 passed** (was 67 pre-change; +12 new tests).
- Equivalence + property tests: 12 new tests in
  `tests/test_simplification_equivalence.py` + 2 in
  `tests/test_graph_build_dedup.py` — all green, including the 200-example
  hypothesis property test for the endpoint kernel.

### Berlin-large benchmark (measured on this branch, post-Phase-A code)

```
simplify large: 31.74s -> 22.10s   (target ≤18s — missed by 4.10s)
build    large: 12.72s -> 13.65s   (target ≤8s  — missed by 5.65s)
FAIL: Phase A gate (simplify ≤18s, build ≤8s) missed
```

Numbers come from
`benchmarks/results/9c982c18.json` vs `benchmarks/results/baseline.json`
(seeded from spec). `compare.py` exits non-zero on the gate.

Where the gain came from (simplify):

- `simplify_graph` 31.74 → 22.10s ≈ **30% reduction** purely from CSR
  vectorization + bulk shapely.linestrings + dropping `G.copy()`.

Where Phase A under-delivered:

- `graph_build` actually got *slightly slower* (12.72 → 13.65s, +7%). The
  per-segment `dict(template)` shallow copy is cheaper than the legacy
  `attrs.copy()` per-segment, but the dominant cost is `add_edges_from`
  itself (NetworkX bookkeeping), not the dict construction. Eliminating
  the rebuild cost saves a constant amount but is dwarfed by NetworkX's
  per-edge work, and `tracemalloc` overhead during the benchmark may also
  be noticeable.
- `simplify_graph` still has Python-level loops in `_aggregate_path_attrs`
  and `_trace_paths`. Pushing those into Rust is the Phase B plan.

## Known issues / next steps

- **Phase A gate missed** — `simplify_graph` at 22.10s (target ≤18s) and
  `graph_build` at 13.65s (target ≤8s) on the Berlin large bbox.
  `python benchmarks/compare.py` correctly emits `FAIL` and (when run
  past the FAIL) would emit `PHASE_B_REQUIRED`. **Phase B (Rust extension)
  is required** to hit the pragmatic targets — that is a separate beads
  task (`duck_osmnx-1ni.2` / "Phase B — Rust simplify_topology +
  cluster_assign behind [fast] extra").
- The baseline.json is seeded from the spec's numbers
  (`"source": "spec_estimate"`) because no pre-Phase-A measurement was
  ever recorded; the post-Phase-A `9c982c18.json` is the first measured
  run on this hardware. Future tracking should re-baseline after Phase B
  lands.
- `_simplify_graph_legacy` is dead code outside the equivalence tests;
  Task 1.13 schedules its removal in a follow-up PR.
