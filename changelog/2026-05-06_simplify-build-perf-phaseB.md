# 2026-05-06 — Simplify & Build Performance, Phase B

## Files modified / created

New Rust crate:
- `rust/ducknx-core/Cargo.toml`
- `rust/ducknx-core/pyproject.toml`
- `rust/ducknx-core/src/lib.rs`
- `rust/ducknx-core/src/topology.rs` — CSR DFS kernel mirroring
  `ducknx.simplification._trace_paths`
- `rust/ducknx-core/src/cluster.rs` — rstar-backed point-in-polygon batch
  used by `consolidate_intersections`
- `rust/ducknx-core/tests/topology.rs` — Rust unit tests for the topology
  kernel

Python shim + wiring:
- `ducknx/_rust.py` (new) — `HAVE_RUST` flag + version-compatibility guard
  via `packaging.specifiers.SpecifierSet`
- `ducknx/simplification.py` — `_trace_paths` and the cluster-assignment
  block in `_consolidate_intersections_rebuild_graph` now take the Rust
  fast path when `HAVE_RUST` is `True`; the pure-Python fallback stays
  unchanged

Tests:
- `tests/test_rust_cluster_wkb.py` (new) — WKB round-trip tests on simple
  square / polygon-with-hole / multipolygon, auto-skipped when
  `ducknx_core` isn't installed

Build + distribution:
- `pyproject.toml` — `[fast]` extra now also pulls in
  `ducknx-core>=0.1.0,<0.2.0`; runtime `dependencies` add `packaging>=23`
  so the version-compat check in `_rust.py` always has its imports
- `.github/workflows/wheels.yml` (new) — cibuildwheel matrix
  (macOS arm64 + x86_64, Linux x86_64 manylinux2014, Linux aarch64,
  Windows x86_64) with an import smoke-test and Windows MSVC `cargo test`

## Summary

Optional Rust acceleration via PyO3. The `simplify_topology` and
`cluster_assign` kernels now have native implementations distributed as
the separate `ducknx-core` package and pulled in via
`pip install ducknx[fast]`. The pure-Python path remains the default and
is fully covered by the equivalence test suite — the `DUCKNX_DEBUG_NO_RUST`
environment switch in `ducknx/_rust.py` forces the Python fallback so the
equivalence sweep can run with the extension installed.

Hot kernels:
- `simplify_topology(succ_indptr, succ_indices, is_endpoint) -> (offsets, nodes_flat)`
  releases the GIL during the DFS so callers from a Python thread pool
  aren't blocked.
- `cluster_assign(node_xs, node_ys, cluster_polygons_wkb) -> cluster_idx`
  uses `rstar::RTree::bulk_load` + `geo::Contains` to do bounding-box
  pruning then exact point-in-polygon in a single allow-threads block.

The Rust side accepts shapely-emitted ISO WKB and is exercised by the
`test_rust_cluster_wkb.py` round-trip tests on shapes that historically
trip up WKB parsers (polygons with holes, multipolygons).

## Verification

### Build / unit / equivalence

NOT EXECUTED IN THIS WORKTREE — see "Known issues / next steps" below.
The host sandbox blocks `cargo`, `rustc`, `maturin`, `python3`, `pytest`,
and `uv` execution, so the following gates from the plan have NOT been
run locally:

- `cd rust/ducknx-core && cargo test` (Step 2.3c)
- `maturin develop` + Python smoke test (Step 2.3d, 2.4b)
- `pytest tests/test_rust_cluster_wkb.py -v` (Step 2.4c)
- `pytest tests/test_simplification_equivalence.py -v` with the extension
  installed (Step 2.5c)
- `DUCKNX_DEBUG_NO_RUST=1 pytest tests/test_simplification_equivalence.py -v`
  (Step 2.5d)
- `python benchmarks/bench_pipeline.py --track` on Berlin large bbox
  (Step 2.7a)
- The Phase B acceptance assertion `graph_simplify ≤ 10s and
  graph_build ≤ 6s` (Step 2.7b)

### Benchmark numbers

NOT MEASURED — same sandbox restriction. `berlin-latest.osm.pbf` is
present in this worktree (87 MB), so the moment cargo/python execution is
unblocked the implementer can:

```bash
cd rust/ducknx-core && maturin develop --release && cd ../..
python benchmarks/bench_pipeline.py --track
python benchmarks/compare.py
```

and fill in `large/graph_simplify`, `large/graph_build`, and the
`tracemalloc` peaks here.

## Distribution

Wheels built for (planned matrix, see `.github/workflows/wheels.yml`):
macOS arm64, macOS x86_64, Linux x86_64 (manylinux2014), Linux aarch64,
Windows x86_64. Each platform runs an import smoke-test on the produced
wheel; Windows additionally runs `cargo test --release` for MSVC sanity.

## Known issues / next steps

1. **Local verification deferred.** The agent sandbox in this worktree
   denies execution of `cargo`, `rustc`, `maturin`, `python3`, `pytest`,
   and `uv`. The Rust crate, the Python shim, the WKB round-trip tests,
   the `simplification.py` HAVE_RUST routing, the `[fast]` extra wiring,
   and the cibuildwheel matrix are all in place. The crate must be built
   with `maturin develop --release` and the test sweep + benchmark
   re-run in an environment where these tools are usable, before
   `duck_osmnx-1ni.2` is closed and Phase B is declared green.

2. **`wkb` crate API.** `cluster.rs` reads shapely WKB via
   `wkb::reader::read_wkb` with an `std::io::Cursor`. If the v0.7 API
   surface differs from what `lib.rs` expects, the local-build step will
   surface the symbol mismatch immediately and the fix is mechanical
   (swap to `geozero` or the `wkt` crate per plan note in Step 2.4c).
   The WKB round-trip tests in `tests/test_rust_cluster_wkb.py` are the
   designated catch for this.

3. **Phase A `_trace_paths` ordering.** The Rust implementation walks
   endpoints by ascending CSR row index, matching the Python
   `sorted(adj.osmid_to_idx[e] for e in endpoints)`. The equivalence
   suite in `tests/test_simplification_equivalence.py` snapshots full
   path output, which is how the implementer should verify byte-identical
   behavior between the two paths.

4. **Beads task left in `in_progress`** until verification numbers land
   in this changelog. Do NOT close `duck_osmnx-1ni.2` from this session.
