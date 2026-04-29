# ducknx Performance Optimization — Design Spec

**Date:** 2026-04-21
**Status:** Approved
**Scope:** Arrow pipeline, vectorized graph construction, DuckDB-side feature logic, rustworkx opt-in backend

## Problem

ducknx works well at city scale but becomes slow at region/country scale (hundreds of thousands to millions of ways/nodes). Most heavy lifting is already delegated to native code (DuckDB, Shapely, numpy), but the Python-side graph construction, feature post-processing, and data marshalling have unnecessary overhead.

## Goals

- Reduce wall time for country-scale `graph_from_*()` and `features_from_*()` calls
- Reduce peak memory by avoiding redundant data copies
- No breaking changes to public API — all optimizations either apply to the default path or are opt-in
- Establish a benchmark suite to measure and validate improvements

## Non-Goals

- Porting simplification or routing to rustworkx (deferred until benchmarks justify it)
- Custom Rust extensions via PyO3/maturin (deferred — use existing compiled libraries first)
- Async HTTP for Nominatim (I/O-bound, not CPU-bound; OSM rate limits dominate)
- Replacing NetworkX as the default graph output

## Design Decisions

- **Profile before optimizing.** A benchmark suite runs first to establish baselines and confirm where wall time actually goes. All subsequent tracks are validated against these baselines.
- **Arrow as internal data format.** DuckDB returns Arrow tables via `fetch_arrow_table()`. Data stays in Arrow through the pipeline. Pandas/GeoDataFrame materialized only at API boundaries. `pyarrow` is already a dependency.
- **Vectorize before adding dependencies.** Graph construction is optimized with numpy bulk operations and single `add_edges_from()` calls before reaching for rustworkx. This improves the default NetworkX path for all users.
- **Push computation into DuckDB.** Feature construction logic (`_should_be_polygon()`, tag filtering, geometry validation) moves into SQL rather than being optimized in Python. DuckDB already has the data loaded; SQL is faster than Python post-processing.
- **rustworkx as opt-in, not internal.** Instead of using rustworkx internally and converting to NetworkX (which negates the speedup), offer `backend="rustworkx"` that returns a rustworkx graph directly. Users who want speed use rustworkx end-to-end. No conversion tax.
- **Optional dependency.** rustworkx is not required. It's an optional extra (`ducknx[fast]`). Missing import raises a clear error with install instructions.

## Architecture

### Data Flow (default NetworkX path, after optimization)

```
PBF file
  → DuckDB SQL (spatial filter, tag filter, geometry construction, polygon classification, validation)
  → Arrow Table (zero-copy from DuckDB)
  → numpy arrays (zero-copy from Arrow for numeric columns)
  → NetworkX MultiDiGraph (single batch add_edges_from())
```

### Data Flow (rustworkx opt-in path)

```
PBF file
  → DuckDB SQL (same as above)
  → Arrow Table
  → numpy arrays
  → rx.PyDiGraph (batch add_nodes_from() + add_edges_from(), Rust-speed)
```

### Data Flow (features path)

```
PBF file
  → DuckDB SQL (tag filtering in WHERE, _should_be_polygon as CASE, ST_MakeValid)
  → Arrow Table (clean, correctly-typed, pre-filtered geometries)
  → pandas DataFrame (materialized from Arrow)
  → GeoDataFrame (Shapely geometries from WKB)
```

## Track 1: Benchmark Suite

**File:** `benchmarks/bench_pipeline.py`

Measures each pipeline stage independently:

| Stage | What it covers |
|---|---|
| `duckdb_query` | SQL execution + Arrow/pandas fetch |
| `graph_build` | `_create_graph_from_dfs()` |
| `graph_simplify` | `simplify_graph()` |
| `features_gdf` | `_create_gdf_from_dfs()` |
| `distance_calc` | `add_edge_lengths()` |
| `end_to_end` | Full `graph_from_bbox()` / `features_from_bbox()` |

Scale tiers using the Berlin PBF:

| Tier | Bbox size | Approximate data |
|---|---|---|
| Small | ~0.01 deg | Hundreds of nodes |
| Medium | ~0.1 deg | Tens of thousands of nodes |
| Large | Full PBF extent | Hundreds of thousands of nodes |

Instruments with `time.perf_counter()` for wall time and `tracemalloc` for peak memory. Prints a table to stdout. Added to justfile as `just bench`.

## Track 2: Arrow Pipeline

**Files modified:** `ducknx/_pbf_reader.py`

### Changes

- `_read_pbf_network_duckdb()`: Switch `conn.execute(...).fetchdf()` to `conn.execute(...).fetch_arrow_table()`. Return type changes from `pd.DataFrame` to `pyarrow.Table`.
- `_read_pbf_features_duckdb()`: Same change.
- These are internal functions. Public API is unaffected.

### Downstream impact

- `graph._create_graph_from_dfs()` receives Arrow tables, extracts numpy arrays via `.to_numpy()` (zero-copy for numeric columns).
- `features._create_gdf_from_dfs()` receives Arrow tables, materializes to pandas right before `GeoDataFrame()` constructor.
- `distance.add_edge_lengths()` is unaffected (works on graph data, not DataFrames).

### Risk

Low. `pyarrow` is already a dependency. Main risk is subtle dtype differences (Arrow string vs pandas object) caught by existing tests.

## Track 3: Vectorized Graph Construction

**Files modified:** `ducknx/graph.py`

### Current bottleneck

`_create_graph_from_dfs()` (lines 516-582) iterates rows with `itertuples()`, deduplicates consecutive nodes with Python `groupby()`, extracts tags per-row, and calls `_add_paths()` per-way which loops over node pairs.

### Changes

1. **Consecutive node deduplication:** Replace Python `groupby()` with numpy shift-and-compare on the refs arrays.
2. **Bulk edge pair construction:** Build all `(u, v)` pairs across all ways as numpy arrays, then call `G.add_edges_from()` once.
3. **Vectorized tag extraction:** Extract full tag columns from Arrow table as numpy arrays. Build attribute dicts using column-wise slicing instead of per-row `getattr()`.
4. **Vectorized one-way/reverse detection:** Extract `oneway`, `junction`, etc. columns as arrays. Compute boolean masks across all ways at once instead of per-path dict lookups in `_is_path_one_way()` / `_is_path_reversed()`.

### Expected improvement

2-3x for graph construction. Single `add_edges_from()` reduces Python→C overhead from O(edges) to O(1).

### What stays the same

NetworkX `MultiDiGraph` as output. Public API unchanged. `_add_paths()` is replaced by the bulk edge construction logic but produces identical graph structure.

## Track 4: DuckDB-side Feature Logic

**Files modified:** `ducknx/_pbf_reader.py`, `ducknx/features.py`

### Changes to `_read_pbf_features_duckdb()`

1. **Polygon classification in SQL:** Encode `_should_be_polygon()` rules as a SQL CASE expression:

```sql
CASE
  WHEN tags['area'] = 'yes' THEN ST_MakePolygon(geom)
  WHEN tags['area'] = 'no' THEN geom
  WHEN tags['building'] IS NOT NULL THEN ST_MakePolygon(geom)
  WHEN tags['landuse'] IS NOT NULL THEN ST_MakePolygon(geom)
  -- remaining OSM wiki polygon rules
  ELSE geom
END AS geometry
```

2. **Tag filtering in WHERE:** Move tag filter conditions from Python `_filter_features()` into the SQL WHERE clause. DuckDB returns only matching rows.

3. **Geometry validation in SQL:** Apply `ST_MakeValid()` in the SELECT before returning results.

### Changes to `features._create_gdf_from_dfs()`

- Receives pre-filtered, correctly-typed, validated geometries from DuckDB
- `_should_be_polygon()` function is no longer called (logic is in SQL)
- `_filter_features()` is simplified (spatial clipping only, tag filtering already done)
- WKB → Shapely conversion remains (GeoDataFrame needs Shapely objects)

### Risk

Medium. The `_should_be_polygon()` rules are complex (30+ tag checks from OSM wiki). SQL CASE translation must be exact. Validated by comparing output against the current Python implementation on test data.

## Track 5: rustworkx Opt-in Backend

**Files modified:** `ducknx/graph.py`, `ducknx/convert.py`, `pyproject.toml`

### API

```python
# Default (unchanged)
G = dx.graph_from_bbox(bbox, network_type="drive")  # nx.MultiDiGraph

# Opt-in fast path
G = dx.graph_from_bbox(bbox, network_type="drive", backend="rustworkx")  # rx.PyDiGraph
```

`backend` parameter added to: `graph_from_bbox()`, `graph_from_point()`, `graph_from_address()`, `graph_from_place()`, `graph_from_polygon()`.

### rustworkx graph structure

- **Nodes:** Payload is `{"osmid": int, "x": float, "y": float, **tags}`
- **Edges:** Payload is `{"osmid": int, "length": float, "oneway": bool, **tags}` (same attributes as NetworkX edge data)
- **Graph attribute:** `G.attrs["node_id_map"]` — dict mapping rustworkx index → OSM node ID
- **Graph attribute:** `G.attrs["crs"]` — CRS string (e.g., `"epsg:4326"`)

### Construction

Uses `rx.PyDiGraph.add_nodes_from()` and `rx.PyDiGraph.add_edges_from_no_data()` / `add_edges_from()` for batch insertion at Rust speed.

### Conversion utility

`dx.convert.to_networkx(rx_graph)` → `nx.MultiDiGraph`. Iterates nodes and edges once. For users who want fast construction then NetworkX algorithms.

### Dependency

```toml
[project.optional-dependencies]
fast = ["rustworkx>=0.15"]
```

Import guarded:
```python
try:
    import rustworkx as rx
except ImportError:
    msg = "rustworkx is required for backend='rustworkx'. Install with: pip install ducknx[fast]"
    raise ImportError(msg)
```

### What is NOT ported to rustworkx

- `simplification.py` — deep NetworkX API coupling, deferred
- `routing.py` — stays NetworkX-only
- `plot.py`, `stats.py`, `io.py`, `truncate.py` — stay NetworkX-only
- Users on the rustworkx path use rustworkx's own algorithm library for downstream analysis

## Track 6: Distance Vectorization

**File modified:** `ducknx/distance.py`

### Change

Replace list comprehension at line 218:

```python
# Before
c = np.array([(y[u], x[u], y[v], x[v]) for u, v, k in uvk])

# After
u_arr = np.fromiter((u for u, v, k in uvk), dtype=np.intp, count=len(uvk))
v_arr = np.fromiter((v for u, v, k in uvk), dtype=np.intp, count=len(uvk))
c = np.column_stack([y[u_arr], x[u_arr], y[v_arr], x[v_arr]])
```

Numpy does the coordinate indexing in C instead of building Python tuples. `great_circle()` is already vectorized.

### Expected improvement

Significant at country scale (100k+ edges). Minimal at city scale.

## Implementation Order

1. **Track 1:** Benchmark suite — establishes baseline
2. **Track 2:** Arrow pipeline — low risk, enables tracks 3-4
3. **Track 3:** Vectorized graph construction — biggest default-path win
4. **Track 4:** DuckDB-side feature logic — biggest features-path win
5. **Track 5:** rustworkx opt-in backend — opt-in, independent of tracks 2-4
6. **Track 6:** Distance vectorization — small targeted fix

Re-benchmark after each track to validate improvement and inform whether the next track is worth pursuing.

## Testing

### Existing tests (no changes)

All tests in `test_osmnx.py`, `test_http.py`, `test_duckdb.py`, `test_pbf_reader.py`, `test_features_vectorized.py` continue to pass.

### New tests

| File | Coverage |
|---|---|
| `tests/test_arrow_pipeline.py` | Arrow table dtypes, zero-copy numpy extraction, round-trip correctness |
| `tests/test_rustworkx_backend.py` | Graph construction, node/edge payloads, `to_networkx()` round-trip |
| `tests/test_vectorized_graph.py` | Batch edge construction matches row-by-row, vectorized one-way detection |

### Integration

Extend `test_osmnx.py` with parameterized `backend` fixture for key graph tests to validate both NetworkX and rustworkx paths produce equivalent results.

### Validation of DuckDB-side polygon logic

Compare DuckDB CASE expression output against current Python `_should_be_polygon()` on the full Berlin PBF test data. Every way must produce the same geometry type.

## Out of Scope

- Porting simplification/routing to rustworkx
- Custom Rust extensions (PyO3/maturin)
- Async HTTP for Nominatim
- Replacing NetworkX as default output
- Polars integration (Arrow is sufficient)
- Multi-threading (DuckDB is single-threaded by design)
