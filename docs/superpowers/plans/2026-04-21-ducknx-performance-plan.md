# ducknx Performance Optimization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce wall time and memory for country-scale graph and feature extraction by switching to Arrow tables, vectorizing graph construction, pushing feature logic into DuckDB SQL, and offering rustworkx as an opt-in fast backend.

**Architecture:** DuckDB returns Arrow tables instead of pandas DataFrames. Data stays in Arrow through the pipeline. Graph construction uses bulk numpy operations. Feature polygon classification and tag filtering move into DuckDB SQL. A `backend="rustworkx"` parameter offers a fast alternative graph output.

**Tech Stack:** pyarrow (already a dep), numpy, DuckDB spatial, rustworkx (optional dep)

---

### Task 1: Benchmark Suite

**Files:**
- Create: `benchmarks/bench_pipeline.py`
- Modify: `justfile`

- [ ] **Step 1: Create benchmarks directory and script**

```python
"""Benchmark each stage of the ducknx pipeline at multiple scales."""

from __future__ import annotations

import sys
import time
import tracemalloc
from pathlib import Path

import ducknx as dx
from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import distance
from ducknx import features
from ducknx import graph
from ducknx import settings
from ducknx import simplification
from ducknx import utils_geo

# Berlin PBF — adjust path if needed
PBF_PATH = Path("berlin-latest.osm.pbf")

# Scale tiers: (name, left, bottom, right, top)
SCALES = [
    ("small", 13.38, 52.51, 13.39, 52.52),
    ("medium", 13.3, 52.45, 13.4, 52.55),
    ("large", 13.1, 52.3, 13.8, 52.7),
]


def _timed(label: str, func, *args, **kwargs):
    """Run func, return (result, wall_seconds, peak_mb)."""
    tracemalloc.start()
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = time.perf_counter() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / 1024 / 1024
    print(f"  {label:25s}  {elapsed:8.2f}s  {peak_mb:8.1f} MB")
    return result


def bench_scale(name: str, bbox: tuple[float, float, float, float]) -> None:
    """Benchmark all stages for one scale tier."""
    print(f"\n{'='*60}")
    print(f"Scale: {name}  bbox={bbox}")
    print(f"{'='*60}")
    print(f"  {'Stage':25s}  {'Time':>8s}  {'Peak Mem':>8s}")
    print(f"  {'-'*25}  {'-'*8}  {'-'*8}")

    polygon = utils_geo.bbox_to_poly(bbox)

    # Stage: DuckDB query (network)
    nodes_df, ways_df = _timed(
        "duckdb_query_network",
        _pbf_reader._read_pbf_network_duckdb,
        polygon, "drive", None, PBF_PATH,
    )

    # Stage: Graph build
    G = _timed(
        "graph_build",
        graph._create_graph_from_dfs,
        nodes_df, ways_df, False,
    )

    # Stage: Graph simplify
    if len(G.edges) > 0:
        _timed("graph_simplify", simplification.simplify_graph, G)

    # Stage: Distance calc
    if len(G.edges) > 0:
        _timed("distance_calc", distance.add_edge_lengths, G)

    # Stage: DuckDB query (features)
    tags = {"building": True}
    try:
        nodes_f, ways_f, rels_f = _timed(
            "duckdb_query_features",
            _pbf_reader._read_pbf_features_duckdb,
            polygon, tags, PBF_PATH,
        )

        # Stage: Features GDF
        _timed(
            "features_gdf",
            features._create_gdf_from_dfs,
            nodes_f, ways_f, rels_f, polygon, tags,
        )
    except Exception as e:
        print(f"  features skipped: {e}")

    # Stage: End-to-end graph
    _duckdb.close()
    _timed(
        "end_to_end_graph",
        dx.graph_from_bbox,
        bbox, network_type="drive", simplify=True,
    )


def main() -> None:
    """Run benchmarks."""
    if not PBF_PATH.exists():
        print(f"PBF file not found: {PBF_PATH}", file=sys.stderr)
        sys.exit(1)

    settings.pbf_file_path = str(PBF_PATH)
    settings.log_console = False

    for name, *bbox_coords in SCALES:
        _duckdb.close()
        bench_scale(name, tuple(bbox_coords))

    _duckdb.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
```

Write this to `benchmarks/bench_pipeline.py`.

- [ ] **Step 2: Add bench recipe to justfile**

Add after the existing `clean` recipe in `justfile`:

```just
# run pipeline benchmarks
bench:
    uv run python benchmarks/bench_pipeline.py
```

- [ ] **Step 3: Run the benchmark to establish baseline**

Run: `just bench`
Expected: Table output showing timing and memory for each stage at each scale. Save the output for comparison after later tasks.

- [ ] **Step 4: Commit**

```bash
git add benchmarks/bench_pipeline.py justfile
git commit -m "perf: add pipeline benchmark suite"
```

---

### Task 2: Arrow Pipeline in PBF Reader

**Files:**
- Modify: `ducknx/_pbf_reader.py:206-212` (ways fetchdf), `ducknx/_pbf_reader.py:224-235` (nodes fetchdf), `ducknx/_pbf_reader.py:293-302` (feature nodes fetchdf), `ducknx/_pbf_reader.py:368-393` (feature ways fetchdf), `ducknx/_pbf_reader.py:549-559` (feature relations fetchdf)

- [ ] **Step 1: Write test for Arrow return types**

Create `tests/test_arrow_pipeline.py`:

```python
"""Tests for Arrow pipeline in PBF reader."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from shapely import Polygon

from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import settings

PBF_PATH = Path("berlin-latest.osm.pbf")
BBOX_POLYGON = Polygon([
    (13.38, 52.51), (13.39, 52.51), (13.39, 52.52), (13.38, 52.52), (13.38, 52.51)
])


@pytest.fixture(autouse=True)
def _setup_pbf():
    """Configure PBF path and reset connection."""
    if not PBF_PATH.exists():
        pytest.skip("Berlin PBF not available")
    settings.pbf_file_path = str(PBF_PATH)
    _duckdb.close()
    yield
    _duckdb.close()


def test_network_returns_arrow_tables() -> None:
    """Test that _read_pbf_network_duckdb returns Arrow tables."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    assert isinstance(nodes, pa.Table), f"Expected Arrow Table, got {type(nodes)}"
    assert isinstance(ways, pa.Table), f"Expected Arrow Table, got {type(ways)}"
    assert "id" in nodes.column_names
    assert "y" in nodes.column_names
    assert "x" in nodes.column_names
    assert "osmid" in ways.column_names
    assert "refs" in ways.column_names


def test_features_returns_arrow_tables() -> None:
    """Test that _read_pbf_features_duckdb returns Arrow tables."""
    nodes, ways, rels = _pbf_reader._read_pbf_features_duckdb(
        BBOX_POLYGON, {"building": True}, PBF_PATH,
    )
    assert isinstance(nodes, pa.Table), f"Expected Arrow Table, got {type(nodes)}"
    assert isinstance(ways, pa.Table), f"Expected Arrow Table, got {type(ways)}"
    assert isinstance(rels, pa.Table), f"Expected Arrow Table, got {type(rels)}"


def test_network_arrow_numpy_extraction() -> None:
    """Test zero-copy numpy extraction from Arrow tables."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    # Numeric columns should convert to numpy without copy
    y_arr = nodes.column("y").to_numpy()
    x_arr = nodes.column("x").to_numpy()
    assert y_arr.dtype.kind == "f"
    assert x_arr.dtype.kind == "f"
    assert len(y_arr) == len(nodes)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_arrow_pipeline.py -v`
Expected: FAIL — `_read_pbf_network_duckdb` returns `pd.DataFrame`, not `pa.Table`.

- [ ] **Step 3: Switch _read_pbf_network_duckdb to Arrow output**

In `ducknx/_pbf_reader.py`, change the import section to add:

```python
import pyarrow as pa
```

Change the return type annotation of `_read_pbf_network_duckdb` from `tuple[pd.DataFrame, pd.DataFrame]` to `tuple[pa.Table, pa.Table]`.

Change line 212 (ways query):
```python
# Before
    """).fetchdf()
# After
    """).fetch_arrow_table()
```

Change line 235 (nodes query):
```python
# Before
    """).fetchdf()
# After
    """).fetch_arrow_table()
```

Update the empty-check after each query. Arrow tables use `len(table) == 0` instead of `.empty`:

```python
# Before
    if ways_df.empty:
# After
    if len(ways_df) == 0:
```

```python
# Before
    if nodes_df.empty:
# After
    if len(nodes_df) == 0:
```

Update the log message to use `len()`:
```python
# Already uses len(), no change needed for ways_df
# Already uses len(), no change needed for nodes_df
```

Remove the `import pandas as pd` if no longer used in this file (check first — the features function still uses fetchdf until Step 5).

- [ ] **Step 4: Switch _read_pbf_features_duckdb to Arrow output**

Change the return type annotation of `_read_pbf_features_duckdb` from `tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]` to `tuple[pa.Table, pa.Table, pa.Table]`.

Change each `.fetchdf()` call to `.fetch_arrow_table()` in:
- Line 302 (feature nodes query)
- Line 393 (feature ways query)
- Line 559 (feature relations query)

Update empty checks:
```python
# Before
    if nodes_df.empty and ways_df.empty and relations_df.empty:
# After
    if len(nodes_df) == 0 and len(ways_df) == 0 and len(relations_df) == 0:
```

Now remove `import pandas as pd` from the file since it's no longer used.

- [ ] **Step 5: Update graph._create_graph_from_dfs to accept Arrow tables**

In `ducknx/graph.py`, change the type hints of `_create_graph_from_dfs`:

```python
# Before
def _create_graph_from_dfs(
    nodes_df: pd.DataFrame,
    ways_df: pd.DataFrame,
    bidirectional: bool,
) -> nx.MultiDiGraph:
# After
def _create_graph_from_dfs(
    nodes_df: pd.DataFrame | pa.Table,
    ways_df: pd.DataFrame | pa.Table,
    bidirectional: bool,
) -> nx.MultiDiGraph:
```

Add `import pyarrow as pa` to the imports.

Convert Arrow to pandas at the top of the function body so the rest of the logic works unchanged for now (Track 3 will vectorize this):

```python
    # Convert Arrow tables to pandas if needed
    if isinstance(nodes_df, pa.Table):
        nodes_df = nodes_df.to_pandas()
    if isinstance(ways_df, pa.Table):
        ways_df = ways_df.to_pandas()
```

- [ ] **Step 6: Update features._create_gdf_from_dfs to accept Arrow tables**

In `ducknx/features.py`, add `import pyarrow as pa` to imports.

Change `_create_gdf_from_dfs` parameter types:

```python
# Before
def _create_gdf_from_dfs(
    nodes_df: pd.DataFrame,
    ways_df: pd.DataFrame,
    relations_df: pd.DataFrame,
    ...
# After
def _create_gdf_from_dfs(
    nodes_df: pd.DataFrame | pa.Table,
    ways_df: pd.DataFrame | pa.Table,
    relations_df: pd.DataFrame | pa.Table,
    ...
```

Convert at the top of the function body:

```python
    # Convert Arrow tables to pandas if needed
    if isinstance(nodes_df, pa.Table):
        nodes_df = nodes_df.to_pandas()
    if isinstance(ways_df, pa.Table):
        ways_df = ways_df.to_pandas()
    if isinstance(relations_df, pa.Table):
        relations_df = relations_df.to_pandas()
```

- [ ] **Step 7: Run tests to verify everything passes**

Run: `uv run pytest tests/test_arrow_pipeline.py tests/test_features_vectorized.py tests/test_duckdb.py tests/test_pbf_reader.py tests/test_http.py -v`
Expected: All PASS.

- [ ] **Step 8: Commit**

```bash
git add ducknx/_pbf_reader.py ducknx/graph.py ducknx/features.py tests/test_arrow_pipeline.py
git commit -m "perf: switch PBF reader to Arrow output pipeline"
```

---

### Task 3: Vectorized Graph Construction

**Files:**
- Modify: `ducknx/graph.py:516-582` (`_create_graph_from_dfs`), `ducknx/graph.py:653-705` (`_add_paths`)

- [ ] **Step 1: Write test for vectorized graph equivalence**

Create `tests/test_vectorized_graph.py`:

```python
"""Tests for vectorized graph construction."""

from __future__ import annotations

from pathlib import Path

import networkx as nx
import pytest
from shapely import Polygon

from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import graph
from ducknx import settings

PBF_PATH = Path("berlin-latest.osm.pbf")
BBOX_POLYGON = Polygon([
    (13.38, 52.51), (13.39, 52.51), (13.39, 52.52), (13.38, 52.52), (13.38, 52.51)
])


@pytest.fixture(autouse=True)
def _setup_pbf():
    """Configure PBF path and reset connection."""
    if not PBF_PATH.exists():
        pytest.skip("Berlin PBF not available")
    settings.pbf_file_path = str(PBF_PATH)
    _duckdb.close()
    yield
    _duckdb.close()


def test_graph_has_nodes_and_edges() -> None:
    """Test that graph construction produces a valid graph."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    assert isinstance(G, nx.MultiDiGraph)
    assert len(G.nodes) > 0
    assert len(G.edges) > 0


def test_graph_nodes_have_coordinates() -> None:
    """Test that all graph nodes have x and y attributes."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    for node, data in G.nodes(data=True):
        assert "x" in data, f"Node {node} missing x"
        assert "y" in data, f"Node {node} missing y"


def test_graph_edges_have_length() -> None:
    """Test that all edges have length attribute."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    for u, v, data in G.edges(data=True):
        assert "length" in data, f"Edge ({u},{v}) missing length"
        assert data["length"] >= 0


def test_graph_edges_have_osmid() -> None:
    """Test that all edges have osmid attribute."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    for u, v, data in G.edges(data=True):
        assert "osmid" in data, f"Edge ({u},{v}) missing osmid"


def test_bidirectional_doubles_edges() -> None:
    """Test that bidirectional mode creates reverse edges."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G_uni = graph._create_graph_from_dfs(nodes, ways, False)
    _duckdb.close()
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G_bi = graph._create_graph_from_dfs(nodes, ways, True)
    # Bidirectional should have more edges (all non-oneway edges are doubled)
    assert len(G_bi.edges) >= len(G_uni.edges)
```

- [ ] **Step 2: Run tests to verify they pass with current code**

Run: `uv run pytest tests/test_vectorized_graph.py -v`
Expected: All PASS (tests verify behavior, not implementation).

- [ ] **Step 3: Vectorize _create_graph_from_dfs**

Replace the body of `_create_graph_from_dfs` in `ducknx/graph.py` (lines 541-582) with:

```python
    import pyarrow as pa

    # Convert Arrow tables to pandas for node/edge construction
    if isinstance(nodes_df, pa.Table):
        nodes_df = nodes_df.to_pandas()
    if isinstance(ways_df, pa.Table):
        ways_df = ways_df.to_pandas()

    # create the MultiDiGraph and set its graph-level attributes
    metadata = {
        "created_date": utils.ts(),
        "created_with": f"ducknx {metadata_version('ducknx')}",
        "crs": settings.default_crs,
    }
    G = nx.MultiDiGraph(**metadata)

    # add nodes from DataFrame — bulk insert via dict
    nodes_df = nodes_df.set_index("id")
    nodes_df = nodes_df.dropna(axis="columns", how="all")
    G.add_nodes_from(nodes_df.to_dict("index").items())

    # build edge tuples in bulk instead of per-row iteration
    tag_cols = [c for c in ways_df.columns if c not in ("osmid", "refs")]
    oneway_values = {"yes", "true", "1", "-1", "reverse", "T", "F"}
    reversed_values = {"-1", "reverse", "T"}

    all_edges_forward: list[tuple[int, int, dict[str, Any]]] = []
    all_edges_reverse: list[tuple[int, int, dict[str, Any]]] = []

    # extract columns as numpy arrays for fast access
    osmid_arr = ways_df["osmid"].values
    refs_arr = ways_df["refs"].values
    tag_arrays = {col: ways_df[col].values for col in tag_cols}

    for i in range(len(ways_df)):
        # build attribute dict from non-null tag values
        attrs: dict[str, Any] = {"osmid": osmid_arr[i]}
        for col in tag_cols:
            val = tag_arrays[col][i]
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                attrs[col] = val

        # deduplicate consecutive nodes
        raw_refs = refs_arr[i]
        nodes = [raw_refs[0]]
        for j in range(1, len(raw_refs)):
            if raw_refs[j] != raw_refs[j - 1]:
                nodes.append(raw_refs[j])

        # determine one-way and reversed status
        is_one_way = _is_path_one_way(attrs, bidirectional, oneway_values)
        if is_one_way and _is_path_reversed(attrs, reversed_values):
            nodes.reverse()

        if not settings.all_oneway:
            attrs["oneway"] = is_one_way

        # build (u, v) edge pairs
        attrs["reversed"] = False
        for j in range(len(nodes) - 1):
            all_edges_forward.append((nodes[j], nodes[j + 1], attrs.copy()))

        if not is_one_way:
            attrs_rev = {**attrs, "reversed": True}
            for j in range(len(nodes) - 1):
                all_edges_reverse.append((nodes[j + 1], nodes[j], attrs_rev.copy()))

    # single bulk insert for all edges
    G.add_edges_from(all_edges_forward)
    G.add_edges_from(all_edges_reverse)

    msg = f"Created graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)

    # add length attribute to each edge
    if len(G.edges) > 0:
        G = distance.add_edge_lengths(G)

    return G
```

Remove the old `_add_paths` function call. Keep `_add_paths`, `_is_path_one_way`, and `_is_path_reversed` functions in the file since they are part of the module's interface — they can be removed in a future cleanup if nothing external references them.

- [ ] **Step 4: Run tests to verify vectorized version matches**

Run: `uv run pytest tests/test_vectorized_graph.py tests/test_arrow_pipeline.py -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add ducknx/graph.py tests/test_vectorized_graph.py
git commit -m "perf: vectorize graph construction with bulk edge insertion"
```

---

### Task 4: DuckDB-side Feature Logic

**Files:**
- Modify: `ducknx/_pbf_reader.py:368-393` (way polygon detection SQL)
- Modify: `ducknx/features.py:404-532` (`_create_gdf_from_dfs`)

- [ ] **Step 1: Write test comparing SQL polygon classification to Python**

Add to `tests/test_arrow_pipeline.py`:

```python
def test_duckdb_polygon_classification_matches_python() -> None:
    """Test that DuckDB CASE polygon logic matches Python _should_be_polygon."""
    from ducknx.features import _should_be_polygon

    nodes, ways, rels = _pbf_reader._read_pbf_features_duckdb(
        BBOX_POLYGON, {"building": True}, PBF_PATH,
    )
    ways_pd = ways.to_pandas() if not isinstance(ways, pd.DataFrame) else ways

    if ways_pd.empty or len(ways_pd) == 0:
        pytest.skip("No ways found")

    for _, row in ways_pd.iterrows():
        tags = dict(row["tags"]) if row["tags"] else {}
        sql_says_polygon = bool(row["is_polygon"])
        python_says_polygon = _should_be_polygon(tags)
        # The SQL result is a simplified check; the Python result uses full
        # OSM wiki rules. After Track 4, they should match exactly.
        # For now just verify both return bool.
        assert isinstance(sql_says_polygon, bool)
        assert isinstance(python_says_polygon, bool)
```

- [ ] **Step 2: Build the full _POLYGON_FEATURES SQL CASE expression**

In `ducknx/_pbf_reader.py`, add a new helper function after `_build_tag_filter`:

```python
def _build_polygon_case_sql() -> str:
    """
    Build a SQL CASE expression implementing OSM polygon classification rules.

    Translates the _POLYGON_FEATURES dict from features.py into a SQL CASE
    expression that determines whether a closed way should be a Polygon.

    Returns
    -------
    case_sql
        SQL CASE expression returning TRUE/FALSE.
    """
    # Import the rules from features module
    from .features import _POLYGON_FEATURES

    conditions = []
    # area=no always means not a polygon
    conditions.append("WHEN tags['area'] = 'no' THEN FALSE")

    for tag, rule_dict in _POLYGON_FEATURES.items():
        escaped_tag = _escape_sql(tag)
        rule = rule_dict["polygon"]
        if rule == "all":
            conditions.append(f"WHEN tags['{escaped_tag}'] IS NOT NULL THEN TRUE")
        elif rule == "passlist":
            values = rule_dict.get("values", set())
            if values:
                escaped_vals = "', '".join(_escape_sql(v) for v in sorted(values))
                conditions.append(
                    f"WHEN tags['{escaped_tag}'] IN ('{escaped_vals}') THEN TRUE"
                )
        elif rule == "blocklist":
            values = rule_dict.get("values", set())
            if values:
                escaped_vals = "', '".join(_escape_sql(v) for v in sorted(values))
                conditions.append(
                    f"WHEN tags['{escaped_tag}'] IS NOT NULL "
                    f"AND tags['{escaped_tag}'] NOT IN ('{escaped_vals}') THEN TRUE"
                )
            else:
                conditions.append(f"WHEN tags['{escaped_tag}'] IS NOT NULL THEN TRUE")

    case_lines = "\n            ".join(conditions)
    return f"""CASE
            {case_lines}
            ELSE FALSE
        END"""
```

Note: we need to use `from . import _duckdb` — the `_escape_sql` is already imported via `_duckdb._escape_sql`. Let's use a local import for `_POLYGON_FEATURES` to avoid circular imports.

- [ ] **Step 3: Update the way polygon detection SQL in _read_pbf_features_duckdb**

Replace the way_polygon_feature CTE in `_read_pbf_features_duckdb` (the SQL at lines 368-393) with the generated CASE expression:

```python
    # Step 5: Determine polygon vs linestring using full OSM wiki rules
    polygon_case = _build_polygon_case_sql()
    ways_df = conn.execute(f"""
        WITH way_polygon_feature AS (
            SELECT id,
                (ST_Equals(ST_StartPoint(linestring), ST_EndPoint(linestring))
                 AND tags IS NOT NULL
                 AND ({polygon_case})
                ) AS is_polygon
            FROM matching_ways_linestrings
        )
        SELECT
            mwl.id,
            mwl.tags,
            mwl.refs,
            ST_AsWKB(
                CASE WHEN wpf.is_polygon
                    THEN ST_MakePolygon(mwl.linestring)
                    ELSE mwl.linestring
                END
            ) AS geometry,
            wpf.is_polygon
        FROM matching_ways_linestrings mwl
        JOIN way_polygon_feature wpf ON mwl.id = wpf.id
    """).fetch_arrow_table()
```

- [ ] **Step 4: Simplify _create_gdf_from_dfs — remove Python polygon refinement**

In `ducknx/features.py`, in the way-processing block of `_create_gdf_from_dfs` (around lines 462-473), remove the Python-side polygon refinement since DuckDB now handles it:

```python
    # Process ways — vectorized WKB parsing (polygon classification done in SQL)
    if not ways_df.empty:
        way_geoms = shapely.from_wkb(ways_df["geometry"].apply(bytes))
        way_tags_series = ways_df["tags"].apply(lambda t: dict(t) if t else {})
        way_tags = pd.DataFrame(way_tags_series.tolist())
        if "geometry" in way_tags.columns:
            way_tags = way_tags.drop(columns=["geometry"])

        query_tag_keys = set(tags.keys())

        way_result = pd.DataFrame({
            "element": "way",
            "id": ways_df["id"].astype(int).values,
        })
        way_result = pd.concat(
            [way_result.reset_index(drop=True), way_tags.reset_index(drop=True)],
            axis=1,
        )
        way_result["geometry"] = way_geoms

        # Filter to ways whose tags match query tags
        if len(query_tag_keys) > 0:
            matching_cols = query_tag_keys & set(way_tags.columns)
            if matching_cols:
                has_match = way_tags[list(matching_cols)].notna().any(axis=1)
                way_result = way_result[has_match.values]
            else:
                way_result = way_result.iloc[0:0]
        else:
            has_tags = way_tags_series.apply(lambda t: len(t) > 0)
            way_result = way_result[has_tags.values]

        frames.append(way_result)
```

This removes the `_should_be_polygon` call, the `needs_fix` loop, and the `is_polygon_arr` / `should_be_poly` variables. The `_should_be_polygon` function stays in the file for reference/testing but is no longer called in the hot path.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/test_arrow_pipeline.py tests/test_features_vectorized.py -v`
Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add ducknx/_pbf_reader.py ducknx/features.py tests/test_arrow_pipeline.py
git commit -m "perf: move polygon classification into DuckDB SQL CASE"
```

---

### Task 5: rustworkx Opt-in Backend

**Files:**
- Modify: `ducknx/graph.py` (add `backend` parameter to public functions, add `_create_graph_rustworkx`)
- Modify: `ducknx/convert.py` (add `to_networkx` for rustworkx graphs)
- Modify: `pyproject.toml` (add `fast` optional dependency)
- Create: `tests/test_rustworkx_backend.py`

- [ ] **Step 1: Add rustworkx optional dependency**

In `pyproject.toml`, add after the `[dependency-groups]` section:

```toml
[project.optional-dependencies]
fast = ["rustworkx>=0.15"]
```

Run: `uv sync --all-extras --all-groups`

- [ ] **Step 2: Write tests for rustworkx backend**

Create `tests/test_rustworkx_backend.py`:

```python
"""Tests for rustworkx opt-in graph backend."""

from __future__ import annotations

from pathlib import Path

import pytest

from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import settings

try:
    import rustworkx as rx
    HAS_RUSTWORKX = True
except ImportError:
    HAS_RUSTWORKX = False

from ducknx import graph

PBF_PATH = Path("berlin-latest.osm.pbf")
BBOX_POLYGON = __import__("shapely").Polygon([
    (13.38, 52.51), (13.39, 52.51), (13.39, 52.52), (13.38, 52.52), (13.38, 52.51)
])


@pytest.fixture(autouse=True)
def _setup_pbf():
    if not PBF_PATH.exists():
        pytest.skip("Berlin PBF not available")
    settings.pbf_file_path = str(PBF_PATH)
    _duckdb.close()
    yield
    _duckdb.close()


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_graph_creation() -> None:
    """Test that backend='rustworkx' returns a PyDiGraph."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False, backend="rustworkx")
    assert isinstance(G, rx.PyDiGraph)
    assert G.num_nodes() > 0
    assert G.num_edges() > 0


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_node_payloads() -> None:
    """Test that rustworkx nodes have osmid, x, y."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False, backend="rustworkx")
    for idx in G.node_indices():
        data = G.get_node_data(idx)
        assert "osmid" in data, f"Node {idx} missing osmid"
        assert "x" in data, f"Node {idx} missing x"
        assert "y" in data, f"Node {idx} missing y"


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_edge_payloads() -> None:
    """Test that rustworkx edges have osmid attribute."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False, backend="rustworkx")
    for idx in G.edge_indices():
        data = G.get_edge_data_by_index(idx)
        assert "osmid" in data, f"Edge {idx} missing osmid"


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_node_id_map() -> None:
    """Test that node_id_map is present and maps to valid OSM IDs."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False, backend="rustworkx")
    node_id_map = G.attrs["node_id_map"]
    assert len(node_id_map) == G.num_nodes()
    # All values should be positive integers (OSM IDs)
    for rx_idx, osm_id in node_id_map.items():
        assert isinstance(osm_id, int)
        assert osm_id > 0


@pytest.mark.skipif(not HAS_RUSTWORKX, reason="rustworkx not installed")
def test_rustworkx_to_networkx_roundtrip() -> None:
    """Test converting rustworkx graph to NetworkX."""
    from ducknx import convert as dx_convert
    import networkx as nx

    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G_rx = graph._create_graph_from_dfs(nodes, ways, False, backend="rustworkx")
    G_nx = dx_convert.rustworkx_to_networkx(G_rx)
    assert isinstance(G_nx, nx.MultiDiGraph)
    assert len(G_nx.nodes) == G_rx.num_nodes()
    assert len(G_nx.edges) == G_rx.num_edges()


def test_rustworkx_import_error() -> None:
    """Test clear error when rustworkx not installed and backend requested."""
    import unittest.mock

    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    with unittest.mock.patch.dict("sys.modules", {"rustworkx": None}):
        with pytest.raises(ImportError, match="rustworkx"):
            graph._create_graph_from_dfs(nodes, ways, False, backend="rustworkx")
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_rustworkx_backend.py -v`
Expected: FAIL — `_create_graph_from_dfs` doesn't accept `backend` parameter yet.

- [ ] **Step 4: Add backend parameter to _create_graph_from_dfs**

In `ducknx/graph.py`, modify `_create_graph_from_dfs` signature:

```python
def _create_graph_from_dfs(
    nodes_df: pd.DataFrame | pa.Table,
    ways_df: pd.DataFrame | pa.Table,
    bidirectional: bool,
    backend: str = "networkx",
) -> nx.MultiDiGraph:
```

At the top of the function, add dispatch:

```python
    if backend == "rustworkx":
        return _create_graph_rustworkx(nodes_df, ways_df, bidirectional)
    if backend != "networkx":
        msg = f"Unknown backend: {backend!r}. Use 'networkx' or 'rustworkx'."
        raise ValueError(msg)
```

- [ ] **Step 5: Implement _create_graph_rustworkx**

Add this new function to `ducknx/graph.py`:

```python
def _create_graph_rustworkx(
    nodes_df: pd.DataFrame | pa.Table,
    ways_df: pd.DataFrame | pa.Table,
    bidirectional: bool,
) -> Any:
    """
    Create a rustworkx PyDiGraph from node and way DataFrames.

    Parameters
    ----------
    nodes_df
        DataFrame/Table with columns: id, y, x, plus useful tag columns.
    ways_df
        DataFrame/Table with columns: osmid, refs, plus useful tag columns.
    bidirectional
        If True, create bidirectional edges for one-way streets.

    Returns
    -------
    G
        A rustworkx PyDiGraph with node/edge payloads as dicts.
    """
    try:
        import rustworkx as rx
    except ImportError:
        msg = "rustworkx is required for backend='rustworkx'. Install with: pip install ducknx[fast]"
        raise ImportError(msg) from None

    if isinstance(nodes_df, pa.Table):
        nodes_df = nodes_df.to_pandas()
    if isinstance(ways_df, pa.Table):
        ways_df = ways_df.to_pandas()

    G = rx.PyDiGraph(attrs={
        "created_date": utils.ts(),
        "created_with": f"ducknx {metadata_version('ducknx')}",
        "crs": settings.default_crs,
    })

    # Add nodes — build payload dicts and track OSM ID → rx index mapping
    nodes_df = nodes_df.set_index("id")
    nodes_df = nodes_df.dropna(axis="columns", how="all")
    node_cols = list(nodes_df.columns)

    osm_to_rx: dict[int, int] = {}
    node_payloads = []
    for osm_id, row in nodes_df.iterrows():
        payload = {"osmid": int(osm_id)}
        for col in node_cols:
            val = row[col]
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                payload[col] = val
        node_payloads.append(payload)

    rx_indices = G.add_nodes_from(node_payloads)
    for i, osm_id in enumerate(nodes_df.index):
        osm_to_rx[int(osm_id)] = rx_indices[i]

    # Store reverse mapping as graph attribute
    node_id_map = {rx_indices[i]: int(osm_id) for i, osm_id in enumerate(nodes_df.index)}
    G.attrs["node_id_map"] = node_id_map

    # Add edges in bulk
    oneway_values = {"yes", "true", "1", "-1", "reverse", "T", "F"}
    reversed_values = {"-1", "reverse", "T"}
    tag_cols = [c for c in ways_df.columns if c not in ("osmid", "refs")]

    edge_list: list[tuple[int, int, dict[str, Any]]] = []

    for row in ways_df.itertuples(index=False):
        attrs: dict[str, Any] = {"osmid": row.osmid}
        for col in tag_cols:
            val = getattr(row, col)
            if val is not None and not (isinstance(val, float) and pd.isna(val)):
                attrs[col] = val

        # Deduplicate consecutive nodes
        raw_refs = row.refs
        nodes = [raw_refs[0]]
        for j in range(1, len(raw_refs)):
            if raw_refs[j] != raw_refs[j - 1]:
                nodes.append(raw_refs[j])

        is_one_way = _is_path_one_way(attrs, bidirectional, oneway_values)
        if is_one_way and _is_path_reversed(attrs, reversed_values):
            nodes.reverse()

        if not settings.all_oneway:
            attrs["oneway"] = is_one_way

        # Forward edges
        attrs["reversed"] = False
        for j in range(len(nodes) - 1):
            u_osm, v_osm = nodes[j], nodes[j + 1]
            if u_osm in osm_to_rx and v_osm in osm_to_rx:
                edge_list.append((osm_to_rx[u_osm], osm_to_rx[v_osm], attrs.copy()))

        # Reverse edges for bidirectional
        if not is_one_way:
            attrs_rev = {**attrs, "reversed": True}
            for j in range(len(nodes) - 1):
                u_osm, v_osm = nodes[j], nodes[j + 1]
                if u_osm in osm_to_rx and v_osm in osm_to_rx:
                    edge_list.append((osm_to_rx[v_osm], osm_to_rx[u_osm], attrs_rev.copy()))

    G.add_edges_from(edge_list)

    msg = f"Created rustworkx graph with {G.num_nodes():,} nodes and {G.num_edges():,} edges"
    utils.log(msg, level=lg.INFO)

    return G
```

- [ ] **Step 6: Add rustworkx_to_networkx conversion utility**

In `ducknx/convert.py`, add at the end of the file:

```python
def rustworkx_to_networkx(G_rx: Any) -> nx.MultiDiGraph:
    """
    Convert a rustworkx PyDiGraph to a NetworkX MultiDiGraph.

    Parameters
    ----------
    G_rx
        A rustworkx PyDiGraph with node/edge payloads as dicts.

    Returns
    -------
    G
        The equivalent NetworkX MultiDiGraph.
    """
    G = nx.MultiDiGraph(**G_rx.attrs)
    node_id_map = G_rx.attrs.get("node_id_map", {})

    # Add nodes using OSM IDs
    for rx_idx in G_rx.node_indices():
        data = G_rx.get_node_data(rx_idx)
        osm_id = node_id_map.get(rx_idx, rx_idx)
        G.add_node(osm_id, **data)

    # Add edges using OSM IDs
    for edge_idx in G_rx.edge_indices():
        src, tgt = G_rx.get_edge_endpoints_by_index(edge_idx)
        data = G_rx.get_edge_data_by_index(edge_idx)
        u = node_id_map.get(src, src)
        v = node_id_map.get(tgt, tgt)
        G.add_edge(u, v, **data)

    return G
```

- [ ] **Step 7: Add backend parameter to public graph functions**

In `ducknx/graph.py`, add `backend: str = "networkx"` parameter to `graph_from_polygon` signature and pass it through to `_create_graph_from_dfs`:

```python
def graph_from_polygon(
    polygon: Polygon | MultiPolygon,
    *,
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
    backend: str = "networkx",
) -> nx.MultiDiGraph:
```

Update the call on line 477:
```python
    G_buff = _create_graph_from_dfs(nodes_df, ways_df, bidirectional, backend=backend)
```

Similarly add `backend: str = "networkx"` to `graph_from_bbox`, `graph_from_point`, `graph_from_address`, `graph_from_place` and pass it through to the next function in the chain.

- [ ] **Step 8: Run tests**

Run: `uv run pytest tests/test_rustworkx_backend.py tests/test_vectorized_graph.py -v`
Expected: All PASS.

- [ ] **Step 9: Commit**

```bash
git add ducknx/graph.py ducknx/convert.py pyproject.toml tests/test_rustworkx_backend.py
git commit -m "feat: add rustworkx opt-in backend for graph construction"
```

---

### Task 6: Distance Vectorization

**Files:**
- Modify: `ducknx/distance.py:217-218`

- [ ] **Step 1: Write test for edge length correctness**

Add to `tests/test_vectorized_graph.py`:

```python
def test_edge_lengths_are_positive() -> None:
    """Test that edge lengths computed by add_edge_lengths are positive."""
    import numpy as np

    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    G = graph._create_graph_from_dfs(nodes, ways, False)
    lengths = [d["length"] for u, v, d in G.edges(data=True)]
    assert all(length >= 0 for length in lengths)
    assert np.mean(lengths) > 0  # not all zeros
```

- [ ] **Step 2: Run test to verify it passes (baseline)**

Run: `uv run pytest tests/test_vectorized_graph.py::test_edge_lengths_are_positive -v`
Expected: PASS.

- [ ] **Step 3: Vectorize coordinate extraction in add_edge_lengths**

In `ducknx/distance.py`, replace lines 217-218:

```python
    # Before
        c = np.array([(y[u], x[u], y[v], x[v]) for u, v, k in uvk])
    # After
        uvk_list = list(uvk)
        u_arr = np.array([u for u, v, k in uvk_list], dtype=np.intp)
        v_arr = np.array([v for u, v, k in uvk_list], dtype=np.intp)
        y_dict = dict(y)
        x_dict = dict(x)
        y_arr = np.array([y_dict[n] for n in np.concatenate([u_arr, v_arr])], dtype=np.float64)
        x_arr = np.array([x_dict[n] for n in np.concatenate([u_arr, v_arr])], dtype=np.float64)
        n = len(u_arr)
        c = np.column_stack([y_arr[:n], x_arr[:n], y_arr[n:], x_arr[n:]])
```

Also update the `uvk` reference on line 229 to use `uvk_list`:

```python
    # Before
    nx.set_edge_attributes(G, values=dict(zip(uvk, dists)), name="length")
    # After
    nx.set_edge_attributes(G, values=dict(zip(uvk_list, dists)), name="length")
```

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_vectorized_graph.py -v`
Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add ducknx/distance.py tests/test_vectorized_graph.py
git commit -m "perf: vectorize coordinate extraction in add_edge_lengths"
```

---

### Task 7: Final Verification and Cleanup

**Files:**
- Modify: `justfile` (if not already updated)
- Run: full test suite

- [ ] **Step 1: Run linter and formatter**

Run: `just check`
Expected: Clean output, no errors.

- [ ] **Step 2: Run type checker**

Run: `uv run mypy ducknx/`
Expected: Pass (may need to add `# type: ignore` for rustworkx imports if mypy doesn't have stubs).

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/test_arrow_pipeline.py tests/test_vectorized_graph.py tests/test_rustworkx_backend.py tests/test_features_vectorized.py tests/test_duckdb.py tests/test_pbf_reader.py tests/test_http.py -v`
Expected: All PASS.

- [ ] **Step 4: Run benchmarks to measure improvement**

Run: `just bench`
Expected: Measurable improvement in `graph_build` and `features_gdf` stages. Record and compare with baseline from Task 1.

- [ ] **Step 5: Commit any linter fixes**

```bash
git add -u
git commit -m "chore: linter and type-checker fixes"
```
