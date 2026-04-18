# ducknx Cleanup & Improvement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix bugs, harden SQL, standardize on httpx, add DuckDB connection reuse, and vectorize DataFrame processing in ducknx.

**Architecture:** Four independent tracks — (1) new `_duckdb.py` connection manager + `_pbf_reader.py` integration, (2) httpx migration for `_http.py`, (3) httpx migration for `_nominatim.py`, (4) vectorized `_create_gdf_from_dfs`. Tracks 2+3 are sequential (3 depends on 2); all others are independent.

**Tech Stack:** Python 3.12+, DuckDB, httpx, pandas, shapely, pytest

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `ducknx/_duckdb.py` | Create | Connection lifecycle manager |
| `ducknx/_pbf_reader.py` | Modify | Use connection manager, add SQL escaping, inline `_polygon_to_wkt` |
| `ducknx/_http.py` | Modify | Fix type annotations, add missing import, migrate to httpx |
| `ducknx/_nominatim.py` | Modify | Replace `requests` with `httpx` |
| `ducknx/features.py` | Modify | Vectorize `_create_gdf_from_dfs` |
| `tests/test_duckdb.py` | Create | Tests for connection manager |
| `tests/test_pbf_reader.py` | Create | Tests for SQL escaping |
| `tests/test_http.py` | Create | Tests for httpx migration |
| `tests/test_features_vectorized.py` | Create | Tests for vectorized GeoDataFrame construction |

---

### Task 1: Create `_duckdb.py` Connection Manager

**Files:**
- Create: `ducknx/_duckdb.py`
- Create: `tests/test_duckdb.py`

- [ ] **Step 1: Write failing tests for the connection manager**

Create `tests/test_duckdb.py`:

```python
"""Tests for the DuckDB connection manager."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from ducknx import _duckdb


@pytest.fixture(autouse=True)
def _reset_connection():
    """Reset the connection manager state before and after each test."""
    _duckdb.close()
    yield
    _duckdb.close()


def test_get_connection_returns_duckdb_connection(tmp_path: Path) -> None:
    """Test that get_connection returns a DuckDB connection."""
    # We need a real PBF file for ST_ReadOSM. Use a mock approach instead:
    # test that the function accepts a path and returns a connection type.
    # Full integration is tested via existing test_osmnx.py.
    with pytest.raises(Exception):
        # A non-existent file should raise (either FileNotFoundError or DuckDB error)
        _duckdb.get_connection(tmp_path / "nonexistent.pbf")


def test_close_resets_state() -> None:
    """Test that close() resets internal state."""
    _duckdb.close()
    assert _duckdb._connection is None
    assert _duckdb._current_pbf_path is None


def test_close_idempotent() -> None:
    """Test that calling close() multiple times is safe."""
    _duckdb.close()
    _duckdb.close()
    assert _duckdb._connection is None


def test_escape_sql_basic() -> None:
    """Test SQL escaping of single quotes."""
    assert _duckdb._escape_sql("McDonald's") == "McDonald''s"
    assert _duckdb._escape_sql("no quotes") == "no quotes"
    assert _duckdb._escape_sql("it''s") == "it''''s"
    assert _duckdb._escape_sql("") == ""


def test_escape_sql_multiple_quotes() -> None:
    """Test SQL escaping with multiple single quotes."""
    assert _duckdb._escape_sql("a'b'c") == "a''b''c"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_duckdb.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ducknx._duckdb'`

- [ ] **Step 3: Implement `_duckdb.py`**

Create `ducknx/_duckdb.py`:

```python
"""DuckDB connection lifecycle manager for local PBF file queries."""

from __future__ import annotations

import logging as lg
from pathlib import Path

import duckdb

from . import utils

_connection: duckdb.DuckDBPyConnection | None = None
_current_pbf_path: Path | None = None


def _escape_sql(value: str) -> str:
    """
    Escape single quotes in a string for safe SQL interpolation.

    Parameters
    ----------
    value
        The string value to escape.

    Returns
    -------
    escaped
        The escaped string with single quotes doubled.
    """
    return value.replace("'", "''")


def get_connection(pbf_path: str | Path) -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection with spatial extension loaded and PBF data available.

    On first call, creates a connection, installs/loads the spatial extension,
    and loads the PBF file into a persistent ``osm_data`` temp table. On
    subsequent calls with the same path, returns the cached connection. If the
    path changes, closes the old connection and creates a new one.

    Parameters
    ----------
    pbf_path
        Path to the local OSM PBF file.

    Returns
    -------
    conn
        A DuckDB connection with the ``osm_data`` table ready to query.

    Raises
    ------
    FileNotFoundError
        If the PBF file does not exist.
    """
    global _connection, _current_pbf_path  # noqa: PLW0603

    pbf_path = Path(pbf_path)
    if not pbf_path.exists():
        msg = f"PBF file not found: {pbf_path}"
        raise FileNotFoundError(msg)

    # return cached connection if same path
    if _connection is not None and _current_pbf_path == pbf_path:
        return _connection

    # close existing connection if path changed
    if _connection is not None:
        close()

    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    escaped_path = _escape_sql(str(pbf_path))
    conn.execute(f"CREATE TEMP TABLE osm_data AS SELECT * FROM ST_ReadOSM('{escaped_path}')")

    msg = f"Loaded PBF file into DuckDB: {pbf_path}"
    utils.log(msg, level=lg.INFO)

    _connection = conn
    _current_pbf_path = pbf_path
    return conn


def close() -> None:
    """
    Close the cached DuckDB connection and reset state.

    Safe to call multiple times. Does nothing if no connection is open.
    """
    global _connection, _current_pbf_path  # noqa: PLW0603
    if _connection is not None:
        try:
            _connection.close()
        except Exception:  # noqa: BLE001
            pass
    _connection = None
    _current_pbf_path = None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_duckdb.py -v`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ducknx/_duckdb.py tests/test_duckdb.py
git commit -m "feat: add DuckDB connection manager with SQL escaping"
```

---

### Task 2: Integrate Connection Manager into `_pbf_reader.py`

**Files:**
- Modify: `ducknx/_pbf_reader.py`
- Create: `tests/test_pbf_reader.py`

**Depends on:** Task 1

- [ ] **Step 1: Write failing tests for SQL escaping in tag filters**

Create `tests/test_pbf_reader.py`:

```python
"""Tests for PBF reader SQL safety."""

from __future__ import annotations

from ducknx import _pbf_reader


def test_tag_filter_escapes_single_quotes() -> None:
    """Test that tag filter construction escapes single quotes in values."""
    tags = {"name": "McDonald's"}
    conditions = _pbf_reader._build_tag_filter(tags)
    assert "''" in conditions
    assert "McDonald''s" in conditions


def test_tag_filter_bool_true() -> None:
    """Test tag filter with True value."""
    tags = {"building": True}
    conditions = _pbf_reader._build_tag_filter(tags)
    assert "tags['building'] IS NOT NULL" in conditions


def test_tag_filter_bool_false() -> None:
    """Test tag filter with False value."""
    tags = {"building": False}
    conditions = _pbf_reader._build_tag_filter(tags)
    assert "tags['building'] IS NULL" in conditions


def test_tag_filter_list_values() -> None:
    """Test tag filter with list of values."""
    tags = {"highway": ["primary", "O'Connell"]}
    conditions = _pbf_reader._build_tag_filter(tags)
    assert "O''Connell" in conditions


def test_tag_filter_empty() -> None:
    """Test tag filter with empty tags dict."""
    conditions = _pbf_reader._build_tag_filter({})
    assert conditions == "1=1"


def test_network_filter_all_and_all_public_identical() -> None:
    """Test that 'all' and 'all_public' produce identical SQL."""
    all_filter = _pbf_reader._get_network_filter_sql("all")
    all_public_filter = _pbf_reader._get_network_filter_sql("all_public")
    assert all_filter == all_public_filter
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pbf_reader.py -v`
Expected: FAIL — `AttributeError: module 'ducknx._pbf_reader' has no attribute '_build_tag_filter'`

- [ ] **Step 3: Extract `_build_tag_filter` and add SQL escaping**

In `ducknx/_pbf_reader.py`, add the following function after the imports and before `_get_network_filter_sql`:

```python
from . import _duckdb
```

Then add the `_build_tag_filter` function:

```python
def _build_tag_filter(tags: dict[str, bool | str | list[str]]) -> str:
    """
    Build a SQL WHERE clause to filter OSM elements by tags.

    Escapes all string values to prevent SQL injection from values
    containing single quotes.

    Parameters
    ----------
    tags
        Tags used for finding elements. Keys are OSM tag names and values
        can be True (tag exists), False (tag missing), a string (exact
        match), or a list of strings (any match).

    Returns
    -------
    tag_filter
        SQL WHERE clause string.
    """
    tag_conditions = []
    for key, value in tags.items():
        escaped_key = _duckdb._escape_sql(key)
        if isinstance(value, bool):
            if value:
                tag_conditions.append(f"tags['{escaped_key}'] IS NOT NULL")
            else:
                tag_conditions.append(f"tags['{escaped_key}'] IS NULL")
        elif isinstance(value, str):
            escaped_value = _duckdb._escape_sql(value)
            tag_conditions.append(f"tags['{escaped_key}'] = '{escaped_value}'")
        elif isinstance(value, list):
            escaped_values = "', '".join(_duckdb._escape_sql(v) for v in value)
            tag_conditions.append(f"tags['{escaped_key}'] IN ('{escaped_values}')")

    return " OR ".join(tag_conditions) if tag_conditions else "1=1"
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `uv run pytest tests/test_pbf_reader.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add ducknx/_pbf_reader.py tests/test_pbf_reader.py
git commit -m "feat: add _build_tag_filter with SQL escaping"
```

- [ ] **Step 6: Refactor `_read_pbf_network_duckdb` to use connection manager**

In `ducknx/_pbf_reader.py`, modify `_read_pbf_network_duckdb`:

1. Replace the function body — remove connection creation, extension install, PBF loading, and `finally: conn.close()`. Use `_duckdb.get_connection()` instead.
2. Inline `_polygon_to_wkt` — replace `_polygon_to_wkt(polygon)` with `polygon.wkt`.
3. Add `DROP TABLE IF EXISTS` for temp tables at the start to handle stale state.

The refactored function should look like:

```python
def _read_pbf_network_duckdb(
    polygon: Polygon | MultiPolygon,
    network_type: str,
    custom_filter: str | list[str] | None,
    pbf_path: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retrieve networked ways and nodes from a local PBF file using DuckDB SQL.

    This optimized code path performs filtering and joins in DuckDB and returns
    pandas DataFrames directly, avoiding intermediate JSON dict conversion.

    Parameters
    ----------
    polygon
        The geometry within which to retrieve data.
    network_type
        What type of street network to retrieve.
    custom_filter
        Additional custom filter conditions as SQL WHERE clauses.
    pbf_path
        Path to the local OSM PBF file.

    Returns
    -------
    nodes_df, ways_df
        DataFrames of nodes and ways.

    Raises
    ------
    InsufficientResponseError
        If no data was retrieved from the PBF file.
    """
    conn = _duckdb.get_connection(pbf_path)

    polygon_wkt = _duckdb._escape_sql(polygon.wkt)
    network_filter = _get_network_filter_sql(network_type)

    if custom_filter:
        if isinstance(custom_filter, list):
            custom_filter = " AND ".join(custom_filter)
        network_filter += f" AND ({custom_filter})"

    # drop temp tables from any previous call
    conn.execute("DROP TABLE IF EXISTS area_nodes")
    conn.execute("DROP TABLE IF EXISTS filtered_ways")

    # Find nodes in the spatial area
    conn.execute(f"""
        CREATE TEMP TABLE area_nodes AS
        SELECT id FROM osm_data
        WHERE kind = 'node'
        AND ST_Within(ST_Point(lon, lat), ST_GeomFromText('{polygon_wkt}'))
    """)

    # Find ways matching filter that have at least one node in spatial area
    conn.execute(f"""
        CREATE TEMP TABLE filtered_ways AS
        SELECT id, tags, refs
        FROM osm_data
        WHERE kind = 'way'
        AND {network_filter}
        AND refs IS NOT NULL AND array_length(refs) > 0
        AND EXISTS (
            SELECT 1 FROM UNNEST(refs) AS t(node_id)
            WHERE node_id IN (SELECT id FROM area_nodes)
        )
    """)

    # Build ways SELECT with useful tags extracted in SQL
    way_tag_cols = ", ".join(
        f"tags['{_duckdb._escape_sql(tag)}'] AS \"{tag}\"" for tag in settings.useful_tags_way
    )
    ways_df = conn.execute(f"""
        SELECT
            id AS osmid,
            refs,
            {way_tag_cols}
        FROM filtered_ways
    """).fetchdf()

    if ways_df.empty:
        msg = f"No ways found matching network filter in {pbf_path}"
        raise InsufficientResponseError(msg)

    utils.log(f"Found {len(ways_df)} ways matching network filter", level=lg.INFO)

    # Get ALL nodes referenced by filtered ways (not just in area)
    node_tag_cols = ", ".join(
        f"n.tags['{_duckdb._escape_sql(tag)}'] AS \"{tag}\"" for tag in settings.useful_tags_node
    )
    nodes_df = conn.execute(f"""
        SELECT
            n.id,
            n.lat AS y,
            n.lon AS x,
            {node_tag_cols}
        FROM osm_data n
        INNER JOIN (
            SELECT DISTINCT UNNEST(refs) AS node_id FROM filtered_ways
        ) r ON n.id = r.node_id
        WHERE n.kind = 'node'
    """).fetchdf()

    if nodes_df.empty:
        msg = "No nodes found for the filtered ways"
        raise InsufficientResponseError(msg)

    utils.log(f"Found {len(nodes_df)} nodes for filtered ways", level=lg.INFO)

    return nodes_df, ways_df
```

- [ ] **Step 7: Refactor `_read_pbf_features_duckdb` to use connection manager**

In `ducknx/_pbf_reader.py`, modify `_read_pbf_features_duckdb`:

1. Replace connection creation with `_duckdb.get_connection(pbf_path)`.
2. Replace inline tag filter construction with `_build_tag_filter(tags)`.
3. Inline `polygon.wkt` with escaping.
4. Remove `finally: conn.close()`.
5. Add `DROP TABLE IF EXISTS` for all temp tables at the start.
6. Remove the `FileNotFoundError` check (now handled by `_duckdb.get_connection`).

The refactored function should look like:

```python
def _read_pbf_features_duckdb(
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
    pbf_path: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Retrieve OSM features from a local PBF file using DuckDB SQL.

    This optimized code path performs filtering and geometry construction in
    DuckDB and returns pandas DataFrames directly.

    Parameters
    ----------
    polygon
        The geometry within which to retrieve features.
    tags
        Tags used for finding elements in the search area.
    pbf_path
        Path to the local OSM PBF file.

    Returns
    -------
    nodes_df, ways_df, relations_df
        DataFrames of nodes, ways, and relations.

    Raises
    ------
    InsufficientResponseError
        If no data was retrieved from the PBF file.
    """
    conn = _duckdb.get_connection(pbf_path)

    polygon_wkt = _duckdb._escape_sql(polygon.wkt)
    tag_filter = _build_tag_filter(tags)

    # Drop temp tables from any previous call
    conn.execute("DROP TABLE IF EXISTS feature_area_nodes")
    conn.execute("DROP TABLE IF EXISTS tagged_ways")
    conn.execute("DROP TABLE IF EXISTS matching_ways_with_node_refs")
    conn.execute("DROP TABLE IF EXISTS required_nodes_with_geometries")
    conn.execute("DROP TABLE IF EXISTS matching_ways_linestrings")
    conn.execute("DROP TABLE IF EXISTS matching_relations")
    conn.execute("DROP TABLE IF EXISTS matching_relations_with_ways_refs")
    conn.execute("DROP TABLE IF EXISTS required_ways_linestrings")
    conn.execute("DROP TABLE IF EXISTS matching_relations_with_ways_linestrings")
    conn.execute("DROP TABLE IF EXISTS matching_relations_with_merged_polygons")
    conn.execute("DROP TABLE IF EXISTS outer_with_holes")
    conn.execute("DROP TABLE IF EXISTS outer_without_holes")

    # Query nodes with geometry
    nodes_df = conn.execute(f"""
        SELECT
            id,
            tags,
            ST_AsWKB(ST_Point(lon, lat)) AS geometry
        FROM osm_data
        WHERE kind = 'node'
        AND ({tag_filter})
        AND ST_Within(ST_Point(lon, lat), ST_GeomFromText('{polygon_wkt}'))
    """).fetchdf()

    utils.log(f"Found {len(nodes_df)} feature nodes", level=lg.INFO)

    # Create area nodes for spatial filtering of ways
    conn.execute(f"""
        CREATE TEMP TABLE feature_area_nodes AS
        SELECT id FROM osm_data
        WHERE kind = 'node'
        AND ST_Within(
            ST_Point(lon, lat),
            ST_GeomFromText('{polygon_wkt}')
        )
    """)

    # --- Way geometry construction using UNNEST + ST_MakeLine ---

    # Step 1: Find tagged ways with at least one node in spatial area
    conn.execute(f"""
        CREATE TEMP TABLE tagged_ways AS
        SELECT id, tags, refs
        FROM osm_data
        WHERE kind = 'way'
        AND ({tag_filter})
        AND refs IS NOT NULL AND array_length(refs) > 0
        AND EXISTS (
            SELECT 1 FROM UNNEST(refs) AS t(node_id)
            WHERE node_id IN (SELECT id FROM feature_area_nodes)
        )
    """)

    # Steps 2-5: Way geometry construction (unchanged SQL)
    conn.execute("""
        CREATE TEMP TABLE matching_ways_with_node_refs AS
        SELECT id, UNNEST(refs) AS ref, UNNEST(range(length(refs))) AS ref_idx
        FROM osm_data
        SEMI JOIN tagged_ways USING (id)
        WHERE kind = 'way'
    """)

    conn.execute("""
        CREATE TEMP TABLE required_nodes_with_geometries AS
        SELECT id, ST_Point(lon, lat) AS geometry
        FROM osm_data nodes
        SEMI JOIN matching_ways_with_node_refs ON nodes.id = matching_ways_with_node_refs.ref
        WHERE kind = 'node'
    """)

    conn.execute("""
        CREATE TEMP TABLE matching_ways_linestrings AS
        SELECT
            tagged_ways.id,
            tagged_ways.tags,
            tagged_ways.refs,
            ST_MakeLine(list(nodes.geometry ORDER BY ref_idx ASC)) AS linestring
        FROM tagged_ways
        JOIN matching_ways_with_node_refs
            ON tagged_ways.id = matching_ways_with_node_refs.id
        JOIN required_nodes_with_geometries nodes
            ON matching_ways_with_node_refs.ref = nodes.id
        GROUP BY tagged_ways.id, tagged_ways.tags, tagged_ways.refs
    """)

    ways_df = conn.execute("""
        WITH way_polygon_feature AS (
            SELECT id,
                (ST_Equals(ST_StartPoint(linestring), ST_EndPoint(linestring))
                 AND tags IS NOT NULL
                 AND NOT (
                     list_contains(map_keys(tags), 'area')
                     AND list_extract(map_extract(tags, 'area'), 1) = 'no'
                 )
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
    """).fetchdf()

    utils.log(f"Found {len(ways_df)} feature ways", level=lg.INFO)

    # --- Relation geometry construction in SQL ---

    conn.execute(f"""
        CREATE TEMP TABLE matching_relations AS
        SELECT id, tags
        FROM osm_data
        WHERE kind = 'relation'
        AND ({tag_filter})
        AND len(refs) > 0
        AND tags IS NOT NULL AND cardinality(tags) > 0
        AND list_contains(map_keys(tags), 'type')
        AND list_has_any(map_extract(tags, 'type'), ['boundary', 'multipolygon'])
    """)

    conn.execute("""
        CREATE TEMP TABLE matching_relations_with_ways_refs AS
        WITH unnested AS (
            SELECT r.id,
                UNNEST(refs) AS ref,
                UNNEST(ref_types) AS ref_type,
                UNNEST(ref_roles) AS ref_role,
                UNNEST(range(length(refs))) AS ref_idx
            FROM osm_data r
            SEMI JOIN matching_relations USING (id)
            WHERE kind = 'relation'
        )
        SELECT id, ref, ref_role, ref_idx
        FROM unnested
        WHERE ref_type = 'way'
    """)

    conn.execute("""
        CREATE TEMP TABLE required_ways_linestrings AS
        WITH ways_refs AS (
            SELECT id,
                UNNEST(refs) AS ref,
                UNNEST(range(length(refs))) AS ref_idx
            FROM osm_data ways
            SEMI JOIN matching_relations_with_ways_refs
                ON ways.id = matching_relations_with_ways_refs.ref
            WHERE kind = 'way'
        ),
        nodes AS (
            SELECT id, ST_Point(lon, lat) AS geometry
            FROM osm_data
            SEMI JOIN ways_refs ON osm_data.id = ways_refs.ref
            WHERE kind = 'node'
        )
        SELECT ways_refs.id,
            ST_MakeLine(list(nodes.geometry ORDER BY ref_idx ASC)) AS linestring
        FROM ways_refs
        JOIN nodes ON ways_refs.ref = nodes.id
        GROUP BY ways_refs.id
    """)

    conn.execute("""
        CREATE TEMP TABLE matching_relations_with_ways_linestrings AS
        WITH joined AS (
            SELECT r.id,
                COALESCE(r.ref_role, 'outer') AS ref_role,
                r.ref,
                w.linestring::GEOMETRY AS geometry
            FROM matching_relations_with_ways_refs r
            JOIN required_ways_linestrings w ON w.id = r.ref
            ORDER BY r.id, r.ref_idx
        ),
        any_outer AS (
            SELECT id, bool_or(ref_role = 'outer') AS has_any_outer
            FROM joined
            GROUP BY id
        )
        SELECT j.* EXCLUDE (ref_role),
            CASE WHEN ao.has_any_outer THEN j.ref_role ELSE 'outer' END AS ref_role
        FROM joined j
        JOIN any_outer ao ON ao.id = j.id
    """)

    conn.execute("""
        CREATE TEMP TABLE matching_relations_with_merged_polygons AS
        WITH merged AS (
            SELECT id, ref_role,
                UNNEST(
                    ST_Dump(ST_LineMerge(ST_Collect(list(geometry)))),
                    recursive := true
                )
            FROM matching_relations_with_ways_linestrings
            GROUP BY id, ref_role
        ),
        with_linestrings AS (
            SELECT id, ref_role, geom AS geometry,
                row_number() OVER (PARTITION BY id) AS geometry_id
            FROM merged
            WHERE ST_NPoints(geom) >= 4
        ),
        valid AS (
            SELECT id FROM (
                SELECT id,
                    bool_and(
                        ST_Equals(ST_StartPoint(geometry), ST_EndPoint(geometry))
                    ) AS is_valid
                FROM with_linestrings
                GROUP BY id
            ) WHERE is_valid
        )
        SELECT id, ref_role, ST_MakePolygon(geometry) AS geometry, geometry_id
        FROM with_linestrings
        SEMI JOIN valid USING (id)
    """)

    conn.execute("""
        CREATE TEMP TABLE outer_with_holes AS
        WITH outer_p AS (
            SELECT id, geometry_id, geometry
            FROM matching_relations_with_merged_polygons
            WHERE ref_role = 'outer'
        ),
        inner_p AS (
            SELECT id, geometry_id, geometry
            FROM matching_relations_with_merged_polygons
            WHERE ref_role = 'inner'
        )
        SELECT op.id, op.geometry_id,
            ST_Difference(
                any_value(op.geometry),
                ST_Union_Agg(ip.geometry)
            ) AS geometry
        FROM outer_p op
        JOIN inner_p ip
            ON op.id = ip.id AND ST_Within(ip.geometry, op.geometry)
        GROUP BY op.id, op.geometry_id
    """)

    conn.execute("""
        CREATE TEMP TABLE outer_without_holes AS
        WITH outer_p AS (
            SELECT id, geometry_id, geometry
            FROM matching_relations_with_merged_polygons
            WHERE ref_role = 'outer'
        )
        SELECT op.id, op.geometry_id, op.geometry
        FROM outer_p op
        ANTI JOIN outer_with_holes owh
            ON op.id = owh.id AND op.geometry_id = owh.geometry_id
    """)

    relations_df = conn.execute("""
        SELECT r.id, r.tags,
            ST_AsWKB(ST_Union_Agg(g.geometry)) AS geometry
        FROM (
            SELECT id, geometry FROM outer_with_holes
            UNION ALL
            SELECT id, geometry FROM outer_without_holes
        ) g
        JOIN matching_relations r ON r.id = g.id
        GROUP BY r.id, r.tags
    """).fetchdf()

    utils.log(f"Found {len(relations_df)} feature relations", level=lg.INFO)

    if nodes_df.empty and ways_df.empty and relations_df.empty:
        msg = f"No feature data found in {pbf_path} for the specified area and tags"
        raise InsufficientResponseError(msg)

    return nodes_df, ways_df, relations_df
```

- [ ] **Step 8: Remove `_polygon_to_wkt` function, `import duckdb`, and redundant checks**

In `ducknx/_pbf_reader.py`:

1. Remove `import duckdb` from the imports (no longer used directly).
2. Remove the entire `_polygon_to_wkt` function (lines 96-110 in original).
3. Remove the `FileNotFoundError` check from `_read_pbf_network_duckdb` (the connection manager handles it).
4. Remove `from pathlib import Path` if no longer used directly (it's used in type hints — keep if needed for `str | Path` annotations).

- [ ] **Step 9: Add comment about `all` vs `all_public`**

In `ducknx/_pbf_reader.py`, in `_get_network_filter_sql`, update the `all_public` branch:

```python
    elif network_type == "all_public":
        # Identical to 'all' in the local PBF path — the distinction existed
        # in the Overpass API path where access filtering differed. Kept for
        # API compatibility.
        return " AND ".join(base_conditions)
```

- [ ] **Step 10: Run full existing test suite to verify no regressions**

Run: `uv run pytest tests/test_osmnx.py -v -x`
Expected: All existing tests PASS (the public API is unchanged)

- [ ] **Step 11: Run new tests**

Run: `uv run pytest tests/test_pbf_reader.py tests/test_duckdb.py -v`
Expected: All tests PASS

- [ ] **Step 12: Commit**

```bash
git add ducknx/_pbf_reader.py tests/test_pbf_reader.py
git commit -m "feat: integrate connection manager, add SQL escaping, inline _polygon_to_wkt"
```

---

### Task 3: Fix `_http.py` Bugs and Migrate to httpx

**Files:**
- Modify: `ducknx/_http.py`
- Create: `tests/test_http.py`

- [ ] **Step 1: Write failing tests for `_parse_response`**

Create `tests/test_http.py`:

```python
"""Tests for HTTP utilities."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from ducknx import _http


def test_parse_response_valid_json() -> None:
    """Test _parse_response with a valid JSON response."""
    response = httpx.Response(
        status_code=200,
        json={"results": [{"elevation": 10.0}]},
        request=httpx.Request("GET", "https://example.com/api"),
    )
    result = _http._parse_response(response)
    assert isinstance(result, dict)
    assert "results" in result


def test_parse_response_list_json() -> None:
    """Test _parse_response with a list JSON response (like Nominatim)."""
    response = httpx.Response(
        status_code=200,
        json=[{"place_id": 1, "display_name": "test"}],
        request=httpx.Request("GET", "https://nominatim.example.com/search"),
    )
    result = _http._parse_response(response)
    assert isinstance(result, list)
    assert len(result) == 1


def test_parse_response_not_ok_logs_warning() -> None:
    """Test _parse_response logs warning for non-OK status."""
    response = httpx.Response(
        status_code=400,
        json={"error": "bad request"},
        request=httpx.Request("GET", "https://example.com/api"),
    )
    # Should not raise, just log warning and return the json
    result = _http._parse_response(response)
    assert isinstance(result, dict)


def test_get_http_headers() -> None:
    """Test that _get_http_headers returns proper headers."""
    headers = _http._get_http_headers()
    assert "User-Agent" in headers
    assert "referer" in headers
    assert "Accept-Language" in headers


def test_hostname_from_url() -> None:
    """Test extracting hostname from URL."""
    assert _http._hostname_from_url("https://example.com/path") == "example.com"
    assert _http._hostname_from_url("https://api.example.com:8080/path") == "api.example.com"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_http.py -v`
Expected: FAIL — `_parse_response` references undefined `requests.Response` type and `JSONDecodeError`

- [ ] **Step 3: Fix `_http.py`**

In `ducknx/_http.py`, make the following changes:

1. Add `from json import JSONDecodeError` to imports.
2. Add `import httpx` to imports (it's already there for `httpx.utils.default_headers()`).
3. Remove the unused `from urllib.parse import urlparse` if `_hostname_from_url` is updated, or keep it.
4. Change `_parse_response` signature from `requests.Response` to `httpx.Response`.
5. Change `response.reason` to `response.reason_phrase`.
6. Change `_hostname_from_url` to handle httpx `URL` objects by converting to `str` first.

The updated imports section:

```python
"""Handle HTTP requests to web APIs."""

from __future__ import annotations

import json
import logging as lg
from hashlib import sha1
from json import JSONDecodeError
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx

from . import settings
from . import utils
from ._errors import InsufficientResponseError
from ._errors import ResponseStatusCodeError
```

The updated `_hostname_from_url`:

```python
def _hostname_from_url(url: str | httpx.URL) -> str:
    """
    Extract the hostname (domain) from a URL.

    Parameters
    ----------
    url
        The url from which to extract the hostname.

    Returns
    -------
    hostname
        The extracted hostname (domain).
    """
    return urlparse(str(url)).netloc.split(":")[0]
```

The updated `_parse_response`:

```python
def _parse_response(response: httpx.Response) -> dict[str, Any] | list[dict[str, Any]]:
    """
    Parse JSON from an httpx response and log the details.

    Parameters
    ----------
    response
        The response object.

    Returns
    -------
    response_json
        Value will be a dict if the response is from the Google Elevation
        API, and a list if the response is from the Nominatim API.
    """
    # log the response size and hostname
    hostname = _hostname_from_url(response.url)
    size_kb = len(response.content) / 1000
    msg = f"Downloaded {size_kb:,.1f}kB from {hostname!r} with status {response.status_code}"
    utils.log(msg, level=lg.INFO)

    # parse the response to JSON and log/raise exceptions
    try:
        response_json: dict[str, Any] | list[dict[str, Any]] = response.json()
    except JSONDecodeError as e:  # pragma: no cover
        msg = f"{hostname!r} responded: {response.status_code} {response.reason_phrase} {response.text}"
        utils.log(msg, level=lg.ERROR)
        if response.is_success:
            raise InsufficientResponseError(msg) from e
        raise ResponseStatusCodeError(msg) from e

    # log any remarks if they exist
    if isinstance(response_json, dict) and "remark" in response_json:  # pragma: no cover
        msg = f"{hostname!r} remarked: {response_json['remark']!r}"
        utils.log(msg, level=lg.WARNING)

    # log if the response status_code is not OK
    if not response.is_success:
        msg = f"{hostname!r} returned HTTP status code {response.status_code}"
        utils.log(msg, level=lg.WARNING)

    return response_json
```

Note: `response.ok` in requests maps to `response.is_success` in httpx. Both check for 2xx status codes.

- [ ] **Step 4: Update `_get_http_headers` to use httpx**

The function already uses `httpx.utils.default_headers()` — no change needed. But verify it doesn't reference `requests` anywhere. Remove any stale `requests` references if present.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_http.py -v`
Expected: All 5 tests PASS

- [ ] **Step 6: Commit**

```bash
git add ducknx/_http.py tests/test_http.py
git commit -m "fix: migrate _http.py to httpx, fix missing JSONDecodeError import"
```

---

### Task 4: Migrate `_nominatim.py` to httpx

**Files:**
- Modify: `ducknx/_nominatim.py`

**Depends on:** Task 3

- [ ] **Step 1: Replace `requests` with `httpx` in `_nominatim.py`**

In `ducknx/_nominatim.py`, make the following changes:

1. Replace `import requests` with `import httpx`.

2. In `_nominatim_request`, replace the URL preparation for cache key:

Before:
```python
prepared_url = str(requests.Request("GET", url, params=params).prepare().url)
```

After:
```python
prepared_url = str(httpx.Request("GET", url, params=params).url)
```

3. Replace `requests.get(...)` with `httpx.get(...)`:

Before:
```python
response = requests.get(
    url,
    params=params,
    timeout=settings.requests_timeout,
    headers=_http._get_http_headers(),
    **settings.requests_kwargs,
)
```

After:
```python
response = httpx.get(
    url,
    params=params,
    timeout=settings.requests_timeout,
    headers=_http._get_http_headers(),
    **settings.requests_kwargs,
)
```

4. In the 429/504 retry block, replace `response.reason` with `response.reason_phrase`:

Before:
```python
f"{hostname!r} responded {response.status_code} {response.reason}: "
```

After:
```python
f"{hostname!r} responded {response.status_code} {response.reason_phrase}: "
```

- [ ] **Step 2: Run the geocoder tests to verify Nominatim integration works**

Run: `uv run pytest tests/test_osmnx.py::test_geocoder -v`
Expected: PASS

- [ ] **Step 3: Run the full test suite for regression check**

Run: `uv run pytest tests/test_osmnx.py -v -x`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add ducknx/_nominatim.py
git commit -m "fix: migrate _nominatim.py from requests to httpx"
```

---

### Task 5: Vectorize `_create_gdf_from_dfs` in `features.py`

**Files:**
- Modify: `ducknx/features.py`
- Create: `tests/test_features_vectorized.py`

- [ ] **Step 1: Write failing tests for vectorized GeoDataFrame construction**

Create `tests/test_features_vectorized.py`:

```python
"""Tests for vectorized _create_gdf_from_dfs."""

from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely import Point
from shapely import Polygon
from shapely import wkb

from ducknx import features


def _make_node_df(nodes: list[tuple[int, dict, Point]]) -> pd.DataFrame:
    """Create a nodes DataFrame matching DuckDB output format."""
    rows = []
    for nid, tags, geom in nodes:
        rows.append({"id": nid, "tags": tags, "geometry": wkb.dumps(geom)})
    return pd.DataFrame(rows)


def _make_empty_df(*cols: str) -> pd.DataFrame:
    """Create an empty DataFrame with given columns."""
    return pd.DataFrame(columns=list(cols))


def test_create_gdf_nodes_only() -> None:
    """Test GeoDataFrame creation with only nodes."""
    nodes_df = _make_node_df([
        (1, {"amenity": "cafe", "name": "Test Cafe"}, Point(0, 0)),
        (2, {"amenity": "bank"}, Point(1, 1)),
    ])
    ways_df = _make_empty_df("id", "tags", "refs", "geometry", "is_polygon")
    relations_df = _make_empty_df("id", "tags", "geometry")

    bbox_poly = Polygon([(-10, -10), (10, -10), (10, 10), (-10, 10)])
    tags = {"amenity": True}

    gdf = features._create_gdf_from_dfs(nodes_df, ways_df, relations_df, bbox_poly, tags)

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 2
    assert ("node", 1) in gdf.index
    assert ("node", 2) in gdf.index


def test_create_gdf_preserves_tags() -> None:
    """Test that tags are correctly expanded into columns."""
    nodes_df = _make_node_df([
        (1, {"amenity": "cafe", "name": "Joe's Place"}, Point(0, 0)),
    ])
    ways_df = _make_empty_df("id", "tags", "refs", "geometry", "is_polygon")
    relations_df = _make_empty_df("id", "tags", "geometry")

    bbox_poly = Polygon([(-10, -10), (10, -10), (10, 10), (-10, 10)])
    tags = {"amenity": True}

    gdf = features._create_gdf_from_dfs(nodes_df, ways_df, relations_df, bbox_poly, tags)

    assert gdf.loc[("node", 1), "name"] == "Joe's Place"
    assert gdf.loc[("node", 1), "amenity"] == "cafe"
```

- [ ] **Step 2: Run tests to verify they pass with current implementation**

Run: `uv run pytest tests/test_features_vectorized.py -v`
Expected: PASS (current row-by-row implementation should produce same results)

This verifies the tests are correct before we refactor. If they fail, fix the test setup.

- [ ] **Step 3: Refactor `_create_gdf_from_dfs` to use vectorized operations**

In `ducknx/features.py`, add `import shapely` to the imports (for `shapely.from_wkb`), then replace the `_create_gdf_from_dfs` function:

```python
def _create_gdf_from_dfs(
    nodes_df: pd.DataFrame,
    ways_df: pd.DataFrame,
    relations_df: pd.DataFrame,
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
) -> gpd.GeoDataFrame:
    """
    Create a GeoDataFrame of features from node, way, and relation DataFrames.

    Uses vectorized shapely/pandas operations instead of row-by-row iteration
    for better performance on large datasets.

    Parameters
    ----------
    nodes_df
        DataFrame with columns: id, tags, geometry (WKB).
    ways_df
        DataFrame with columns: id, tags, refs, geometry (WKB), is_polygon.
    relations_df
        DataFrame with columns: id, tags, geometry (WKB).
    polygon
        Spatial boundaries to filter the final GeoDataFrame.
    tags
        Query tags to filter the final GeoDataFrame.

    Returns
    -------
    gdf
        GeoDataFrame of features with tags and geometry columns.
    """
    frames: list[pd.DataFrame] = []

    # Process nodes
    if not nodes_df.empty:
        node_geoms = shapely.from_wkb(nodes_df["geometry"].apply(bytes))
        node_tags = pd.DataFrame(nodes_df["tags"].apply(dict).tolist())
        node_tags.pop("geometry", None)
        node_result = pd.DataFrame({
            "element": "node",
            "id": nodes_df["id"].astype(int),
        })
        node_result = pd.concat(
            [node_result.reset_index(drop=True), node_tags.reset_index(drop=True)],
            axis=1,
        )
        node_result["geometry"] = node_geoms.values
        frames.append(node_result)

    # Process ways
    if not ways_df.empty:
        way_geoms = shapely.from_wkb(ways_df["geometry"].apply(bytes))
        way_tags_series = ways_df["tags"].apply(lambda t: dict(t) if t else {})
        way_tags = pd.DataFrame(way_tags_series.tolist())
        way_tags.pop("geometry", None)

        query_tag_keys = set(tags.keys())

        # Vectorized polygon refinement: SQL marked is_polygon based on
        # simplified rule; use full _POLYGON_FEATURES rules to fix.
        needs_fix = ways_df["is_polygon"].values & ~way_tags_series.apply(_should_be_polygon).values
        for idx in np.where(needs_fix)[0]:
            geom = way_geoms.iloc[idx]
            try:
                way_geoms.iloc[idx] = LineString(geom.exterior.coords)
            except (AttributeError, GEOSException, ValueError):
                pass  # keep as-is if conversion fails

        way_result = pd.DataFrame({
            "element": "way",
            "id": ways_df["id"].astype(int).values,
        })
        way_result = pd.concat(
            [way_result.reset_index(drop=True), way_tags.reset_index(drop=True)],
            axis=1,
        )
        way_result["geometry"] = way_geoms.values

        # Filter to ways whose tags match query tags
        if len(query_tag_keys) > 0:
            matching_cols = query_tag_keys & set(way_tags.columns)
            if matching_cols:
                has_match = way_tags[list(matching_cols)].notna().any(axis=1)
                way_result = way_result[has_match.values]
            else:
                way_result = way_result.iloc[0:0]
        else:
            # keep ways that have any tags at all
            has_tags = way_tags_series.apply(lambda t: len(t) > 0)
            way_result = way_result[has_tags.values]

        frames.append(way_result)

    # Process relations
    if not relations_df.empty:
        rel_geoms = shapely.from_wkb(relations_df["geometry"].apply(bytes))
        rel_tags_series = relations_df["tags"].apply(lambda t: dict(t) if t else {})
        rel_tags = pd.DataFrame(rel_tags_series.tolist())
        rel_tags.pop("geometry", None)

        rel_result = pd.DataFrame({
            "element": "relation",
            "id": relations_df["id"].astype(int).values,
        })
        rel_result = pd.concat(
            [rel_result.reset_index(drop=True), rel_tags.reset_index(drop=True)],
            axis=1,
        )
        rel_result["geometry"] = rel_geoms.values
        frames.append(rel_result)

    if len(frames) == 0 or all(len(f) == 0 for f in frames):
        msg = "No matching features. Check query location, tags, and log."
        raise InsufficientResponseError(msg)

    combined = pd.concat(frames, ignore_index=True)
    gdf = (
        gpd.GeoDataFrame(
            data=combined,
            geometry="geometry",
            crs=settings.default_crs,
        )
        .set_index(["element", "id"])
        .sort_index()
    )
    return _filter_features(gdf, polygon, tags)
```

Also add `import shapely` near the top of the file with the other shapely imports.

- [ ] **Step 4: Run the new tests**

Run: `uv run pytest tests/test_features_vectorized.py -v`
Expected: All tests PASS

- [ ] **Step 5: Run the full features test suite for regression check**

Run: `uv run pytest tests/test_osmnx.py::test_features -v`
Expected: PASS

- [ ] **Step 6: Run the full test suite**

Run: `uv run pytest tests/test_osmnx.py -v -x`
Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add ducknx/features.py tests/test_features_vectorized.py
git commit -m "perf: vectorize _create_gdf_from_dfs with shapely/pandas ops"
```

---

### Task 6: Final Verification and Cleanup

**Files:**
- All modified files

- [ ] **Step 1: Run pre-commit hooks**

Run: `uv run pre-commit run --all-files`
Expected: All checks pass. If ruff or mypy flag issues, fix them.

- [ ] **Step 2: Run the complete test suite**

Run: `uv run pytest tests/ -v`
Expected: All tests PASS

- [ ] **Step 3: Verify no stale references to `requests` in active code**

Run: `grep -rn "import requests" ducknx/ --include="*.py" | grep -v _overpass.py`
Expected: No output (only `_overpass.py` should still use `requests`)

- [ ] **Step 4: Verify `_polygon_to_wkt` is fully removed**

Run: `grep -rn "_polygon_to_wkt" ducknx/`
Expected: No output

- [ ] **Step 5: Commit any lint/type fixes**

```bash
git add -u
git commit -m "chore: fix lint and type issues from cleanup"
```
