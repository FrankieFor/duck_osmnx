# ducknx Cleanup & Improvement Design Spec

**Date:** 2026-04-17
**Status:** Draft

## Overview

Improve ducknx's active code paths by fixing bugs, hardening SQL construction, standardizing HTTP libraries, introducing DuckDB connection reuse, and vectorizing DataFrame processing.

## Out of Scope

- Modifying `_overpass.py` (kept as-is)
- Removing any modules or settings (no dead code removal)
- Refactoring `_pbf_reader.py`'s internal SQL structure (the 7-step relation geometry pipeline stays as-is)
- Thread safety for the DuckDB connection manager

## Design Decisions

- **httpx over requests for active code:** `_nominatim.py` and `_http.py` migrate to httpx. `_overpass.py` keeps `requests` untouched. `requests` stays as a dependency in `pyproject.toml`.
- **Module-level cached connection:** A single DuckDB connection is cached per PBF path. No thread safety guarantees — DuckDB connections are single-threaded by design. Users can call `close()` to release.
- **SQL escaping, not parameterization:** DuckDB doesn't support parameters inside `CREATE TEMP TABLE AS ...` statements. We use consistent single-quote escaping (`'` → `''`) via a helper function instead.
- **Vectorize with shapely/pandas ops:** Replace Python-level row iteration in `_create_gdf_from_dfs` with `shapely.from_wkb()`, `pd.json_normalize()`, and `DataFrame.apply()` for the `_should_be_polygon` check.

## Component 1: `_duckdb.py` — Connection Manager

### Purpose

New internal module that owns the DuckDB connection lifecycle, ensuring the spatial extension is installed once and the PBF file is loaded into a temp table once per path.

### Interface

```python
def get_connection(pbf_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Return a connection with spatial loaded and PBF data in osm_data table.

    If a connection already exists for the given pbf_path, return it.
    If the path has changed, close the old connection and create a new one.
    """

def close() -> None:
    """Close the cached connection and reset state."""
```

### Internal State

- `_connection: duckdb.DuckDBPyConnection | None` — the cached connection
- `_current_pbf_path: Path | None` — which PBF file is currently loaded

### Behavior

1. First call: `duckdb.connect()` → `INSTALL spatial; LOAD spatial` → `CREATE TEMP TABLE osm_data AS SELECT * FROM ST_ReadOSM('{escaped_path}')` → cache and return.
2. Same path: return cached connection.
3. Different path: `close()` old connection, repeat step 1 with new path.
4. `close()`: call `conn.close()`, set both state variables to `None`.

### Impact on `_pbf_reader.py`

- Remove `duckdb.connect()`, `INSTALL spatial`, `LOAD spatial`, `CREATE TEMP TABLE osm_data` from both `_read_pbf_network_duckdb` and `_read_pbf_features_duckdb`.
- Replace with `conn = _duckdb.get_connection(pbf_path)`.
- Remove `conn.close()` from `finally` blocks (connection is managed by `_duckdb`).
- Temp tables created during query execution (e.g., `area_nodes`, `filtered_ways`) must still be cleaned up after each call to avoid collisions on subsequent calls. Each reader function starts with `DROP TABLE IF EXISTS` for all its temp tables (handles interrupted previous calls).

## Component 2: SQL Safety in `_pbf_reader.py`

### Problem

String interpolation of user-provided values into SQL:
```python
f"tags['{key}'] = '{value}'"  # breaks on: McDonald's
```

### Solution

Add `_escape_sql(value: str) -> str` helper in `_pbf_reader.py`:
```python
def _escape_sql(value: str) -> str:
    return value.replace("'", "''")
```

Apply to all interpolated values:
- Tag keys and values in `_read_pbf_features_duckdb`'s tag filter construction
- Polygon WKT strings
- PBF file paths (in `_duckdb.py` when loading the file)

### Additional Cleanup

- Inline `_polygon_to_wkt` — replace `_polygon_to_wkt(polygon)` with `polygon.wkt` at both call sites.
- Add a comment to `_get_network_filter_sql` noting that `"all"` and `"all_public"` intentionally produce identical SQL (they were differentiated in the Overpass path but not here).

## Component 3: Migrate `_nominatim.py` and `_http.py` to httpx

### `_http.py` Changes

| Before | After |
|--------|-------|
| `response: requests.Response` | `response: httpx.Response` |
| `JSONDecodeError` (undefined) | `from json import JSONDecodeError` |
| `response.reason` | `response.reason_phrase` |
| `response.url` (str in requests) | `str(response.url)` (httpx URL object) |

Remove `_config_dns` if present (requests-specific DNS workaround not needed with httpx).

### `_nominatim.py` Changes

| Before | After |
|--------|-------|
| `import requests` | `import httpx` |
| `requests.get(url, params=..., ...)` | `httpx.get(url, params=..., ...)` |
| `requests.Request("GET", url, params=params).prepare().url` | `str(httpx.Request("GET", url, params=params).url)` |
| `response.reason` | `response.reason_phrase` |

The retry logic on 429/504 stays structurally identical.

### Dependency Impact

- `requests` stays in `pyproject.toml` (still used by `_overpass.py`)
- `httpx` should already be listed (used by `elevation.py`)

## Component 4: Vectorize `_create_gdf_from_dfs` in `features.py`

### Problem

Three Python `for` loops iterate row-by-row over DataFrames to:
1. Decode WKB geometry
2. Expand the `tags` map into individual columns
3. Apply polygon refinement logic
4. Build a list of dicts → GeoDataFrame

### Solution

Replace with vectorized operations per element type:

**Nodes:**
```python
nodes_df["geometry"] = shapely.from_wkb(nodes_df["geometry"])
nodes_df["element"] = "node"
# Expand tags map to columns
tags_expanded = pd.json_normalize(nodes_df["tags"].apply(dict))
nodes_gdf = pd.concat([nodes_df[["id", "element"]], tags_expanded, nodes_df[["geometry"]]], axis=1)
```

**Ways:**
```python
ways_df["geometry"] = shapely.from_wkb(ways_df["geometry"])
ways_df["element"] = "way"
tags_expanded = pd.json_normalize(ways_df["tags"].apply(dict))
# Vectorized polygon refinement: for rows where SQL marked is_polygon=True
# but _should_be_polygon(tags) returns False, convert polygon → linestring
needs_fix = ways_df["is_polygon"] & ~tags_expanded.apply(_should_be_polygon, axis=1)
ways_df.loc[needs_fix, "geometry"] = ways_df.loc[needs_fix, "geometry"].apply(
    lambda g: LineString(g.exterior.coords)
)
# Filter to ways with matching query tags
```

**Relations:**
```python
relations_df["geometry"] = shapely.from_wkb(relations_df["geometry"])
relations_df["element"] = "relation"
tags_expanded = pd.json_normalize(relations_df["tags"].apply(dict))
```

**Assembly:**
```python
gdf = pd.concat([nodes_gdf, ways_gdf, relations_gdf], ignore_index=True)
gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=settings.default_crs)
gdf = gdf.set_index(["element", "id"]).sort_index()
```

### Adaptation to `_should_be_polygon`

The function signature stays the same (takes a dict of tags, returns bool). When called from the vectorized path, it's invoked via `DataFrame.apply(_should_be_polygon, axis=1)` on the expanded tags DataFrame (each row is a dict-like Series).

## Testing Strategy

- Existing tests in `tests/test_osmnx.py` should continue to pass — no public API changes.
- The connection manager should be tested for: same-path reuse, different-path reset, explicit `close()`.
- SQL escaping should be tested with values containing single quotes.
- The httpx migration should be verified by running the Nominatim-dependent tests (geocoding functions).

## Risks

- **Persistent connection state:** If something corrupts the connection or temp tables mid-session, subsequent calls will fail. Mitigation: `close()` resets everything, and the connection manager can detect stale connections.
- **Temp table collisions:** Since the connection persists, the intermediate temp tables (e.g., `area_nodes`, `filtered_ways`) from one call could collide with the next. Mitigation: `DROP TABLE IF EXISTS` at the start of each reader function.
- **httpx behavioral differences:** httpx is mostly API-compatible with requests for GET calls, but edge cases exist (redirect handling, timeout semantics). Mitigation: existing tests cover the Nominatim integration path.
