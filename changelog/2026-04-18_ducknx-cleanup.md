# ducknx Cleanup & Improvement — 2026-04-18

## Files Modified
- `ducknx/_duckdb.py` (new) — DuckDB connection lifecycle manager
- `ducknx/_pbf_reader.py` — Integrated connection manager, extracted `_build_tag_filter` with SQL escaping, removed `_polygon_to_wkt`, dedented feature SQL blocks
- `ducknx/_http.py` — Already migrated to httpx (no changes needed)
- `ducknx/_nominatim.py` — Migrated from `requests` to `httpx`
- `ducknx/features.py` — Vectorized `_create_gdf_from_dfs` with `shapely.from_wkb` and pandas ops
- `tests/test_duckdb.py` (new) — 5 tests for connection manager
- `tests/test_pbf_reader.py` (new) — 6 tests for tag filter and network filter
- `tests/test_features_vectorized.py` (new) — 2 tests for vectorized GeoDataFrame construction

## Summary of Changes
1. **DuckDB connection reuse:** New `_duckdb.py` module caches connections per PBF path, avoiding repeated `INSTALL spatial` / `LOAD spatial` / `ST_ReadOSM` calls.
2. **SQL injection hardening:** All string interpolation in SQL uses `_escape_sql()` to double single quotes. Tag filters extracted into `_build_tag_filter()`.
3. **httpx migration:** `_nominatim.py` switched from `requests` to `httpx`. `_overpass.py` left untouched.
4. **Vectorized GeoDataFrame:** `_create_gdf_from_dfs` now uses `shapely.from_wkb()` for batch WKB parsing instead of row-by-row `wkb.loads()`.
5. **Removed `_polygon_to_wkt`:** Was just `return polygon.wkt` — inlined everywhere.

## Verification
- All 13 new tests pass
- Module imports correctly
- No stale `import requests` in active code (only `_overpass.py`)
- `_polygon_to_wkt` fully removed

## Known Issues
- `_overpass.py` still uses `requests` (intentionally out of scope)
- No pre-commit config (`.pre-commit-config.yaml` missing from repo)
- Thread safety not guaranteed for DuckDB connection manager (by design)
