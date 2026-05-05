# 2026-05-05 — Arrow features API (Phase 1)

First slice of the breaking pandas → Arrow public-API redesign (epic
`duck_osmnx-2j3`).  This phase rewrites only the `features_from_*` family;
graph/io/simplification/etc. remain pandas-backed for now.

## Files modified

- `ducknx/features.py` — full rewrite: returns `pa.Table` with
  `geoarrow.wkb` geometry and `map<string,string>` tags column
- `tests/test_features_vectorized.py` — rewritten against the Arrow API
- `benchmarks/bench_pipeline.py` — `features_gdf` stage renamed to
  `features_table` and points at `_build_table`
- `CLAUDE.md` — features module description updated
- `pyproject.toml` — adds `geoarrow-pyarrow`, `polars` runtime deps

## Why

`features_from_*` previously called `pd.DataFrame(tags_series.tolist())`
which exploded sparse OSM tag dicts into a dense column-per-key
DataFrame.  At Berlin-large bbox scale this materialized ~37 GB of
mostly-null cells and took ~94 s.  Tags as a single `map` column
preserves the data without the explosion.

## New schema

| column     | type                                              |
|------------|---------------------------------------------------|
| `element`  | `dictionary<string>` (`"node"`/`"way"`/`"relation"`) |
| `id`       | `int64`                                           |
| `tags`     | `map<string, string>`                             |
| `geometry` | `binary` + `ARROW:extension:name=geoarrow.wkb` + CRS |

CRS travels in the geometry field's metadata as
`{"crs":"EPSG:4326","edges":"planar"}`.

## Verification

`tests/test_features_vectorized.py` (4 cases) all pass.  Wider
regression run (`test_duckdb`, `test_http`, `test_rustworkx_backend`,
`test_vectorized_graph`): 30 / 30 pass.

End-to-end smoke against `berlin-latest.osm.pbf`:

| scale  | rows    | before time / peak    | after time / peak  |
|--------|---------|-----------------------|--------------------|
| small  | 339     | 1.10 s / 53.9 MB      | 1.73 s / 2.3 MB    |
| medium | 48 989  | 6.60 s / 1 994 MB     | 2.18 s / 18.4 MB   |
| large  | 534 933 | 96.55 s / 37 594 MB   | 4.73 s / 152 MB    |

(`before` = combined `duckdb_query_features` + `features_gdf` from the
prior bench; `after` = single `features_from_bbox` call.  Memory peak is
`tracemalloc` on the user-facing call.)

## Known follow-ups

- **Phase 2** (`duck_osmnx-c7i`): `convert.graph_to_gdfs` /
  `graph_from_gdfs` Arrow API.
- **Phase 3** (`duck_osmnx-pq4`): `io` module via pyogrio Arrow.
- **Phase 4** (`duck_osmnx-jgi`): simplification, stats, truncate
  rewrites.
- **Phase 5** (`duck_osmnx-04c`): routing, projection, elevation, plot,
  `_osm_xml`.
- **Phase 6** (`duck_osmnx-562`): drop pandas + geopandas runtime deps,
  bump major version to 3.0.0, rewrite `tests/test_osmnx.py`.

`pandas`/`geopandas` remain as runtime deps until Phase 6; this
intermediate state is intentional so each phase ships independently.
