# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Changelog

After every executed plan, create a markdown file in the `changelog/` folder that summarizes the work done. Name the file `YYYY-MM-DD_short-description.md` (e.g. `2026-02-07_simplification-module-improvements.md`). Each entry should include: the date, which files were modified, a summary of all changes, verification results, and any known issues or incomplete work.

## Development Commands

### Environment Setup
```bash
# Install all dependencies including dev/test groups
uv sync --all-extras --all-groups

# Set up pre-commit hooks for code formatting and linting
pre-commit install
```

### Code Quality and Testing
```bash
# Run all pre-commit hooks (formatting, linting, type checking)
pre-commit run --all-files

# Run the complete test suite including linting, building, and testing
bash ./tests/lint_test.sh

# Run specific test modules
pytest tests/test_osmnx.py --verbose

# Run tests with coverage reporting
pytest --cov=ducknx --cov-report=term-missing:skip-covered

# Type checking only
mypy ducknx/

# Linting and formatting only
ruff check --fix
ruff format
```

### Building and Documentation
```bash
# Build the package
uv build

# Validate the built package
twine check --strict ./dist/*
validate-pyproject ./pyproject.toml

# Build documentation
mkdocs build --strict

# Serve documentation locally
mkdocs serve
```

## Code Architecture

ducknx is a Python package for working with OpenStreetMap street network data from local PBF files using DuckDB. All data reading goes through DuckDB SQL queries that return pandas DataFrames directly — there are no legacy JSON-dict code paths or Overpass API queries for data retrieval.

### Data Flow

1. User calls a public API function (e.g., `graph_from_bbox()`, `features_from_polygon()`)
2. The function delegates to `_pbf_reader` which executes DuckDB SQL against a local `.osm.pbf` file
3. DuckDB performs spatial filtering, tag filtering, JOINs, and geometry construction in SQL
4. Results come back as pandas DataFrames (nodes, ways, and optionally relations)
5. The calling module converts DataFrames into the final output (NetworkX graph or GeoDataFrame)

### Core Modules
- **`graph.py`**: Creates street network graphs from local OSM PBF data. Public API: `graph_from_bbox()`, `graph_from_point()`, `graph_from_address()`, `graph_from_place()`, `graph_from_polygon()`. Internal: `_create_graph_from_dfs()` builds a NetworkX graph from node/way DataFrames; `_add_paths()`, `_is_path_one_way()`, `_is_path_reversed()` handle edge directionality.
- **`_pbf_reader.py`**: Reads local OSM PBF files using DuckDB's spatial extension. Two main functions: `_read_pbf_network_duckdb()` returns node/way DataFrames for graph building; `_read_pbf_features_duckdb()` returns node/way/relation DataFrames with WKB geometries for feature extraction. Helper functions handle network type SQL filters and polygon-to-WKT conversion.
- **`features.py`**: Creates GeoDataFrames of OSM features (buildings, amenities, etc.). Public API: `features_from_bbox()`, `features_from_point()`, `features_from_address()`, `features_from_place()`, `features_from_polygon()`. Internal: `_create_gdf_from_dfs()` converts DataFrames to a GeoDataFrame; `_filter_features()` applies spatial and tag filtering; `_should_be_polygon()` determines geometry type for closed ways using OSM wiki rules.
- **`settings.py`**: Global configuration settings including `pbf_file_path` (must be set before use), `useful_tags_node`/`useful_tags_way`, network types, and CRS defaults.
- **`routing.py`**: Network routing and shortest path algorithms.
- **`plot.py`**: Visualization functions for graphs and networks.

### Utility Modules
- **`geocoder.py`**: Geocoding via Nominatim to convert place names to coordinates/polygons.
- **`utils.py` / `utils_geo.py`**: General utilities and geospatial helper functions.
- **`projection.py`**: Coordinate reference system transformations.
- **`distance.py`**: Distance calculations and spatial operations.
- **`bearing.py`**: Bearing and orientation calculations.
- **`elevation.py`**: Elevation data integration.

### Data Processing
- **`convert.py`**: Convert between different graph formats.
- **`simplification.py`**: Simplify and clean network topology.
- **`truncate.py`**: Truncate graphs to specific boundaries.
- **`stats.py`**: Calculate network statistics and metrics.
- **`io.py`**: Save/load graphs in various formats (GraphML, GeoPackage, OSM XML).

### Internal Modules
- **`_api_v1.py`**: Backwards compatibility layer re-exporting public functions at the package level.
- **`_nominatim.py`**: Nominatim geocoding API integration (HTTP-based).
- **`_osm_xml.py`**: OSM XML format processing — only used for *serialization* via `io.save_graph_xml()`. Not used for reading data.
- **`_errors.py`**: Custom exception classes (`InsufficientResponseError`, `GraphSimplificationError`, etc.).

### Key Internal Functions

**`_pbf_reader._read_pbf_network_duckdb(polygon, network_type, custom_filter, pbf_path)`**
- Loads PBF into a DuckDB temp table
- Finds nodes in the spatial area, filters ways by network type and spatial intersection
- Extracts `useful_tags_way` and `useful_tags_node` as individual columns
- Returns `(nodes_df, ways_df)` — nodes have `id, y, x, tag_columns`; ways have `osmid, refs, tag_columns`

**`_pbf_reader._read_pbf_features_duckdb(polygon, tags, pbf_path)`**
- Constructs geometry in SQL: nodes get `ST_Point`, ways get `ST_MakeLine` (promoted to polygon for closed ways), relations get full multipolygon assembly with hole subtraction
- Returns `(nodes_df, ways_df, relations_df)` — all with `id, tags, geometry` (WKB)

**`graph._create_graph_from_dfs(nodes_df, ways_df, bidirectional)`**
- Creates a NetworkX MultiDiGraph from DataFrames
- Deduplicates consecutive nodes, adds edge directionality via `_add_paths()`
- Adds great-circle edge lengths

**`features._create_gdf_from_dfs(nodes_df, ways_df, relations_df, polygon, tags)`**
- Converts WKB geometries to Shapely objects
- Applies `_should_be_polygon()` to refine closed-way geometry types using full OSM wiki rules
- Assembles a GeoDataFrame indexed by `(element_type, osm_id)`

## Code Style and Standards

- Uses **uv** for dependency management and building
- **Ruff** for linting and code formatting (line length: 100 characters)
- **MyPy** for strict type checking
- **NumPy-style docstrings** with numpydoc validation
- **Pre-commit hooks** enforce code quality standards
- All public APIs are exposed through `__init__.py` via `_api_v1.py` wildcard import

## Testing

- Main test file: `tests/test_osmnx.py`
- Uses **pytest** with coverage reporting and parallel test execution
- CI runs comprehensive checks including docs building, type checking, and multi-platform testing
- Test data located in `tests/input_data/`

## Key Dependencies

Core: `duckdb`, `geopandas`, `networkx`, `numpy`, `pandas`, `shapely`
Optional: `scipy`, `scikit-learn`, `rasterio`, `matplotlib` for extended functionality

## Configuration

Before using ducknx, you must configure the path to a local OSM PBF file:
```python
import ducknx as dx
dx.settings.pbf_file_path = "/path/to/your/data.osm.pbf"
```

All graph and feature queries read from this local file using DuckDB — no Overpass API calls are made for data retrieval. Nominatim is still used for geocoding place names to coordinates.


## ducknx-cleanup — Implemented 2026-04-18

**What was built:** Fixed bugs, hardened SQL construction, standardized on httpx, added DuckDB connection reuse, and vectorized DataFrame processing.
**Key decisions:**
- httpx over requests for active code (`_nominatim.py`, `_http.py`); `_overpass.py` keeps `requests` untouched
- Module-level cached DuckDB connection per PBF path (no thread safety — DuckDB is single-threaded by design)
- SQL escaping via `_escape_sql()` helper (not parameterization, since DuckDB doesn't support params in `CREATE TEMP TABLE AS`)
- Vectorized WKB parsing with `shapely.from_wkb()` instead of row-by-row iteration
**New dependencies:** None (httpx was already a dependency)
**Spec:** `docs/superpowers/specs/2026-04-17-ducknx-cleanup-design.md`
**Plan:** `docs/superpowers/plans/2026-04-17-ducknx-cleanup-plan.md`

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->
