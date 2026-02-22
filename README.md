# ducknx

A Python package to model, analyze, and visualize street networks and other geospatial features from OpenStreetMap PBF files using DuckDB.

ducknx is a fork of [OSMnx](https://github.com/gboeing/osmnx) that replaces the Overpass API with **DuckDB** for reading OSM data from local PBF files. All graph and feature queries are executed as SQL against a local `.osm.pbf` file using DuckDB's spatial extension. This provides high-performance, offline access to OpenStreetMap data without any network requests for data retrieval.

## Quick Start

```python
import ducknx as dx

# Point to a local PBF file (download from https://download.geofabrik.de/)
dx.settings.pbf_file_path = "/path/to/your/region.osm.pbf"

# Create a driving network graph
G = dx.graph_from_bbox((13.38, 52.51, 13.42, 52.53), network_type="drive")

# Extract building footprints as a GeoDataFrame
gdf = dx.features_from_bbox((13.38, 52.51, 13.42, 52.53), tags={"building": True})

# Query by place name (uses Nominatim for geocoding)
G = dx.graph_from_place("Piedmont, California, USA", network_type="walk")

# Convert graph to GeoDataFrames
nodes_gdf, edges_gdf = dx.graph_to_gdfs(G)
```

## Installation

Requires Python 3.10+.

```bash
pip install ducknx
```

To install with all optional dependencies:

```bash
pip install "ducknx[all]"
```

### Optional Extras

| Extra | Packages | Use case |
|-------|----------|----------|
| `entropy` | scipy | Street orientation entropy |
| `neighbors` | scikit-learn, scipy | Nearest-neighbor spatial analysis |
| `raster` | rasterio, rio-vrt | Elevation from local raster files |
| `visualization` | matplotlib | Static plotting |
| `all` | All of the above | Everything |

## How It Works

### Data Flow

1. User calls a public API function (e.g., `graph_from_bbox()`, `features_from_polygon()`)
2. The function delegates to the `_pbf_reader` module which executes DuckDB SQL against the local `.osm.pbf` file
3. DuckDB performs spatial filtering, tag filtering, JOINs, and geometry construction entirely in SQL
4. Results come back as pandas DataFrames (nodes, ways, and optionally relations)
5. The calling module converts DataFrames into the final output (NetworkX MultiDiGraph or GeoDataFrame)

### Graph Construction

The `graph` module builds a NetworkX MultiDiGraph from OSM way/node data:

- **Spatial filtering**: DuckDB finds all nodes within the query polygon, then finds ways that reference those nodes
- **Network type filtering**: SQL WHERE clauses filter ways by highway tags for the requested network type (`drive`, `walk`, `bike`, etc.)
- **Tag extraction**: Configured `useful_tags_way` and `useful_tags_node` are extracted as individual DataFrame columns
- **Topology**: One-way streets become single directed edges; bidirectional streets get two reciprocal directed edges
- **Simplification**: Non-intersection nodes are removed while preserving full edge geometry
- **Edge lengths**: Great-circle distances computed for all edges

### Feature Extraction

The `features` module builds a GeoDataFrame from OSM nodes, ways, and relations:

- **Nodes**: Geometry constructed as `ST_Point(lon, lat)`
- **Ways**: Geometry constructed as `ST_MakeLine` from referenced nodes; closed ways promoted to polygons based on OSM wiki tag rules
- **Relations**: Full multipolygon assembly with outer/inner ring handling and hole subtraction, all in SQL
- **Tag filtering**: Supports any OSM tag query (e.g., `{"building": True}`, `{"amenity": "restaurant"}`, `{"highway": ["primary", "secondary"]}`)

### Query Methods

All graph and feature functions support five query methods:

| Function suffix | Input | Example |
|----------------|-------|---------|
| `_from_bbox` | `(west, south, east, north)` | `dx.graph_from_bbox((13.38, 52.51, 13.42, 52.53))` |
| `_from_point` | `(lat, lon)` + distance | `dx.graph_from_point((52.52, 13.40), dist=1000)` |
| `_from_address` | Address string + distance | `dx.graph_from_address("Berlin Mitte", dist=500)` |
| `_from_place` | Place name | `dx.graph_from_place("Piedmont, CA, USA")` |
| `_from_polygon` | Shapely Polygon | `dx.graph_from_polygon(my_polygon)` |

The `_from_address` and `_from_place` variants use the Nominatim geocoding API (HTTP) to resolve names to coordinates/polygons. All other variants are fully offline.

## Modules

### Core

| Module | Description |
|--------|-------------|
| `graph` | Create street network MultiDiGraphs: `graph_from_bbox()`, `graph_from_point()`, `graph_from_address()`, `graph_from_place()`, `graph_from_polygon()` |
| `features` | Create GeoDataFrames of OSM features: `features_from_bbox()`, `features_from_point()`, `features_from_address()`, `features_from_place()`, `features_from_polygon()` |
| `settings` | Global configuration: `pbf_file_path`, `useful_tags_node`, `useful_tags_way`, network types, CRS defaults |
| `routing` | Shortest path solving, speed imputation, travel time calculation |
| `plot` | Static visualization of graphs, routes, figure-ground diagrams, orientation roses |

### Analysis & Processing

| Module | Description |
|--------|-------------|
| `stats` | Network statistics: intersection density, circuity, node degree, betweenness centrality |
| `bearing` | Street bearing/orientation analysis and entropy |
| `distance` | Nearest-node/edge queries via spatial index |
| `elevation` | Attach node elevations from raster files or Google Elevation API; compute edge grades |
| `simplification` | Graph simplification (merge non-intersection nodes) and intersection consolidation |
| `convert` | Convert MultiDiGraph to/from GeoDataFrames, DiGraph, or MultiGraph |
| `projection` | Project graphs/geometries to UTM or other CRS |
| `truncate` | Truncate graphs by bounding box or polygon |

### I/O

| Module | Description |
|--------|-------------|
| `io` | Save/load graphs as GraphML, GeoPackage, or OSM XML |
| `geocoder` | Geocode place names/addresses via Nominatim |

## Configuration

```python
import ducknx as dx

# Required: path to local OSM PBF file
dx.settings.pbf_file_path = "/path/to/region.osm.pbf"

# Control which OSM tags become graph attributes
dx.settings.useful_tags_way = ["highway", "name", "maxspeed", "oneway", "lanes"]
dx.settings.useful_tags_node = ["highway", "junction", "railway", "ref"]

# Network types: "drive", "walk", "bike", "all", "all_private", "drive_service"
G = dx.graph_from_bbox(bbox, network_type="drive")

# Custom Overpass-style filter for full control
G = dx.graph_from_bbox(bbox, custom_filter='["highway"~"motorway|trunk"]')

# Logging
dx.settings.log_console = True
dx.settings.log_file = True
```

## Key Dependencies

| Package | Role |
|---------|------|
| `duckdb` (>=1.3.0) | SQL engine for reading PBF files via spatial extension |
| `geopandas` (>=1.1.0) | GeoDataFrame output for features |
| `networkx` (>=2.5) | MultiDiGraph data structure for street networks |
| `numpy` (>=1.24) | Numerical operations |
| `pandas` (>=2.0) | DataFrame intermediary between DuckDB and output |
| `pyarrow` (>=23.0.0) | Efficient data transfer from DuckDB to pandas |
| `shapely` (>=2.0) | Geometry objects and spatial operations |

## Differences from Upstream OSMnx

| | OSMnx | ducknx |
|---|-------|--------|
| Data source | Overpass API (HTTP) | Local PBF file (DuckDB SQL) |
| Network required | Yes (for data retrieval) | No (fully offline, except Nominatim geocoding) |
| Geometry construction | Python-side from JSON | SQL-side (`ST_MakeLine`, `ST_MakePolygon`, etc.) |
| Relation handling | Overpass assembles geometry | DuckDB assembles multipolygons with hole subtraction |
| Import alias | `import osmnx as ox` | `import ducknx as dx` |
| XML reading | `graph_from_xml()` available | Removed |
| PBF files | Via Overpass, not local | `dx.settings.pbf_file_path` must be set |

## Development

```bash
# Clone and install
git clone <repo-url>
cd duck_osmnx
uv sync --all-extras --all-groups
pre-commit install

# Run tests
pytest tests/test_osmnx.py --verbose

# Lint and format
pre-commit run --all-files

# Type check
mypy ducknx/

# Build docs
mkdocs build --strict
mkdocs serve
```

## Citation

If you use ducknx in your work, please cite the original paper:

Boeing, G. (2025). Modeling and Analyzing Urban Networks and Amenities with OSMnx. *Geographical Analysis*, published online ahead of print. doi:[10.1111/gean.70009](https://doi.org/10.1111/gean.70009)

## License

MIT. OpenStreetMap data is subject to the [ODbL](https://www.openstreetmap.org/copyright/).
