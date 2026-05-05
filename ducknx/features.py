"""
Download and create Arrow tables of OpenStreetMap geospatial features.

Retrieve points of interest, building footprints, transit lines/stops, or any
other map features from OSM, including their geometries and attribute data,
then construct a PyArrow Table of them. You can use this module to query for
nodes, ways, and relations (the latter of type "multipolygon" or "boundary"
only) by passing a dictionary of desired OSM tags.

The returned table has columns:

- ``element``  — dictionary<string>  ("node" / "way" / "relation")
- ``id``       — int64               (OSM element ID)
- ``tags``     — map<string, string> (full tag set; not exploded per key)
- ``geometry`` — geoarrow.wkb        (WKB extension type with CRS metadata)

For more details, see https://wiki.openstreetmap.org/wiki/Map_features and
https://wiki.openstreetmap.org/wiki/Elements

Refer to the Getting Started guide for usage limitations.
"""

from __future__ import annotations

import logging as lg

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import shapely
from shapely import MultiPolygon
from shapely import Polygon
from shapely import STRtree

from . import _pbf_reader
from . import geocoder
from . import settings
from . import utils
from . import utils_geo
from ._errors import InsufficientResponseError

# OSM tags to determine if closed ways should be polygons, based on JSON from
# https://wiki.openstreetmap.org/wiki/Overpass_turbo/Polygon_Features
# Used by `_pbf_reader._build_polygon_case_sql` to classify polygon vs line in
# DuckDB; kept here as the canonical source of truth.
_POLYGON_FEATURES: dict[str, dict[str, str | set[str]]] = {
    "aeroway": {"polygon": "blocklist", "values": {"taxiway"}},
    "amenity": {"polygon": "all"},
    "area": {"polygon": "all"},
    "area:highway": {"polygon": "all"},
    "barrier": {
        "polygon": "passlist",
        "values": {"city_wall", "ditch", "hedge", "retaining_wall", "spikes"},
    },
    "boundary": {"polygon": "all"},
    "building": {"polygon": "all"},
    "building:part": {"polygon": "all"},
    "craft": {"polygon": "all"},
    "golf": {"polygon": "all"},
    "highway": {"polygon": "passlist", "values": {"elevator", "escape", "rest_area", "services"}},
    "historic": {"polygon": "all"},
    "indoor": {"polygon": "all"},
    "landuse": {"polygon": "all"},
    "leisure": {"polygon": "all"},
    "man_made": {"polygon": "blocklist", "values": {"cutline", "embankment", "pipeline"}},
    "military": {"polygon": "all"},
    "natural": {
        "polygon": "blocklist",
        "values": {"arete", "cliff", "coastline", "ridge", "tree_row"},
    },
    "office": {"polygon": "all"},
    "place": {"polygon": "all"},
    "power": {"polygon": "passlist", "values": {"generator", "plant", "substation", "transformer"}},
    "public_transport": {"polygon": "all"},
    "railway": {
        "polygon": "passlist",
        "values": {"platform", "roundhouse", "station", "turntable"},
    },
    "ruins": {"polygon": "all"},
    "shop": {"polygon": "all"},
    "tourism": {"polygon": "all"},
    "waterway": {"polygon": "passlist", "values": {"boatyard", "dam", "dock", "riverbank"}},
}


def _should_be_polygon(way_tags: dict[str, str]) -> bool:
    """
    Determine if a closed way should be represented as a Polygon.

    Mirrors the SQL CASE expression in ``_pbf_reader._build_polygon_case_sql``;
    retained as a Python reference to validate the SQL implementation.

    Parameters
    ----------
    way_tags
        OSM tags for the way.

    Returns
    -------
    is_polygon
        ``True`` if the closed way should be a Polygon, otherwise ``False``.
    """
    if way_tags.get("area") == "no":
        return False
    for tag in set(way_tags.keys()) & _POLYGON_FEATURES.keys():
        rule = _POLYGON_FEATURES[tag]["polygon"]
        values = _POLYGON_FEATURES[tag].get("values", set())
        if (
            rule == "all"
            or (rule == "passlist" and way_tags[tag] in values)
            or (rule == "blocklist" and way_tags[tag] not in values)
        ):
            return True
    return False


def _wkb_field(crs: str) -> pa.Field:
    """
    Return a PyArrow ``geometry`` field with geoarrow.wkb extension metadata.

    The geometry column is encoded as ``binary`` with extension type metadata
    following the GeoArrow specification, allowing downstream consumers
    (DuckDB Spatial, GDAL ≥3.8, lonboard, etc.) to interpret it natively.

    Parameters
    ----------
    crs
        Coordinate reference system as an authority code string
        (e.g. ``"EPSG:4326"``) used as the ``crs`` field metadata.

    Returns
    -------
    field
        Arrow field named ``geometry`` carrying the geoarrow.wkb extension.
    """
    metadata = {
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": (
            b'{"crs":"' + crs.encode("ascii") + b'","edges":"planar"}'
        ),
    }
    return pa.field("geometry", pa.binary(), nullable=True, metadata=metadata)


def features_from_bbox(
    bbox: tuple[float, float, float, float],
    tags: dict[str, bool | str | list[str]],
) -> pa.Table:
    """
    Download OSM features within a lat-lon bounding box.

    Parameters
    ----------
    bbox
        Bounding box as ``(left, bottom, right, top)`` in EPSG:4326 degrees.
    tags
        Tags for finding elements. Keys are OSM tag names; values may be
        ``True`` (any value), a string (exact match), or a list of strings.

    Returns
    -------
    table
        Arrow table of OSM features.
    """
    polygon = utils_geo.bbox_to_poly(bbox)
    return features_from_polygon(polygon, tags)


def features_from_point(
    center_point: tuple[float, float],
    tags: dict[str, bool | str | list[str]],
    dist: float,
) -> pa.Table:
    """
    Download OSM features within some distance of a lat-lon point.

    Parameters
    ----------
    center_point
        ``(lat, lon)`` center point in EPSG:4326 degrees.
    tags
        Tags for finding elements (see ``features_from_bbox``).
    dist
        Distance in meters from ``center_point`` for the bounding box.

    Returns
    -------
    table
        Arrow table of OSM features.
    """
    bbox = utils_geo.bbox_from_point(center_point, dist)
    return features_from_bbox(bbox, tags)


def features_from_address(
    address: str,
    tags: dict[str, bool | str | list[str]],
    dist: float,
) -> pa.Table:
    """
    Download OSM features within some distance of an address.

    Parameters
    ----------
    address
        Address to geocode as the query center.
    tags
        Tags for finding elements (see ``features_from_bbox``).
    dist
        Distance in meters from the geocoded address.

    Returns
    -------
    table
        Arrow table of OSM features.
    """
    center_point = geocoder.geocode(address)
    return features_from_point(center_point, tags, dist)


def features_from_place(
    query: str | dict[str, str] | list[str | dict[str, str]],
    tags: dict[str, bool | str | list[str]],
    *,
    which_result: int | None | list[int | None] = None,
) -> pa.Table:
    """
    Download OSM features within the boundaries of some place(s).

    Parameters
    ----------
    query
        Place query/queries to geocode to a polygon boundary.
    tags
        Tags for finding elements (see ``features_from_bbox``).
    which_result
        Which geocoding result to use; ``None`` auto-selects.

    Returns
    -------
    table
        Arrow table of OSM features.
    """
    place_tbl = geocoder.geocode_to_arrow(query, which_result=which_result)
    geoms = shapely.from_wkb(place_tbl.column("geometry").to_pylist())
    polygon = shapely.union_all(geoms)
    utils.log("Constructed place geometry polygon(s) to query", level=lg.INFO)
    return features_from_polygon(polygon, tags)


def features_from_polygon(
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
) -> pa.Table:
    """
    Download OSM features within the boundaries of a (Multi)Polygon.

    Parameters
    ----------
    polygon
        Geometry within which to retrieve features (EPSG:4326 degrees).
    tags
        Tags for finding elements (see ``features_from_bbox``).

    Returns
    -------
    table
        Arrow table of OSM features.
    """
    if not polygon.is_valid:
        msg = "The geometry of `polygon` is invalid."
        raise ValueError(msg)
    if not isinstance(polygon, (Polygon, MultiPolygon)):
        msg = (
            "Boundaries must be a Polygon or MultiPolygon. If you requested "
            "`features_from_place`, ensure your query geocodes to a Polygon "
            "or MultiPolygon. See the documentation for details."
        )
        raise TypeError(msg)
    if not settings.pbf_file_path:
        msg = "No PBF file path configured. Set settings.pbf_file_path to a local OSM PBF file."
        raise ValueError(msg)

    nodes_tbl, ways_tbl, relations_tbl = _pbf_reader._read_pbf_features_duckdb(
        polygon, tags, settings.pbf_file_path,
    )
    return _build_table(nodes_tbl, ways_tbl, relations_tbl, polygon, tags)


def _coalesce_element(
    element: str,
    tbl: pa.Table,
    tags_map_type: pa.DataType | None,
) -> pa.Table | None:
    """
    Normalize a per-element Arrow table into the unified output schema.

    Drops Arrow rows with null/empty geometries and prepends an ``element``
    column. The input ``tbl`` must have ``id``, ``tags``, ``geometry`` columns
    (the schema produced by ``_pbf_reader``). Returns ``None`` when empty.

    Parameters
    ----------
    element
        Element type literal: ``"node"``, ``"way"``, or ``"relation"``.
    tbl
        Source Arrow table from DuckDB.
    tags_map_type
        Canonical map type to cast the ``tags`` column to (so all element
        sub-tables share an identical ``tags`` field for concatenation).

    Returns
    -------
    out
        Normalized table, or ``None`` if the input has no rows.
    """
    if tbl.num_rows == 0:
        return None
    geom = tbl.column("geometry")
    valid = pc.is_valid(geom)
    nonempty = pc.greater(pc.binary_length(pc.fill_null(geom, b"")), 0)
    keep = pc.and_(valid, nonempty)
    if pc.sum(pc.cast(keep, pa.int64())).as_py() == 0:
        return None
    tbl = tbl.filter(keep)
    tags_col = tbl.column("tags")
    if tags_map_type is not None and not tags_col.type.equals(tags_map_type):
        tags_col = tags_col.cast(tags_map_type)
    return pa.table(
        {
            "element": pa.array([element] * tbl.num_rows, type=pa.dictionary(pa.int8(), pa.string())),
            "id": tbl.column("id").cast(pa.int64()),
            "tags": tags_col,
            "geometry": tbl.column("geometry").cast(pa.binary()),
        },
    )


def _filter_arrow(
    tbl: pa.Table,
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
) -> pa.Table:
    """
    Filter the unified Arrow table by spatial polygon and tag query.

    Spatial filtering uses a shapely STRtree built directly on WKB-decoded
    geometries (no GeoPandas). Tag filtering operates on the
    ``map<string,string>`` column via PyArrow compute kernels.

    Parameters
    ----------
    tbl
        Unified Arrow table (``element``, ``id``, ``tags``, ``geometry``).
    polygon
        Spatial boundary; if empty, no spatial filter is applied.
    tags
        Tag query; if empty, no tag filter is applied.

    Returns
    -------
    tbl
        Filtered Arrow table.
    """
    if tbl.num_rows == 0:
        return tbl

    # spatial filter
    if not polygon.is_empty:
        wkb_arr = np.asarray(tbl.column("geometry").to_pylist(), dtype=object)
        geoms = shapely.from_wkb(wkb_arr)
        # repair invalid geoms once before the predicate test
        invalid = ~shapely.is_valid(geoms)
        if invalid.any():
            geoms = np.where(invalid, shapely.make_valid(geoms), geoms)
        tree = STRtree(geoms)
        # query returns indices of geometries whose bounds intersect; refine via predicate
        candidates = tree.query(polygon, predicate="intersects")
        spatial_mask = np.zeros(len(geoms), dtype=bool)
        spatial_mask[candidates] = True
        # write repaired geometries back so downstream consumers receive valid WKB
        if invalid.any():
            new_wkb = shapely.to_wkb(geoms)
            tbl = tbl.set_column(
                tbl.schema.get_field_index("geometry"),
                tbl.schema.field("geometry"),
                pa.array(new_wkb, type=pa.binary()),
            )
        tbl = tbl.filter(pa.array(spatial_mask))

    if tbl.num_rows == 0:
        msg = "No matching features. Check query location, tags, and log."
        raise InsufficientResponseError(msg)

    # tag filter — operate on the map column with pa.compute
    if len(tags) > 0:
        match_mask = None
        for key, value in tags.items():
            looked = pc.map_lookup(tbl.column("tags"), pa.scalar(key), "first")
            if value is True:
                cond = pc.is_valid(looked)
            elif isinstance(value, str):
                cond = pc.equal(looked, pa.scalar(value))
            elif isinstance(value, list):
                cond = pc.is_in(looked, pa.array(value, type=pa.string()))
            else:
                cond = pc.is_null(looked)
            match_mask = cond if match_mask is None else pc.or_(match_mask, cond)
        match_mask = pc.fill_null(match_mask, fill_value=False)
        tbl = tbl.filter(match_mask)

    if tbl.num_rows == 0:
        msg = "No matching features. Check query location, tags, and log."
        raise InsufficientResponseError(msg)

    utils.log(f"{tbl.num_rows:,} features in the final Arrow table", level=lg.INFO)
    return tbl


def _build_table(
    nodes_tbl: pa.Table,
    ways_tbl: pa.Table,
    relations_tbl: pa.Table,
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
) -> pa.Table:
    """
    Concatenate per-element Arrow tables into the unified features schema.

    Parameters
    ----------
    nodes_tbl, ways_tbl, relations_tbl
        Per-element tables from ``_pbf_reader``.
    polygon
        Spatial boundary for final filtering.
    tags
        Tag query for final filtering.

    Returns
    -------
    table
        Unified, filtered Arrow table with geoarrow.wkb geometry.
    """
    # pick a canonical tags map type from whichever element table has rows
    tags_type: pa.DataType | None = None
    for cand in (nodes_tbl, ways_tbl, relations_tbl):
        if cand.num_rows > 0:
            tags_type = cand.schema.field("tags").type
            break
    if tags_type is None:
        # all empty
        msg = "No matching features. Check query location, tags, and log."
        raise InsufficientResponseError(msg)

    parts: list[pa.Table] = []
    for elem, tbl in (
        ("node", nodes_tbl),
        ("way", ways_tbl),
        ("relation", relations_tbl),
    ):
        normalized = _coalesce_element(elem, tbl, tags_type)
        if normalized is not None:
            parts.append(normalized)

    if not parts:
        msg = "No matching features. Check query location, tags, and log."
        raise InsufficientResponseError(msg)

    combined = pa.concat_tables(parts, promote_options="default")

    # apply spatial + tag filters
    combined = _filter_arrow(combined, polygon, tags)

    # attach geoarrow.wkb extension type to the geometry column
    geom_field = _wkb_field(settings.default_crs.upper())
    fields = list(combined.schema)
    geom_idx = combined.schema.get_field_index("geometry")
    fields[geom_idx] = geom_field
    new_schema = pa.schema(fields, metadata=combined.schema.metadata)
    return combined.cast(new_schema, safe=False)
