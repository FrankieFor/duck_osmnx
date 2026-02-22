"""
Download and create GeoDataFrames from OpenStreetMap geospatial features.

Retrieve points of interest, building footprints, transit lines/stops, or any
other map features from OSM, including their geometries and attribute data,
then construct a GeoDataFrame of them. You can use this module to query for
nodes, ways, and relations (the latter of type "multipolygon" or "boundary"
only) by passing a dictionary of desired OSM tags.

For more details, see https://wiki.openstreetmap.org/wiki/Map_features and
https://wiki.openstreetmap.org/wiki/Elements

Refer to the Getting Started guide for usage limitations.
"""

from __future__ import annotations

import logging as lg
from typing import Any

import geopandas as gpd
import pandas as pd
from shapely import LineString
from shapely import MultiPolygon
from shapely import Polygon
from shapely import wkb
from shapely.errors import GEOSException

from . import _pbf_reader
from . import geocoder
from . import settings
from . import utils
from . import utils_geo
from ._errors import InsufficientResponseError

# OSM tags to determine if closed ways should be polygons, based on JSON from
# https://wiki.openstreetmap.org/wiki/Overpass_turbo/Polygon_Features
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


def features_from_bbox(
    bbox: tuple[float, float, float, float],
    tags: dict[str, bool | str | list[str]],
) -> gpd.GeoDataFrame:
    """
    Download OSM features within a lat-lon bounding box.

    This function searches for features using tags. For more details, see:
    https://wiki.openstreetmap.org/wiki/Map_features

    Parameters
    ----------
    bbox
        Bounding box as `(left, bottom, right, top)`. Coordinates should be in
        unprojected latitude-longitude degrees (EPSG:4326).
    tags
        Tags for finding elements in the selected area. Results are the union,
        not intersection of the tags and each result matches at least one tag.
        The keys are OSM tags (e.g. `building`, `landuse`, `highway`, etc) and
        the values can be either `True` to retrieve all elements matching the
        tag, or a string to retrieve a single `tag:value` combination, or a
        list of strings to retrieve multiple values for the tag. For example,
        `tags = {'building': True}` would return all buildings in the area.
        Or, `tags = {'amenity':True, 'landuse':['retail','commercial'],
        'highway':'bus_stop'}` would return all amenities, any landuse=retail,
        any landuse=commercial, and any highway=bus_stop.

    Returns
    -------
    gdf
        The features, multi-indexed by element type and OSM ID.
    """
    # convert bbox to polygon then create GeoDataFrame of features within it
    polygon = utils_geo.bbox_to_poly(bbox)
    return features_from_polygon(polygon, tags)


def features_from_point(
    center_point: tuple[float, float],
    tags: dict[str, bool | str | list[str]],
    dist: float,
) -> gpd.GeoDataFrame:
    """
    Download OSM features within some distance of a lat-lon point.

    This function searches for features using tags. For more details, see:
    https://wiki.openstreetmap.org/wiki/Map_features

    Parameters
    ----------
    center_point
        The `(lat, lon)` center point around which to retrieve the features.
        Coordinates should be in unprojected latitude-longitude degrees
        (EPSG:4326).
    tags
        Tags for finding elements in the selected area. Results are the union,
        not intersection of the tags and each result matches at least one tag.
        The keys are OSM tags (e.g. `building`, `landuse`, `highway`, etc) and
        the values can be either `True` to retrieve all elements matching the
        tag, or a string to retrieve a single `tag:value` combination, or a
        list of strings to retrieve multiple values for the tag. For example,
        `tags = {'building': True}` would return all buildings in the area.
        Or, `tags = {'amenity':True, 'landuse':['retail','commercial'],
        'highway':'bus_stop'}` would return all amenities, any landuse=retail,
        any landuse=commercial, and any highway=bus_stop.
    dist
        Distance in meters from `center_point` to create a bounding box to
        query.

    Returns
    -------
    gdf
        The features, multi-indexed by element type and OSM ID.
    """
    # create bbox from point and dist, then create gdf of features within it
    bbox = utils_geo.bbox_from_point(center_point, dist)
    return features_from_bbox(bbox, tags)


def features_from_address(
    address: str,
    tags: dict[str, bool | str | list[str]],
    dist: float,
) -> gpd.GeoDataFrame:
    """
    Download OSM features within some distance of an address.

    This function searches for features using tags. For more details, see:
    https://wiki.openstreetmap.org/wiki/Map_features

    Parameters
    ----------
    address
        The address to geocode and use as the center point around which to
        retrieve the features.
    tags
        Tags for finding elements in the selected area. Results are the union,
        not intersection of the tags and each result matches at least one tag.
        The keys are OSM tags (e.g. `building`, `landuse`, `highway`, etc) and
        the values can be either `True` to retrieve all elements matching the
        tag, or a string to retrieve a single `tag:value` combination, or a
        list of strings to retrieve multiple values for the tag. For example,
        `tags = {'building': True}` would return all buildings in the area.
        Or, `tags = {'amenity':True, 'landuse':['retail','commercial'],
        'highway':'bus_stop'}` would return all amenities, any landuse=retail,
        any landuse=commercial, and any highway=bus_stop.
    dist
        Distance in meters from `address` to create a bounding box to query.

    Returns
    -------
    gdf
        The features, multi-indexed by element type and OSM ID.
    """
    # geocode the address to a point, then create gdf of features around it
    center_point = geocoder.geocode(address)
    return features_from_point(center_point, tags, dist)


def features_from_place(
    query: str | dict[str, str] | list[str | dict[str, str]],
    tags: dict[str, bool | str | list[str]],
    *,
    which_result: int | None | list[int | None] = None,
) -> gpd.GeoDataFrame:
    """
    Download OSM features within the boundaries of some place(s).

    The query must be geocodable and OSM must have polygon boundaries for the
    geocode result. If OSM does not have a polygon for this place, you can
    instead get features within it using the `features_from_address`
    function, which geocodes the place name to a point and gets the features
    within some distance of that point.

    If OSM does have polygon boundaries for this place but you're not finding
    it, try to vary the query string, pass in a structured query dict, or vary
    the `which_result` argument to use a different geocode result. If you know
    the OSM ID of the place, you can retrieve its boundary polygon using the
    `geocode_to_gdf` function, then pass it to the `features_from_polygon`
    function.

    This function searches for features using tags. For more details, see:
    https://wiki.openstreetmap.org/wiki/Map_features

    Parameters
    ----------
    query
        The query or queries to geocode to retrieve place boundary polygon(s).
    tags
        Tags for finding elements in the selected area. Results are the union,
        not intersection of the tags and each result matches at least one tag.
        The keys are OSM tags (e.g. `building`, `landuse`, `highway`, etc) and
        the values can be either `True` to retrieve all elements matching the
        tag, or a string to retrieve a single `tag:value` combination, or a
        list of strings to retrieve multiple values for the tag. For example,
        `tags = {'building': True}` would return all buildings in the area.
        Or, `tags = {'amenity':True, 'landuse':['retail','commercial'],
        'highway':'bus_stop'}` would return all amenities, any landuse=retail,
        any landuse=commercial, and any highway=bus_stop.
    which_result
        Which search result to return. If None, auto-select the first
        (Multi)Polygon or raise an error if OSM doesn't return one.

    Returns
    -------
    gdf
        The features, multi-indexed by element type and OSM ID.
    """
    # extract the geometry from the GeoDataFrame to use in query
    polygon = geocoder.geocode_to_gdf(query, which_result=which_result).union_all()
    msg = "Constructed place geometry polygon(s) to query"
    utils.log(msg, level=lg.INFO)

    # create GeoDataFrame using this polygon(s) geometry
    return features_from_polygon(polygon, tags)


def features_from_polygon(
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
) -> gpd.GeoDataFrame:
    """
    Download OSM features within the boundaries of a (Multi)Polygon.

    This function searches for features using tags. For more details, see:
    https://wiki.openstreetmap.org/wiki/Map_features

    Parameters
    ----------
    polygon
        The geometry within which to retrieve features. Coordinates should be
        in unprojected latitude-longitude degrees (EPSG:4326).
    tags
        Tags for finding elements in the selected area. Results are the union,
        not intersection of the tags and each result matches at least one tag.
        The keys are OSM tags (e.g. `building`, `landuse`, `highway`, etc) and
        the values can be either `True` to retrieve all elements matching the
        tag, or a string to retrieve a single `tag:value` combination, or a
        list of strings to retrieve multiple values for the tag. For example,
        `tags = {'building': True}` would return all buildings in the area.
        Or, `tags = {'amenity':True, 'landuse':['retail','commercial'],
        'highway':'bus_stop'}` would return all amenities, any landuse=retail,
        any landuse=commercial, and any highway=bus_stop.

    Returns
    -------
    gdf
        The features, multi-indexed by element type and OSM ID.
    """
    # verify that the geometry is valid and is a Polygon/MultiPolygon
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

    nodes_df, ways_df, relations_df = _pbf_reader._read_pbf_features_duckdb(
        polygon, tags, settings.pbf_file_path
    )
    return _create_gdf_from_dfs(nodes_df, ways_df, relations_df, polygon, tags)


def _filter_features(
    gdf: gpd.GeoDataFrame,
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
) -> gpd.GeoDataFrame:
    """
    Filter features GeoDataFrame by spatial boundaries and query tags.

    If the `polygon` and `tags` arguments are empty objects, the final
    GeoDataFrame will not be filtered accordingly.

    Parameters
    ----------
    gdf
        Original GeoDataFrame of features.
    polygon
        If not empty, the spatial boundaries to filter the GeoDataFrame.
    tags
        If not empty, the query tags to filter the GeoDataFrame.

    Returns
    -------
    gdf
        Filtered GeoDataFrame of features.
    """
    # remove any null or empty geometries then fix any invalid geometries
    gdf = gdf[~(gdf["geometry"].isna() | gdf["geometry"].is_empty)]
    gdf.loc[:, "geometry"] = gdf["geometry"].make_valid()

    # retain rows with geometries that intersect the polygon
    if polygon.is_empty:
        geom_filter = pd.Series(data=True, index=gdf.index)
    else:
        idx = utils_geo._intersect_index_quadrats(gdf["geometry"], polygon)
        geom_filter = gdf.index.isin(idx)

    # retain rows that have any of their tag filters satisfied
    if len(tags) == 0:
        tags_filter = pd.Series(data=True, index=gdf.index)
    else:
        tags_filter = pd.Series(data=False, index=gdf.index)
        for col in set(gdf.columns) & tags.keys():
            value = tags[col]
            if value is True:
                tags_filter |= gdf[col].notna()
            elif isinstance(value, str):
                tags_filter |= gdf[col] == value
            elif isinstance(value, list):
                tags_filter |= gdf[col].isin(set(value))

    # filter gdf then drop any columns with only nulls left after filtering
    gdf = gdf[geom_filter & tags_filter].dropna(axis="columns", how="all")
    if len(gdf) == 0:  # pragma: no cover
        msg = "No matching features. Check query location, tags, and log."
        raise InsufficientResponseError(msg)

    msg = f"{len(gdf):,} features in the final GeoDataFrame"
    utils.log(msg, level=lg.INFO)
    return gdf


def _should_be_polygon(way_tags: dict[str, Any]) -> bool:
    """
    Determine if a closed way should be represented as a Polygon.

    Uses the `_POLYGON_FEATURES` rules from the OSM wiki to decide whether a
    closed way's geometry should be a Polygon rather than a LineString.

    Parameters
    ----------
    way_tags
        The way's tags as a dict.

    Returns
    -------
    is_polygon
        True if the way should be a Polygon, otherwise False.
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


def _create_gdf_from_dfs(
    nodes_df: pd.DataFrame,
    ways_df: pd.DataFrame,
    relations_df: pd.DataFrame,
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
) -> gpd.GeoDataFrame:
    """
    Create a GeoDataFrame of features from node, way, and relation DataFrames.

    Create a GeoDataFrame of features from node, way, and relation
    DataFrames returned by DuckDB.

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
    all_features: list[dict[str, Any]] = []

    # Process nodes
    if not nodes_df.empty:
        for row in nodes_df.itertuples(index=False):
            feature: dict[str, Any] = {
                "element": "node",
                "id": int(row.id),
                "geometry": wkb.loads(bytes(row.geometry)),
            }
            if row.tags:
                row_tags = dict(row.tags)
                row_tags.pop("geometry", None)
                feature.update(row_tags)
            all_features.append(feature)

    # Process ways — geometry already constructed in SQL as linestring or
    # polygon.  The SQL uses a simplified polygon rule (closed + area!='no').
    # Apply the full _POLYGON_FEATURES rules as a post-processing refinement.
    if not ways_df.empty:
        query_tag_keys = set(tags.keys())
        for row in ways_df.itertuples(index=False):
            way_tags = dict(row.tags) if row.tags else {}
            way_tags.pop("geometry", None)

            geom = wkb.loads(bytes(row.geometry))

            # Refine polygon detection: SQL marked is_polygon based on
            # simplified rule; use full _POLYGON_FEATURES rules to fix.
            if row.is_polygon and not _should_be_polygon(way_tags):
                # SQL made it a polygon but full rules say linestring
                try:
                    geom = LineString(geom.exterior.coords)
                except (AttributeError, GEOSException, ValueError):
                    pass  # keep as-is if conversion fails

            # Only include ways whose tags match query tags
            if (len(query_tag_keys) == 0 and len(way_tags) > 0) or (
                len(query_tag_keys & way_tags.keys()) > 0
            ):
                feature = {
                    "element": "way",
                    "id": int(row.id),
                    "geometry": geom,
                }
                feature.update(way_tags)
                all_features.append(feature)

    # Process relations — geometry already constructed in SQL
    if not relations_df.empty:
        for row in relations_df.itertuples(index=False):
            rel_tags = dict(row.tags) if row.tags else {}
            rel_tags.pop("geometry", None)
            feature = {
                "element": "relation",
                "id": int(row.id),
                "geometry": wkb.loads(bytes(row.geometry)),
            }
            feature.update(rel_tags)
            all_features.append(feature)

    if len(all_features) == 0:
        msg = "No matching features. Check query location, tags, and log."
        raise InsufficientResponseError(msg)

    gdf = (
        gpd.GeoDataFrame(
            data=all_features,
            geometry="geometry",
            crs=settings.default_crs,
        )
        .set_index(["element", "id"])
        .sort_index()
    )
    return _filter_features(gdf, polygon, tags)
