"""
Download and create graphs from OpenStreetMap data.

Refer to the Getting Started guide for usage limitations.
"""

from __future__ import annotations

import logging as lg
from collections.abc import Iterable
from importlib.metadata import version as metadata_version
from typing import TYPE_CHECKING
from typing import Any

import networkx as nx
import pandas as pd
import pyarrow as pa
from shapely import MultiPolygon
from shapely import Polygon

if TYPE_CHECKING:
    import numpy as np

from . import _pbf_reader
from . import distance
from . import geocoder
from . import projection
from . import settings
from . import simplification
from . import stats
from . import truncate
from . import utils
from . import utils_geo


def graph_from_bbox(
    bbox: tuple[float, float, float, float],
    *,
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within a lat-lon bounding box.

    You can either specify a pre-defined `network_type` or provide your
    own `custom_filter` to control which ways are included.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph.

    Parameters
    ----------
    bbox
        Bounding box as `(left, bottom, right, top)`. Coordinates should be in
        unprojected latitude-longitude degrees (EPSG:4326).
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology via the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes the outside bounding box if at least one of
        the node's neighbors lies within the bounding box.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    # convert bounding box to a polygon
    polygon = utils_geo.bbox_to_poly(bbox)

    # create graph using this polygon geometry
    G = graph_from_polygon(
        polygon,
        network_type=network_type,
        simplify=simplify,
        retain_all=retain_all,
        truncate_by_edge=truncate_by_edge,
        custom_filter=custom_filter,
    )

    msg = f"graph_from_bbox returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_point(
    center_point: tuple[float, float],
    dist: float,
    *,
    dist_type: str = "bbox",
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within some distance of a lat-lon point.

    You can either specify a pre-defined `network_type` or provide your
    own `custom_filter` to control which ways are included.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph.

    Parameters
    ----------
    center_point
        The `(lat, lon)` center point around which to construct the graph.
        Coordinates should be in unprojected latitude-longitude degrees
        (EPSG:4326).
    dist
        Retain only those nodes within this many meters of `center_point`,
        measuring distance according to `dist_type`.
    dist_type
        {"bbox", "network"}
        If "bbox", retain only those nodes within a bounding box of `dist`
        length/width. If "network", retain only those nodes within `dist`
        network distance of the nearest node to `center_point`.
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes the outside bounding box if at least one of
        the node's neighbors lies within the bounding box.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    if dist_type not in {"bbox", "network"}:  # pragma: no cover
        msg = "`dist_type` must be 'bbox' or 'network'."
        raise ValueError(msg)

    # create bounding box from center point and distance in each direction
    bbox = utils_geo.bbox_from_point(center_point, dist)

    # create a graph from the bounding box
    G = graph_from_bbox(
        bbox,
        network_type=network_type,
        simplify=simplify,
        retain_all=retain_all,
        truncate_by_edge=truncate_by_edge,
        custom_filter=custom_filter,
    )

    if dist_type == "network":
        # find node nearest to center then truncate graph by dist from it
        node = distance.nearest_nodes(G, X=center_point[1], Y=center_point[0])
        G = truncate.truncate_graph_dist(G, node, dist)

    msg = f"graph_from_point returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_address(
    address: str,
    dist: float,
    *,
    dist_type: str = "bbox",
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within some distance of an address.

    You can either specify a pre-defined `network_type` or provide your
    own `custom_filter` to control which ways are included.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph.

    Parameters
    ----------
    address
        The address to geocode and use as the central point around which to
        construct the graph.
    dist
        Retain only those nodes within this many meters of `center_point`,
        measuring distance according to `dist_type`.
    dist_type
        {"network", "bbox"}
        If "bbox", retain only those nodes within a bounding box of `dist`. If
        "network", retain only those nodes within `dist` network distance from
        the centermost node.
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes the outside bounding box if at least one of
        the node's neighbors lies within the bounding box.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    # geocode the address string to a (lat, lon) point
    point = geocoder.geocode(address)

    # then create a graph from this point
    G = graph_from_point(
        point,
        dist,
        dist_type=dist_type,
        network_type=network_type,
        simplify=simplify,
        retain_all=retain_all,
        truncate_by_edge=truncate_by_edge,
        custom_filter=custom_filter,
    )

    msg = f"graph_from_address returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_place(
    query: str | dict[str, str] | list[str | dict[str, str]],
    *,
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    which_result: int | None | list[int | None] = None,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within the boundaries of some place(s).

    The query must be geocodable and OSM must have polygon boundaries for the
    geocode result. If OSM does not have a polygon for this place, you can
    instead get its street network using the `graph_from_address` function,
    which geocodes the place name to a point and gets the network within some
    distance of that point.

    If OSM does have polygon boundaries for this place but you're not finding
    it, try to vary the query string, pass in a structured query dict, or vary
    the `which_result` argument to use a different geocode result. If you know
    the OSM ID of the place, you can retrieve its boundary polygon using the
    `geocode_to_gdf` function, then pass it to the `features_from_polygon`
    function.

    You can either specify a pre-defined `network_type` or provide your
    own `custom_filter` to control which ways are included.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph.

    Parameters
    ----------
    query
        The query or queries to geocode to retrieve place boundary polygon(s).
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes outside the place boundary polygon(s) if at
        least one of the node's neighbors lies within the polygon(s).
    which_result
        Which geocoding result to use. if None, auto-select the first
        (Multi)Polygon or raise an error if OSM doesn't return one.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    # extract the geometry from the GeoDataFrame to use in query
    polygon = geocoder.geocode_to_gdf(query, which_result=which_result).union_all()
    msg = "Constructed place geometry polygon(s) to query"
    utils.log(msg, level=lg.INFO)

    # create graph using this polygon(s) geometry
    G = graph_from_polygon(
        polygon,
        network_type=network_type,
        simplify=simplify,
        retain_all=retain_all,
        truncate_by_edge=truncate_by_edge,
        custom_filter=custom_filter,
    )

    msg = f"graph_from_place returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def graph_from_polygon(
    polygon: Polygon | MultiPolygon,
    *,
    network_type: str = "all",
    simplify: bool = True,
    retain_all: bool = False,
    truncate_by_edge: bool = False,
    custom_filter: str | list[str] | None = None,
) -> nx.MultiDiGraph:
    """
    Download and create a graph within the boundaries of a (Multi)Polygon.

    You can either specify a pre-defined `network_type` or provide your
    own `custom_filter` to control which ways are included.

    Use the `settings` module's `useful_tags_node` and `useful_tags_way`
    settings to configure which OSM node/way tags are added as graph node/edge
    attributes. If you want a fully bidirectional network, ensure your
    `network_type` is in `settings.bidirectional_network_types` before
    creating your graph.

    Parameters
    ----------
    polygon
        The geometry within which to construct the graph. Coordinates should
        be in unprojected latitude-longitude degrees (EPSG:4326).
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve if `custom_filter` is None.
    simplify
        If True, simplify graph topology with the `simplify_graph` function.
    retain_all
        If True, return the entire graph even if it is not connected. If
        False, retain only the largest weakly connected component.
    truncate_by_edge
        If True, retain nodes outside `polygon` if at least one of the node's
        neighbors lies within `polygon`.
    custom_filter
        A custom ways filter to be used instead of the `network_type` presets,
        e.g. `'["power"~"line"]' or '["highway"~"motorway|trunk"]'`. If `str`,
        the intersection of keys/values will be used, e.g., `'[maxspeed=50][lanes=2]'`
        will return all ways having both maxspeed of 50 and two lanes. If
        `list`, the union of the `list` items will be used, e.g.,
        `['[maxspeed=50]', '[lanes=2]']` will return all ways having either
        maximum speed of 50 or two lanes. Also pass in a `network_type` that
        is in `settings.bidirectional_network_types` if you want the graph to
        be fully bidirectional.

    Returns
    -------
    G
        The resulting MultiDiGraph.

    Notes
    -----
    Very large query areas use the `utils_geo._consolidate_subdivide_geometry`
    function to automatically make multiple requests: see that function's
    documentation for caveats.
    """
    # verify that the geometry is valid and is a shapely Polygon/MultiPolygon
    # before proceeding
    if not polygon.is_valid:  # pragma: no cover
        msg = "The geometry of `polygon` is invalid."
        raise ValueError(msg)
    if not isinstance(polygon, (Polygon, MultiPolygon)):  # pragma: no cover
        msg = (
            "Geometry must be a shapely Polygon or MultiPolygon. If you "
            "requested graph from place name, make sure your query resolves "
            "to a Polygon or MultiPolygon, and not some other geometry, like "
            "a Point. See ducknx documentation for details."
        )
        raise TypeError(msg)

    # create a new buffered polygon 0.5km around the desired one
    poly_proj, crs_utm = projection.project_geometry(polygon)
    poly_proj_buff = poly_proj.buffer(500)
    poly_buff, _ = projection.project_geometry(poly_proj_buff, crs=crs_utm, to_latlong=True)

    if not settings.pbf_file_path:
        msg = "No PBF file path configured. Set settings.pbf_file_path to a local OSM PBF file."
        raise ValueError(msg)

    bidirectional = network_type in settings.bidirectional_network_types

    nodes_df, ways_df = _pbf_reader._read_pbf_network_duckdb(
        poly_buff, network_type, custom_filter, settings.pbf_file_path
    )
    G_buff = _create_graph_from_dfs(nodes_df, ways_df, bidirectional)

    # truncate buffered graph to the buffered polygon and retain_all for
    # now. needed because the query returns entire ways that also include
    # nodes outside the poly if the way has a node inside the poly.
    G_buff = truncate.truncate_graph_polygon(G_buff, poly_buff, truncate_by_edge=truncate_by_edge)

    # keep only the largest weakly connected component if retain_all is False
    if not retain_all:
        G_buff = truncate.largest_component(G_buff, strongly=False)

    # simplify the graph topology
    if simplify:
        G_buff = simplification.simplify_graph(G_buff)

    # truncate graph by original polygon to return graph within polygon
    # caller wants. don't simplify again: this allows us to retain
    # intersections along the street that may now only connect 2 street
    # segments in the network, but in reality also connect to an
    # intersection just outside the polygon
    G = truncate.truncate_graph_polygon(G_buff, polygon, truncate_by_edge=truncate_by_edge)

    # keep only the largest weakly connected component if retain_all is False
    # we're doing this again in case the last truncate disconnected anything
    # on the periphery
    if not retain_all:
        G = truncate.largest_component(G, strongly=False)

    # count how many physical streets in buffered graph connect to each
    # intersection in un-buffered graph, to retain true counts for each
    # intersection, even if some of its neighbors are outside the polygon
    spn = stats.count_streets_per_node(G_buff, nodes=G.nodes)
    nx.set_node_attributes(G, values=spn, name="street_count")

    msg = f"graph_from_polygon returned graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)
    return G


def _build_way_edges(
    i: int,
    osmid_arr: np.ndarray,  # type: ignore[type-arg]
    refs_arr: np.ndarray,  # type: ignore[type-arg]
    tag_arrays: dict[str, np.ndarray],  # type: ignore[type-arg]
    tag_cols: list[str],
    bidirectional: bool,  # noqa: FBT001
    oneway_values: set[str],
    reversed_values: set[str],
) -> tuple[list[tuple[int, int, dict[str, Any]]], list[tuple[int, int, dict[str, Any]]]]:
    """
    Build forward and reverse edge tuples for a single way.

    Parameters
    ----------
    i
        Row index in the way arrays.
    osmid_arr
        Array of OSM way IDs.
    refs_arr
        Array of node reference lists.
    tag_arrays
        Dict mapping tag column names to arrays of values.
    tag_cols
        List of tag column names.
    bidirectional
        If True, create bidirectional edges for one-way streets.
    oneway_values
        Set of values OSM uses for "oneway" tag.
    reversed_values
        Set of values OSM uses for reversed direction.

    Returns
    -------
    forward_edges, reverse_edges
        Lists of (u, v, attrs) tuples for forward and reverse directions.
    """
    # build attribute dict from non-null tag values
    attrs: dict[str, Any] = {"osmid": osmid_arr[i]}
    for col in tag_cols:
        val = tag_arrays[col][i]
        if val is not None and not (isinstance(val, float) and pd.isna(val)):
            attrs[col] = val

    # deduplicate consecutive nodes
    raw_refs = refs_arr[i]
    nodes = [raw_refs[0]]
    nodes.extend(raw_refs[j] for j in range(1, len(raw_refs)) if raw_refs[j] != raw_refs[j - 1])

    # determine one-way and reversed status
    is_one_way = _is_path_one_way(attrs, bidirectional, oneway_values)
    if is_one_way and _is_path_reversed(attrs, reversed_values):
        nodes.reverse()

    if not settings.all_oneway:
        attrs["oneway"] = is_one_way

    # build forward edge pairs
    attrs["reversed"] = False
    forward = [(nodes[j], nodes[j + 1], attrs.copy()) for j in range(len(nodes) - 1)]

    # build reverse edges for non-one-way paths
    reverse: list[tuple[int, int, dict[str, Any]]] = []
    if not is_one_way:
        attrs_rev = {**attrs, "reversed": True}
        reverse = [(nodes[j + 1], nodes[j], attrs_rev.copy()) for j in range(len(nodes) - 1)]

    return forward, reverse


def _create_graph_from_dfs(
    nodes_df: pd.DataFrame | pa.Table,
    ways_df: pd.DataFrame | pa.Table,
    bidirectional: bool,  # noqa: FBT001
) -> nx.MultiDiGraph:
    """
    Create a NetworkX MultiDiGraph from node and way DataFrames.

    Create a NetworkX MultiDiGraph from node and way DataFrames or Arrow
    tables returned by DuckDB. Uses bulk edge list construction for
    performance instead of per-row iteration.

    Parameters
    ----------
    nodes_df
        DataFrame or Arrow table with columns: id, y, x, plus useful tag
        columns.
    ways_df
        DataFrame or Arrow table with columns: osmid, refs, plus useful tag
        columns.
    bidirectional
        If True, create bidirectional edges for one-way streets.

    Returns
    -------
    G
        The resulting MultiDiGraph.
    """
    # Convert Arrow tables to pandas if needed
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
    osmid_arr = ways_df["osmid"].to_numpy()
    refs_arr = ways_df["refs"].to_numpy()
    tag_arrays = {col: ways_df[col].to_numpy() for col in tag_cols}

    msg = f"Creating graph from {len(nodes_df):,} OSM nodes and {len(ways_df):,} OSM ways..."
    utils.log(msg, level=lg.INFO)

    for i in range(len(ways_df)):
        fwd, rev = _build_way_edges(
            i, osmid_arr, refs_arr, tag_arrays, tag_cols,
            bidirectional, oneway_values, reversed_values,
        )
        all_edges_forward.extend(fwd)
        all_edges_reverse.extend(rev)

    # single bulk insert for all edges
    G.add_edges_from(all_edges_forward)
    G.add_edges_from(all_edges_reverse)

    msg = f"Created graph with {len(G):,} nodes and {len(G.edges):,} edges"
    utils.log(msg, level=lg.INFO)

    # add length (great-circle distance between nodes) attribute to each edge
    if len(G.edges) > 0:
        G = distance.add_edge_lengths(G)

    return G


def _is_path_one_way(attrs: dict[str, Any], bidirectional: bool, oneway_values: set[str]) -> bool:  # noqa: FBT001
    """
    Determine if a path of nodes allows travel in only one direction.

    Parameters
    ----------
    attrs
        A path's `tag:value` attribute data.
    bidirectional
        Whether this is a bidirectional network type.
    oneway_values
        The values OSM uses in its "oneway" tag to denote True.

    Returns
    -------
    is_one_way
        True if path allows travel in only one direction, otherwise False.
    """
    # rule 1
    if settings.all_oneway:
        # if globally configured to set every edge one-way, then it's one-way
        return True

    # rule 2
    if bidirectional:
        # if this is a bidirectional network type, then nothing in it is
        # considered one-way. eg, if this is a walking network, this may very
        # well be a one-way street (as cars/bikes go), but in a walking-only
        # network it is a bidirectional edge (you can walk both directions on
        # a one-way street). so we will add this path (in both directions) to
        # the graph and set its oneway attribute to False.
        return False

    # rule 3
    if "oneway" in attrs and attrs["oneway"] in oneway_values:
        # if this path is tagged as one-way and if it is not a bidirectional
        # network type then we'll add the path in one direction only
        return True

    # rule 4
    if "junction" in attrs and attrs["junction"] == "roundabout":  # noqa: SIM103
        # roundabouts are also one-way but are not explicitly tagged as such
        return True

    # otherwise, if no rule passed then this path is not tagged as a one-way
    return False


def _is_path_reversed(attrs: dict[str, Any], reversed_values: set[str]) -> bool:
    """
    Determine if the order of nodes in a path should be reversed.

    Parameters
    ----------
    attrs
        A path's `tag:value` attribute data.
    reversed_values
        The values OSM uses in its 'oneway' tag to denote travel can only
        occur in the opposite direction of the node order.

    Returns
    -------
    is_reversed
        True if nodes' order should be reversed, otherwise False.
    """
    return "oneway" in attrs and attrs["oneway"] in reversed_values


def _add_paths(
    G: nx.MultiDiGraph,
    paths: Iterable[dict[str, Any]],
    bidirectional: bool,  # noqa: FBT001
) -> None:
    """
    Add OSM paths to the graph as edges.

    Parameters
    ----------
    G
        The graph to add paths to.
    paths
        Iterable of paths' `tag:value` attribute data dicts.
    bidirectional
        If True, create bidirectional edges for one-way streets.
    """
    # the values OSM uses in its 'oneway' tag to denote True, and to denote
    # travel can only occur in the opposite direction of the node order. see:
    # https://wiki.openstreetmap.org/wiki/Key:oneway
    # https://www.geofabrik.de/de/data/geofabrik-osm-gis-standard-0.7.pdf
    oneway_values = {"yes", "true", "1", "-1", "reverse", "T", "F"}
    reversed_values = {"-1", "reverse", "T"}

    for path in paths:
        # extract/remove the ordered list of nodes from this path element so
        # we don't add it as a superfluous attribute to the edge later
        nodes = path.pop("nodes")

        # reverse the order of nodes in the path if this path is both one-way
        # and only allows travel in the opposite direction of nodes' order
        is_one_way = _is_path_one_way(path, bidirectional, oneway_values)
        if is_one_way and _is_path_reversed(path, reversed_values):
            nodes.reverse()

        # set the oneway attribute, but only if when not forcing all edges to
        # oneway with the all_oneway setting. With the all_oneway setting, you
        # want to preserve the original OSM oneway attribute for later clarity
        if not settings.all_oneway:
            path["oneway"] = is_one_way

        # zip path nodes to get (u, v) tuples like [(0,1), (1,2), (2,3)].
        edges = list(zip(nodes[:-1], nodes[1:]))

        # add all the edge tuples and give them the path's tag:value attrs
        path["reversed"] = False
        G.add_edges_from(edges, **path)

        # if the path is NOT one-way, reverse direction of each edge and add
        # this path going the opposite direction too
        if not is_one_way:
            path["reversed"] = True
            G.add_edges_from([(v, u) for u, v in edges], **path)
