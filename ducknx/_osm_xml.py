"""
Read/write OSM XML files.

For file format information see https://wiki.openstreetmap.org/wiki/OSM_XML
"""

from __future__ import annotations

import logging as lg
import math
from importlib.metadata import version as metadata_version
from pathlib import Path
from typing import Any
from warnings import warn
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import SubElement

import networkx as nx
import polars as pl

from . import convert
from . import projection
from . import settings
from . import truncate
from . import utils
from ._errors import GraphSimplificationError


def _is_present(value: Any) -> bool:  # noqa: ANN401
    """Return True iff ``value`` is non-null and not a NaN float."""
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return True


# default values for standard "node" and "way" XML subelement attributes
# see: https://wiki.openstreetmap.org/wiki/Elements#Common_attributes
ATTR_DEFAULTS = {
    "changeset": "1",
    "timestamp": utils.ts(style="iso8601"),
    "uid": "1",
    "user": "ducknx",
    "version": "1",
    "visible": "true",
}

# default values for standard "osm" root XML element attributes
# current OSM editing API version: https://wiki.openstreetmap.org/wiki/API
ROOT_ATTR_DEFAULTS = {
    "attribution": "https://www.openstreetmap.org/copyright",
    "copyright": "OpenStreetMap and contributors",
    "generator": f"ducknx {metadata_version('ducknx')}",
    "license": "https://opendatacommons.org/licenses/odbl/1-0/",
    "version": "0.6",
}


def _save_graph_xml(
    G: nx.MultiDiGraph,
    filepath: str | Path | None,
    way_tag_aggs: dict[str, Any] | None,
    encoding: str = "utf-8",
) -> None:
    """
    Save graph to disk as an OSM XML file.

    Parameters
    ----------
    G
        Unsimplified, unprojected graph to save as an OSM XML file.
    filepath
        Path to the saved file including extension. If None, use default
        `settings.data_folder/graph.osm`.
    way_tag_aggs
        Keys are OSM way tag keys and values are aggregation functions
        (anything accepted as an argument by `pandas.agg`). Allows user to
        aggregate graph edge attribute values into single OSM way values. If
        None, or if some tag's key does not exist in the dict, the way
        attribute will be assigned the value of the first edge of the way.
    encoding
        The character encoding of the saved OSM XML file.
    """
    # default "oneway" value used to fill this tag where missing
    ONEWAY = False

    # round lat/lon coordinates to 7 decimals (approx 5 to 10 mm resolution)
    PRECISION = 7

    # warn user if dx.settings.all_oneway is not currently True (but maybe it
    # was when they created the graph)
    if not settings.all_oneway:
        msg = "Make sure graph was created with `dx.settings.all_oneway=True` to save as OSM XML."
        warn(msg, category=UserWarning, stacklevel=2)

    # warn user if graph is projected
    if projection.is_projected(G.graph["crs"]):
        msg = (
            "Graph should be unprojected to save as OSM XML: the existing "
            "projected x-y coordinates will be saved as lat-lon node attributes. "
            "Project your graph back to lat-lon to avoid this."
        )
        warn(msg, category=UserWarning, stacklevel=2)

    # raise error if graph has been simplified
    if G.graph.get("simplified", False):
        msg = "Graph must be unsimplified to save as OSM XML."
        raise GraphSimplificationError(msg)

    # set default filepath if None was provided
    filepath = Path(settings.data_folder) / "graph.osm" if filepath is None else Path(filepath)
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # convert graph to node/edge Arrow tables, then to polars for processing
    nodes_tbl, edges_tbl = convert.graph_to_arrow(G, fill_edge_geometry=False)
    nodes_df = pl.from_arrow(nodes_tbl)
    edges_df = pl.from_arrow(edges_tbl)
    if not isinstance(nodes_df, pl.DataFrame):
        nodes_df = pl.DataFrame(nodes_df)
    if not isinstance(edges_df, pl.DataFrame):
        edges_df = pl.DataFrame(edges_df)

    # spatial bounds: built directly from x/y arrays
    xs = nodes_df.get_column("x").to_numpy()
    ys = nodes_df.get_column("y").to_numpy()
    coords = [
        str(round(float(xs.min()), PRECISION)),
        str(round(float(ys.min()), PRECISION)),
        str(round(float(xs.max()), PRECISION)),
        str(round(float(ys.max()), PRECISION)),
    ]
    bounds = dict(zip(["minlon", "minlat", "maxlon", "maxlat"], coords, strict=True))

    nodes_df = _ensure_attr_defaults(nodes_df)
    edges_df = _ensure_attr_defaults(edges_df)

    # nodes: rename + round lat/lon, drop geometry
    rename_map = {"osmid": "id", "x": "lon", "y": "lat"}
    nodes_df = nodes_df.rename({k: v for k, v in rename_map.items() if k in nodes_df.columns})
    nodes_df = nodes_df.with_columns(
        pl.col("lon").round(PRECISION),
        pl.col("lat").round(PRECISION),
    )
    if "geometry" in nodes_df.columns:
        nodes_df = nodes_df.drop("geometry")

    # edges: oneway → "yes"/"no", rename osmid → id, drop geometry
    if "oneway" in edges_df.columns:
        edges_df = edges_df.with_columns(
            pl.col("oneway").fill_null(ONEWAY).map_elements(
                lambda x: "yes" if x else "no", return_dtype=pl.String,
            ),
        )
    if "osmid" in edges_df.columns:
        edges_df = edges_df.rename({"osmid": "id"})
    if "geometry" in edges_df.columns:
        edges_df = edges_df.drop("geometry")

    # create parent XML element then add bounds, nodes, ways as subelements
    element = Element("osm", attrib=ROOT_ATTR_DEFAULTS)
    _ = SubElement(element, "bounds", attrib=bounds)
    _add_nodes_xml(element, nodes_df)
    _add_ways_xml(element, edges_df, way_tag_aggs)

    # write to disk
    ElementTree(element).write(filepath, encoding=encoding, xml_declaration=True)
    msg = f"Saved graph as OSM XML file at {str(filepath)!r}"
    utils.log(msg, level=lg.INFO)


def _ensure_attr_defaults(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add or fill the standard OSM XML attribute columns with defaults.

    Parameters
    ----------
    df
        Polars DataFrame of nodes or edges.

    Returns
    -------
    df
        DataFrame with all ``ATTR_DEFAULTS`` columns present and non-null.
    """
    cols_to_add: dict[str, Any] = {}
    fill_exprs: list[pl.Expr] = []
    for col, value in ATTR_DEFAULTS.items():
        if col not in df.columns:
            cols_to_add[col] = value
        else:
            fill_exprs.append(pl.col(col).fill_null(value))
    if cols_to_add:
        df = df.with_columns([pl.lit(v).alias(k) for k, v in cols_to_add.items()])
    if fill_exprs:
        df = df.with_columns(fill_exprs)
    return df


def _add_nodes_xml(parent: Element, nodes_df: pl.DataFrame) -> None:
    """
    Add graph nodes as XML subelements.

    Parameters
    ----------
    parent
        Parent XML element.
    nodes_df
        Polars DataFrame of nodes (must include ``id``, ``lat``, ``lon``).
    """
    node_tags = set(settings.useful_tags_node)
    node_attrs = {"id", "lat", "lon"}.union(ATTR_DEFAULTS)

    for node in nodes_df.iter_rows(named=True):
        attrs = {k: str(node[k]) for k in node_attrs if k in node and _is_present(node[k])}
        node_element = SubElement(parent, "node", attrib=attrs)
        for tag_key in node_tags & node.keys():
            value = node[tag_key]
            if isinstance(value, list) or _is_present(value):
                _ = SubElement(node_element, "tag", attrib={"k": tag_key, "v": str(value)})


def _add_ways_xml(
    parent: Element,
    edges_df: pl.DataFrame,
    way_tag_aggs: dict[str, Any] | None,
) -> None:
    """
    Add graph edges (grouped into OSM ways) as XML subelements.

    Parameters
    ----------
    parent
        Parent XML element.
    edges_df
        Polars DataFrame of edges (must include ``u``, ``v``, ``key``, ``id``).
    way_tag_aggs
        Optional per-tag aggregation function (callable or polars-compatible
        named aggregation) applied across the edges of a single way.
    """
    way_tags = set(settings.useful_tags_way)
    way_attrs = list({"id"}.union(ATTR_DEFAULTS))

    for osmid, way in edges_df.group_by("id", maintain_order=True):
        osmid_value = osmid[0] if isinstance(osmid, tuple) else osmid
        first_row = way.row(0, named=True)
        attrs = {a: str(first_row[a]) for a in way_attrs if a in first_row}
        way_element = SubElement(parent, "way", attrib=attrs)

        # node ordering: 1-edge way uses (u, v) directly, otherwise topo-sort
        if way.height == 1:
            nodes_seq = (first_row["u"], first_row["v"])
        else:
            uvk = list(zip(
                way.get_column("u").to_list(),
                way.get_column("v").to_list(),
                way.get_column("key").to_list(),
                strict=True,
            ))
            nodes_seq = _sort_nodes(nx.MultiDiGraph(uvk), osmid_value)
        for node in nodes_seq:
            _ = SubElement(way_element, "nd", attrib={"ref": str(node)})

        for tag in way_tags & set(way.columns):
            if way_tag_aggs is not None and tag in way_tag_aggs:
                series = way.get_column(tag).drop_nulls()
                value = _apply_agg(way_tag_aggs[tag], series.to_list()) \
                    if series.len() else None
            else:
                value = way.get_column(tag)[0]
            if _is_present(value):
                _ = SubElement(way_element, "tag", attrib={"k": tag, "v": str(value)})


def _apply_agg(agg: Any, values: list[Any]) -> Any:  # noqa: ANN401
    """
    Apply a callable or named (polars) aggregation to a list of values.

    Parameters
    ----------
    agg
        Either a callable or a polars-compatible aggregation name.
    values
        Non-null values to aggregate.

    Returns
    -------
    result
        Aggregated value.
    """
    if callable(agg):
        return agg(values)
    series = pl.Series(values=values)
    method = getattr(series, agg)
    return method()


def _sort_nodes(G: nx.MultiDiGraph, osmid: int) -> list[int]:
    """
    Topologically sort the nodes of an OSM way.

    Parameters
    ----------
    G
        The graph representing the OSM way.
    osmid
        The OSM way ID.

    Returns
    -------
    ordered_nodes
        The way's node IDs in topologically sorted order.
    """
    try:
        ordered_nodes = list(nx.topological_sort(G))

    except nx.NetworkXUnfeasible:
        # if it couldn't topologically sort the nodes, the way probably
        # contains a cycle. try removing an edge to break the cycle. first,
        # look for multiple edges emanating from the same source node
        insert_before = True
        edges = [
            edge
            for source in [node for node, degree in G.out_degree() if degree > 1]
            for edge in G.out_edges(source, keys=True)
        ]

        # if none found, then look for multiple edges pointing at the same
        # target node instead
        if len(edges) == 0:
            insert_before = False
            edges = [
                edge
                for target in [node for node, degree in G.in_degree() if degree > 1]
                for edge in G.in_edges(target, keys=True)
            ]

            # if still none, then take the first edge of the way: the entire
            # way could just be a cycle in which each node appears once
            if len(edges) == 0:
                edges = [next(iter(G.edges))]

        # remove one edge at a time and, if the graph remains connected, exit
        # the loop and check if we are able to topologically sort the nodes
        for edge in edges:
            G_ = G.copy()
            G_.remove_edge(*edge)
            if nx.is_weakly_connected(G_):
                break

        try:
            ordered_nodes = list(nx.topological_sort(G_))

            # re-insert (before or after its neighbor as needed) the duplicate
            # source or target node from the edge we removed
            dupe_node = edge[0] if insert_before else edge[1]
            neighbor = edge[1] if insert_before else edge[0]
            position = ordered_nodes.index(neighbor)
            position = position if insert_before else position + 1
            ordered_nodes.insert(position, dupe_node)

        except nx.NetworkXUnfeasible:
            # if it failed again, this way probably contains multiple cycles,
            # so remove a cycle then try to sort the nodes again, recursively.
            # note this is destructive and will be missing in the saved data.
            G_ = G.copy()
            G_.remove_edges_from(nx.find_cycle(G_))
            G_ = truncate.largest_component(G_)
            ordered_nodes = _sort_nodes(G_, osmid)
            msg = f"Had to remove a cycle from way {osmid!r} for topological sort"
            utils.log(msg, level=lg.WARNING)

    return ordered_nodes
