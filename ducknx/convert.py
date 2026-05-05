"""Convert spatial graphs to/from different data types."""

from __future__ import annotations

import itertools
import logging as lg
from typing import Any
from typing import Literal
from typing import overload

import networkx as nx
import polars as pl
import pyarrow as pa
import shapely
from shapely import LineString

from . import utils




def to_digraph(G: nx.MultiDiGraph, *, weight: str = "length") -> nx.DiGraph:
    """
    Convert MultiDiGraph to DiGraph.

    Chooses between parallel edges by minimizing `weight` attribute value. See
    also `to_undirected` to convert MultiDiGraph to MultiGraph.

    Parameters
    ----------
    G
        Input graph.
    weight
        Attribute value to minimize when choosing between parallel edges.

    Returns
    -------
    D
        The converted DiGraph.
    """
    # make a copy to not mutate original graph object caller passed in
    G = G.copy()
    to_remove: list[tuple[int, int, int]] = []

    # identify all the parallel edges in the MultiDiGraph
    parallels = ((u, v) for u, v in G.edges(keys=False) if G.number_of_edges(u, v) > 1)

    # among all sets of parallel edges, remove all except the one with the
    # minimum "weight" attribute value
    for u, v in set(parallels):
        k_min, _ = min(G.get_edge_data(u, v).items(), key=lambda x: x[1][weight])
        to_remove.extend((u, v, k) for k in G[u][v] if k != k_min)

    G.remove_edges_from(to_remove)
    msg = "Converted MultiDiGraph to DiGraph"
    utils.log(msg, level=lg.INFO)

    return nx.DiGraph(G)


def to_undirected(G: nx.MultiDiGraph) -> nx.MultiGraph:
    """
    Convert MultiDiGraph to undirected MultiGraph.

    This function has a limited use case: it allows you to create a MultiGraph
    for use with functions/algorithms that only accept a MultiGraph object.
    Rather, if you want a fully bidirectional graph (such as for a walking
    network), configure the `settings` module's `bidirectional_network_types`
    before creating your graph to generate a fully bidirectional MultiDiGraph.

    This function maintains parallel edges only if their geometries differ.
    See also `to_digraph` to convert MultiDiGraph to DiGraph.

    Parameters
    ----------
    G
        Input graph.

    Returns
    -------
    Gu
        The converted MultiGraph.
    """
    # make a copy to not mutate original graph object caller passed in
    G = G.copy()

    # set from/to nodes before making graph undirected
    for u, v, d in G.edges(data=True):
        d["from"] = u
        d["to"] = v

        # add geometry if missing, to compare parallel edges' geometries
        if "geometry" not in d:
            point_u = (G.nodes[u]["x"], G.nodes[u]["y"])
            point_v = (G.nodes[v]["x"], G.nodes[v]["y"])
            d["geometry"] = LineString([point_u, point_v])

    # increment parallel edges' keys so we don't retain only one edge of sets
    # of true parallel edges when we convert from MultiDiGraph to MultiGraph
    G = _update_edge_keys(G)

    # convert MultiDiGraph to MultiGraph, retaining edges in both directions
    # of parallel edges and self-loops for now
    Gu = nx.MultiGraph(**G.graph)
    Gu.add_nodes_from(G.nodes(data=True))
    Gu.add_edges_from(G.edges(keys=True, data=True))

    # the previous operation added all directed edges from G as undirected
    # edges in Gu. we now have duplicate edges for each bidirectional parallel
    # edge or self-loop. so, look through the edges and remove any duplicates.
    duplicate_edges = set()
    for u1, v1, key1, data1 in Gu.edges(keys=True, data=True):
        # if we haven't already flagged this edge as a duplicate
        if (u1, v1, key1) not in duplicate_edges:
            # look at every other edge between u and v, one at a time
            for key2 in Gu[u1][v1]:
                # don't compare this edge to itself
                if key1 != key2:
                    # compare the first edge's data to the second's
                    # if they match up, flag the duplicate for removal
                    data2 = Gu.edges[u1, v1, key2]
                    if _is_duplicate_edge(data1, data2):
                        duplicate_edges.add((u1, v1, key2))

    Gu.remove_edges_from(duplicate_edges)
    msg = "Converted MultiDiGraph to undirected MultiGraph"
    utils.log(msg, level=lg.INFO)

    return Gu


def _is_duplicate_edge(data1: dict[str, Any], data2: dict[str, Any]) -> bool:
    """
    Check if two graph edge data dicts have the same `osmid` and `geometry`.

    Parameters
    ----------
    data1
        The first edge's attribute data.
    data2
        The second edge's attribute data.

    Returns
    -------
    is_dupe
        True if `osmid` and `geometry` are the same, otherwise False.
    """
    is_dupe = False

    # if either edge's osmid contains multiple values (due to simplification)
    # compare them as sets to see if they contain the same values
    osmid1 = set(data1["osmid"]) if isinstance(data1["osmid"], list) else data1["osmid"]
    osmid2 = set(data2["osmid"]) if isinstance(data2["osmid"], list) else data2["osmid"]

    # if they contain the same osmid or set of osmids (due to simplification)
    if osmid1 == osmid2:
        # if both edges have geometry attributes and they match each other
        if ("geometry" in data1) and ("geometry" in data2):
            if _is_same_geometry(data1["geometry"], data2["geometry"]):
                is_dupe = True

        # if neither edge has a geometry attribute
        elif ("geometry" not in data1) and ("geometry" not in data2):
            is_dupe = True

        # if one edge has geometry attribute but the other doesn't: not dupes
        else:
            pass

    return is_dupe


def _is_same_geometry(ls1: LineString, ls2: LineString) -> bool:
    """
    Determine if two LineString geometries are the same (in either direction).

    Check both the normal and reversed orders of their constituent points.

    Parameters
    ----------
    ls1
        The first LineString geometry.
    ls2
        The second LineString geometry.

    Returns
    -------
    is_same
        True if geometries are the same in either direction, otherwise False.
    """
    # extract coordinates from each LineString geometry
    geom1 = [tuple(coords) for coords in ls1.xy]
    geom2 = [tuple(coords) for coords in ls2.xy]

    # reverse the first LineString's coordinates' direction
    geom1_r = [tuple(reversed(coords)) for coords in ls1.xy]

    # if second geometry matches first in either direction, return True
    return geom2 in (geom1, geom1_r)


def _update_edge_keys(G: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """
    Increment key of one edge of parallel edges that differ in geometry.

    For example, two streets from `u` to `v` that bow away from each other as
    separate streets, rather than opposite direction edges of a single street.
    Increment one of these edge's keys so that they do not match across
    `(u, v, k)` or `(v, u, k)` so we can add both to an undirected MultiGraph.

    Parameters
    ----------
    G
        Input graph.

    Returns
    -------
    G
        Graph with incremented keys where needed.
    """
    # iterate G.edges directly: collect (u, v, k, geometry) and group on the
    # canonical sorted-uv-and-key signature so directed pairs (u→v) and (v→u)
    # with the same key collide as one group.
    by_uvk: dict[str, list[tuple[Any, Any, Any, Any]]] = {}
    for u, v, k, data in G.edges(keys=True, data=True):
        geom = data.get("geometry")
        if geom is None:
            continue
        canon = "_".join([*sorted([str(u), str(v)]), str(k)])
        by_uvk.setdefault(canon, []).append((u, v, k, geom))

    different_streets: list[tuple[Any, Any, Any]] = []
    for group in by_uvk.values():
        if len(group) < 2:
            continue
        for (u1, v1, k1, geom1), (_u2, _v2, _k2, geom2) in itertools.combinations(group, 2):
            if not _is_same_geometry(geom1, geom2):
                # flag the first edge in the group, mirroring the legacy semantics
                different_streets.append((u1, v1, k1))
                break

    # for each unique different street, increment its key to make it unique
    for u, v, k in set(different_streets):
        new_key = max(list(G[u][v]) + list(G[v][u])) + 1
        G.add_edge(u, v, key=new_key, **G.get_edge_data(u, v, k))
        G.remove_edge(u, v, key=k)

    return G


def _coerce_mixed_scalar_list(rows: list[dict[str, Any]]) -> None:
    """
    Promote scalar values to single-element lists for mixed scalar/list keys.

    NetworkX simplified graphs can produce attributes whose value is a scalar
    on some edges and a list on others (e.g. ``osmid``). Polars rejects this
    mix when inferring a column type. We unify them to ``list`` semantics in
    place so downstream Arrow conversion succeeds.

    Parameters
    ----------
    rows
        List of attribute dicts; mutated in place.
    """
    seen_list: set[str] = set()
    seen_scalar: set[str] = set()
    for row in rows:
        for key, val in row.items():
            if isinstance(val, list):
                seen_list.add(key)
            elif val is not None:
                seen_scalar.add(key)
    mixed = seen_list & seen_scalar
    if not mixed:
        return
    for row in rows:
        for key in mixed:
            if key in row and not isinstance(row[key], list) and row[key] is not None:
                row[key] = [row[key]]


def _wkb_field(name: str, crs: str | None) -> pa.Field:
    """
    Build a PyArrow field with geoarrow.wkb extension type metadata.

    Parameters
    ----------
    name
        Column name.
    crs
        Authority code string (e.g. ``"EPSG:4326"``) or ``None``.

    Returns
    -------
    field
        Arrow field carrying the geoarrow.wkb extension metadata.
    """
    crs_token = (crs or "").upper()
    metadata = {
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": (
            b'{"crs":"' + crs_token.encode("ascii") + b'","edges":"planar"}'
        ),
    }
    return pa.field(name, pa.binary(), nullable=True, metadata=metadata)


def _attach_geoarrow(tbl: pa.Table, name: str, crs: str | None) -> pa.Table:
    """
    Replace the named binary column's field with a geoarrow.wkb field.

    Parameters
    ----------
    tbl
        Source Arrow table.
    name
        Column name to upgrade.
    crs
        CRS authority code; embedded in the field metadata.

    Returns
    -------
    tbl
        Table with the named column carrying geoarrow.wkb metadata.
    """
    if name not in tbl.column_names:
        return tbl
    fields = list(tbl.schema)
    idx = tbl.schema.get_field_index(name)
    fields[idx] = _wkb_field(name, crs)
    new_schema = pa.schema(fields, metadata=tbl.schema.metadata)
    return tbl.cast(new_schema, safe=False)


def _crs_token(crs: Any) -> str | None:  # noqa: ANN401
    """
    Coerce a CRS-like object to an authority-code string.

    Parameters
    ----------
    crs
        A pyproj CRS, an EPSG-style string, or ``None``.

    Returns
    -------
    token
        e.g. ``"EPSG:4326"`` or ``None`` if unknown.
    """
    if crs is None:
        return None
    if isinstance(crs, str):
        return crs.upper()
    # pyproj.CRS or similar — try the to_string variants
    for attr in ("to_string", "srs"):
        if hasattr(crs, attr):
            value = getattr(crs, attr)
            value = value() if callable(value) else value
            if isinstance(value, str) and value:
                return value.upper()
    return str(crs)


@overload
def graph_to_arrow(
    G: nx.MultiGraph | nx.MultiDiGraph,
    *,
    node_geometry: bool = True,
    fill_edge_geometry: bool = True,
) -> tuple[pa.Table, pa.Table]: ...


@overload
def graph_to_arrow(
    G: nx.MultiGraph | nx.MultiDiGraph,
    *,
    nodes: Literal[True],
    edges: Literal[True],
    node_geometry: bool = True,
    fill_edge_geometry: bool = True,
) -> tuple[pa.Table, pa.Table]: ...


@overload
def graph_to_arrow(
    G: nx.MultiGraph | nx.MultiDiGraph,
    *,
    nodes: Literal[True],
    edges: Literal[False],
    node_geometry: bool = True,
    fill_edge_geometry: bool = True,
) -> pa.Table: ...


@overload
def graph_to_arrow(
    G: nx.MultiGraph | nx.MultiDiGraph,
    *,
    nodes: Literal[False],
    edges: Literal[True],
    node_geometry: bool = True,
    fill_edge_geometry: bool = True,
) -> pa.Table: ...


def graph_to_arrow(
    G: nx.MultiGraph | nx.MultiDiGraph,
    *,
    nodes: bool = True,
    edges: bool = True,
    node_geometry: bool = True,
    fill_edge_geometry: bool = True,
) -> pa.Table | tuple[pa.Table, pa.Table]:
    """
    Convert a graph to node and/or edge Arrow tables.

    Returns the Arrow analog of ``graph_to_gdfs``: nodes and edges as
    ``pa.Table``s with a ``geoarrow.wkb`` ``geometry`` column carrying CRS
    metadata. Node identifiers ride in an ``osmid`` column; edge endpoints
    ride in ``u``, ``v``, ``key`` columns. All other node/edge attributes
    are preserved as columns; types are inferred via Polars and converted
    to Arrow.

    Parameters
    ----------
    G
        Input graph.
    nodes
        If True, emit a nodes Arrow table.
    edges
        If True, emit an edges Arrow table.
    node_geometry
        If True, attach a ``geometry`` column built from each node's
        ``x``/``y`` attributes.
    fill_edge_geometry
        If True, synthesize a LineString edge ``geometry`` from endpoint
        node coordinates when the edge does not already carry one.

    Returns
    -------
    nodes_tbl or edges_tbl or (nodes_tbl, edges_tbl)
        Arrow tables matching the requested outputs.
    """
    crs = _crs_token(G.graph.get("crs"))

    nodes_tbl: pa.Table | None = None
    edges_tbl: pa.Table | None = None

    if nodes:
        if len(G.nodes) == 0:  # pragma: no cover
            msg = "Graph contains no nodes."
            raise ValueError(msg)

        node_rows = [{"osmid": n, **data} for n, data in G.nodes(data=True)]
        _coerce_mixed_scalar_list(node_rows)
        nodes_df = pl.DataFrame(node_rows, infer_schema_length=None, strict=False)

        if node_geometry:
            xs = nodes_df.get_column("x").to_numpy()
            ys = nodes_df.get_column("y").to_numpy()
            wkbs = shapely.to_wkb(shapely.points(xs, ys))
            nodes_df = nodes_df.with_columns(pl.Series("geometry", wkbs, dtype=pl.Binary))

        nodes_tbl = nodes_df.to_arrow()
        if node_geometry:
            nodes_tbl = _attach_geoarrow(nodes_tbl, "geometry", crs)
        utils.log("Created nodes Arrow table from graph", level=lg.INFO)

    if edges:
        if len(G.edges) == 0:  # pragma: no cover
            msg = "Graph contains no edges."
            raise ValueError(msg)

        node_coords = {n: (G.nodes[n]["x"], G.nodes[n]["y"]) for n in G}
        edge_rows: list[dict[str, Any]] = []
        for u, v, k, data in G.edges(keys=True, data=True):
            row: dict[str, Any] = {"u": u, "v": v, "key": k, **data}
            geom = data.get("geometry")
            if geom is None and fill_edge_geometry:
                geom = LineString((node_coords[u], node_coords[v]))
            row["geometry"] = shapely.to_wkb(geom) if geom is not None else None
            edge_rows.append(row)

        _coerce_mixed_scalar_list(edge_rows)
        edges_df = pl.DataFrame(edge_rows, infer_schema_length=None, strict=False)
        edges_tbl = edges_df.to_arrow()
        edges_tbl = _attach_geoarrow(edges_tbl, "geometry", crs)
        utils.log("Created edges Arrow table from graph", level=lg.INFO)

    if nodes and edges:
        return nodes_tbl, edges_tbl  # type: ignore[return-value]
    if nodes:
        return nodes_tbl  # type: ignore[return-value]
    if edges:
        return edges_tbl  # type: ignore[return-value]
    msg = "You must request nodes or edges or both."
    raise ValueError(msg)


def graph_from_arrow(  # noqa: PLR0912
    nodes_tbl: pa.Table,
    edges_tbl: pa.Table,
    *,
    graph_attrs: dict[str, Any] | None = None,
) -> nx.MultiDiGraph:
    """
    Convert node/edge Arrow tables into a MultiDiGraph.

    Inverse of ``graph_to_arrow``. ``nodes_tbl`` must have an ``osmid``
    column and contain ``x``/``y`` columns; ``edges_tbl`` must have
    ``u``, ``v``, ``key`` columns. Any ``geometry`` column is interpreted
    as WKB (geoarrow.wkb) and decoded into shapely geometries.

    Parameters
    ----------
    nodes_tbl
        Arrow table of nodes. Must contain ``osmid``, ``x``, ``y``.
    edges_tbl
        Arrow table of edges. Must contain ``u``, ``v``, ``key``.
    graph_attrs
        Optional ``G.graph`` attribute dict. If ``None``, the CRS is
        recovered from the geometry column metadata.

    Returns
    -------
    G
        The reconstructed MultiDiGraph.
    """
    for required in ("osmid", "x", "y"):
        if required not in nodes_tbl.column_names:
            msg = f"`nodes_tbl` must contain `{required}` column."
            raise ValueError(msg)
    for required in ("u", "v", "key"):
        if required not in edges_tbl.column_names:
            msg = f"`edges_tbl` must contain `{required}` column."
            raise ValueError(msg)

    if graph_attrs is None:
        meta = (edges_tbl.schema.field("geometry").metadata
                if "geometry" in edges_tbl.column_names else None) or {}
        ext_meta = meta.get(b"ARROW:extension:metadata", b"")
        crs = None
        if b'"crs":"' in ext_meta:
            start = ext_meta.find(b'"crs":"') + len(b'"crs":"')
            end = ext_meta.find(b'"', start)
            crs_value = ext_meta[start:end].decode("ascii")
            if crs_value:
                crs = crs_value
        graph_attrs = {"crs": crs} if crs else {}

    G = nx.MultiDiGraph(**graph_attrs)

    nodes_rows = nodes_tbl.to_pylist()
    edges_rows = edges_tbl.to_pylist()

    geom_in_nodes = "geometry" in nodes_tbl.column_names
    geom_in_edges = "geometry" in edges_tbl.column_names

    for row in edges_rows:
        u = row.pop("u")
        v = row.pop("v")
        k = row.pop("key")
        if geom_in_edges:
            wkb = row.get("geometry")
            row["geometry"] = shapely.from_wkb(wkb) if wkb is not None else None
        # drop nulls so edges only get attributes with non-null values
        attrs = {key: val for key, val in row.items() if val is not None}
        G.add_edge(u, v, key=k, **attrs)

    for row in nodes_rows:
        osmid = row.pop("osmid")
        if geom_in_nodes:
            row.pop("geometry", None)  # x/y are authoritative
        attrs = {key: val for key, val in row.items() if val is not None}
        if osmid in G.nodes:
            G.nodes[osmid].update(attrs)
        else:
            G.add_node(osmid, **attrs)

    utils.log("Created graph from node/edge Arrow tables", level=lg.INFO)
    return G


def rustworkx_to_networkx(G_rx: Any) -> nx.MultiDiGraph:  # noqa: ANN401
    """
    Convert a rustworkx PyDiGraph to a NetworkX MultiDiGraph.

    Parameters
    ----------
    G_rx
        A rustworkx PyDiGraph with node/edge payloads as dicts.

    Returns
    -------
    G
        The equivalent NetworkX MultiDiGraph.
    """
    G = nx.MultiDiGraph(**G_rx.attrs)
    node_id_map = G_rx.attrs.get("node_id_map", {})

    # Add nodes using OSM IDs
    for rx_idx in G_rx.node_indices():
        data = G_rx.get_node_data(rx_idx)
        osm_id = node_id_map.get(rx_idx, rx_idx)
        G.add_node(osm_id, **data)

    # Add edges using OSM IDs
    for edge_idx in G_rx.edge_indices():
        src, tgt = G_rx.get_edge_endpoints_by_index(edge_idx)
        data = G_rx.get_edge_data_by_index(edge_idx)
        u = node_id_map.get(src, src)
        v = node_id_map.get(tgt, tgt)
        G.add_edge(u, v, **data)

    return G
