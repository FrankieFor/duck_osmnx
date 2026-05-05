"""Project a graph or geometry to a different CRS."""

from __future__ import annotations

import logging as lg
from typing import TYPE_CHECKING
from typing import Any

import networkx as nx
import numpy as np
import pyproj
import shapely

from . import settings
from . import utils

if TYPE_CHECKING:
    from shapely import Geometry


# UTM applicability bounds (latitude degrees)
_UTM_SOUTH_LIMIT = -80
_UTM_NORTH_LIMIT = 84


def is_projected(crs: Any) -> bool:  # noqa: ANN401
    """
    Determine if a coordinate reference system is projected.

    Parameters
    ----------
    crs
        Anything accepted by ``pyproj.CRS.from_user_input`` — authority
        string (e.g. ``"EPSG:4326"``), WKT, or a CRS object.

    Returns
    -------
    projected
        True if `crs` is projected, otherwise False.
    """
    return bool(pyproj.CRS.from_user_input(crs).is_projected)


def _estimate_utm_crs(xs: np.ndarray, ys: np.ndarray, src_crs: Any) -> pyproj.CRS:  # noqa: ANN401
    """
    Pick an appropriate UTM (or UPS) CRS for the given lon/lat bounds.

    Parameters
    ----------
    xs, ys
        Coordinate arrays in `src_crs`.
    src_crs
        Source CRS for the input coordinates.

    Returns
    -------
    crs
        The chosen target CRS.
    """
    # transform bounds to lon/lat to evaluate UTM zone applicability
    src = pyproj.CRS.from_user_input(src_crs)
    if src.is_geographic:
        lons, lats = xs, ys
    else:
        transformer = pyproj.Transformer.from_crs(src, "EPSG:4326", always_xy=True)
        lons, lats = transformer.transform(xs, ys)

    min_lat = float(np.min(lats))
    max_lat = float(np.max(lats))

    if min_lat < _UTM_SOUTH_LIMIT:
        return pyproj.CRS.from_user_input("EPSG:32761")  # UPS South
    if max_lat > _UTM_NORTH_LIMIT:
        return pyproj.CRS.from_user_input("EPSG:32661")  # UPS North

    # estimate UTM zone via the centroid of the bounding box
    center_lon = (float(np.min(lons)) + float(np.max(lons))) / 2
    center_lat = (min_lat + max_lat) / 2
    utm_zone = int((center_lon + 180) // 6) + 1
    epsg = 32600 + utm_zone if center_lat >= 0 else 32700 + utm_zone
    return pyproj.CRS.from_user_input(f"EPSG:{epsg}")


def project_geometry(
    geom: Geometry,
    *,
    crs: Any | None = None,  # noqa: ANN401
    to_crs: Any | None = None,  # noqa: ANN401
    to_latlong: bool = False,
) -> tuple[Geometry, Any]:
    """
    Project a Shapely geometry from its current CRS to another.

    Parameters
    ----------
    geom
        Geometry to project.
    crs
        Initial CRS of `geom`. Defaults to ``settings.default_crs``.
    to_crs
        Target CRS. If None and not `to_latlong`, an appropriate UTM zone
        is selected from the geometry bounds.
    to_latlong
        If True, project to ``settings.default_crs`` and ignore `to_crs`.

    Returns
    -------
    geom_proj, target_crs
        The projected geometry and its CRS.
    """
    src_crs = settings.default_crs if crs is None else crs

    if to_latlong:
        target = pyproj.CRS.from_user_input(settings.default_crs)
    elif to_crs is not None:
        target = pyproj.CRS.from_user_input(to_crs)
    else:
        coords = np.asarray(geom.bounds)  # (minx, miny, maxx, maxy)
        xs = coords[[0, 2]]
        ys = coords[[1, 3]]
        target = _estimate_utm_crs(xs, ys, src_crs)

    transformer = pyproj.Transformer.from_crs(
        pyproj.CRS.from_user_input(src_crs), target, always_xy=True,
    )
    geom_proj = shapely.ops.transform(transformer.transform, geom)
    msg = f"Projected geometry to {target.to_string()!r}"
    utils.log(msg, level=lg.INFO)
    return geom_proj, target


def project_graph(
    G: nx.MultiDiGraph,
    *,
    to_crs: Any | None = None,  # noqa: ANN401
    to_latlong: bool = False,
) -> nx.MultiDiGraph:
    """
    Project a graph from its current CRS to another.

    Parameters
    ----------
    G
        Graph to project. Must have a ``crs`` graph attribute.
    to_crs
        Target CRS. If None and not `to_latlong`, an appropriate UTM zone
        is chosen from node coordinates.
    to_latlong
        If True, project to ``settings.default_crs`` and ignore `to_crs`.

    Returns
    -------
    G_proj
        The projected graph.
    """
    src_crs = pyproj.CRS.from_user_input(G.graph["crs"])

    # collect node coords
    osmids = list(G.nodes)
    xs = np.fromiter((G.nodes[n]["x"] for n in osmids), dtype=np.float64,
                     count=len(osmids))
    ys = np.fromiter((G.nodes[n]["y"] for n in osmids), dtype=np.float64,
                     count=len(osmids))

    if to_latlong:
        target = pyproj.CRS.from_user_input(settings.default_crs)
    elif to_crs is not None:
        target = pyproj.CRS.from_user_input(to_crs)
    else:
        target = _estimate_utm_crs(xs, ys, src_crs)

    transformer = pyproj.Transformer.from_crs(src_crs, target, always_xy=True)
    new_xs, new_ys = transformer.transform(xs, ys)

    # rebuild graph with projected node coordinates and (if simplified) edge geometries
    G_proj = G.copy()
    for osmid, x, y in zip(osmids, new_xs, new_ys, strict=True):
        G_proj.nodes[osmid]["x"] = float(x)
        G_proj.nodes[osmid]["y"] = float(y)

    if G.graph.get("simplified"):
        for _u, _v, _k, data in G_proj.edges(keys=True, data=True):
            geom = data.get("geometry")
            if geom is not None:
                data["geometry"] = shapely.ops.transform(transformer.transform, geom)

    G_proj.graph["crs"] = target

    msg = (
        f"Projected graph with {len(G)} nodes and {len(G.edges)} edges to "
        f"{target.to_string()!r}"
    )
    utils.log(msg, level=lg.INFO)
    return G_proj
