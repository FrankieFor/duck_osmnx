"""Visualize street networks, routes, orientations, and geospatial features."""

from __future__ import annotations

import logging as lg
from collections.abc import Iterable
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import overload

import networkx as nx
import numpy as np
import pyarrow as pa
import shapely

from . import bearing
from . import convert
from . import projection
from . import settings
from . import utils
from . import utils_geo

if TYPE_CHECKING:
    pass

# matplotlib is an optional dependency needed for visualization
try:
    import matplotlib.pyplot as plt
    from matplotlib import cm
    from matplotlib import colormaps
    from matplotlib import colors
    from matplotlib.axes._axes import Axes  # noqa: TC002
    from matplotlib.figure import Figure  # noqa: TC002
    from matplotlib.projections.polar import PolarAxes  # noqa: TC002

    mpl_available = True

except ImportError:  # pragma: no cover
    mpl_available = False


def get_colors(
    n: int,
    *,
    cmap: str = "viridis",
    start: float = 0,
    stop: float = 1,
    alpha: float | None = None,
) -> list[str]:
    """
    Return `n` evenly-spaced colors from a matplotlib colormap.

    Parameters
    ----------
    n
        How many colors to sample.
    cmap
        Name of the matplotlib colormap from which to sample the colors.
    start
        Where to start sampling from the colorspace (from 0 to 1).
    stop
        Where to end sampling from the colorspace (from 0 to 1).
    alpha
        If `None`, return colors as HTML-like hex triplet "#rrggbb" RGB
        strings. If `float`, return as "#rrggbbaa" RGBa strings.

    Returns
    -------
    color_list
        The sampled colors.
    """
    _verify_mpl()
    color_gen = (colormaps[cmap](x) for x in np.linspace(start, stop, n))
    keep_alpha = alpha is not None
    if keep_alpha:
        color_gen = ((r, g, b, alpha) for r, g, b, _ in color_gen)
    return [colors.to_hex(c, keep_alpha=keep_alpha) for c in color_gen]


def get_node_colors_by_attr(
    G: nx.MultiDiGraph,
    attr: str,
    *,
    num_bins: int | None = None,
    cmap: str = "viridis",
    start: float = 0,
    stop: float = 1,
    na_color: str = "none",
    equal_size: bool = False,
) -> dict[Any, str]:
    """
    Return colors based on nodes' numerical attribute values.

    Parameters
    ----------
    G
        Input graph.
    attr
        Name of a node attribute with numerical values.
    num_bins
        If None, linearly map a color to each value. Otherwise, bin values
        into ``num_bins`` and assign one color per bin.
    cmap
        Matplotlib colormap name.
    start, stop
        Where to start/stop in the colorspace (each in ``[0, 1]``).
    na_color
        Color for nodes missing ``attr``.
    equal_size
        If True (and ``num_bins`` is set), use equal-frequency quantile
        bins; if False, equal-width bins.

    Returns
    -------
    node_colors
        Mapping ``{node_id: color_hex_string}``.
    """
    vals = nx.get_node_attributes(G, attr)
    return _get_colors_by_value(vals, num_bins, cmap, start, stop, na_color, equal_size)


def get_edge_colors_by_attr(
    G: nx.MultiDiGraph,
    attr: str,
    *,
    num_bins: int | None = None,
    cmap: str = "viridis",
    start: float = 0,
    stop: float = 1,
    na_color: str = "none",
    equal_size: bool = False,
) -> dict[Any, str]:
    """
    Return colors based on edges' numerical attribute values.

    Parameters
    ----------
    G
        Input graph.
    attr
        Edge attribute name with numerical values.
    num_bins
        If None, linearly map a color to each value. Otherwise, bin values
        into ``num_bins`` and assign one color per bin.
    cmap
        Matplotlib colormap name.
    start, stop
        Where to start/stop in the colorspace (each in ``[0, 1]``).
    na_color
        Color for edges missing ``attr``.
    equal_size
        If True (and ``num_bins`` is set), use equal-frequency quantile
        bins; if False, equal-width bins.

    Returns
    -------
    edge_colors
        Mapping ``{(u, v, k): color_hex_string}``.
    """
    vals = nx.get_edge_attributes(G, attr)
    return _get_colors_by_value(vals, num_bins, cmap, start, stop, na_color, equal_size)


def plot_graph(  # noqa: PLR0913
    G: nx.MultiGraph | nx.MultiDiGraph,
    *,
    ax: Axes | None = None,
    figsize: tuple[float, float] = (8, 8),
    bgcolor: str = "#111111",
    node_color: str | Sequence[str] = "w",
    node_size: float | Sequence[float] = 15,
    node_alpha: float | None = None,
    node_edgecolor: str | Iterable[str] = "none",
    node_zorder: int = 1,
    edge_color: str | Iterable[str] = "#999999",
    edge_linewidth: float | Sequence[float] = 1,
    edge_alpha: float | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    show: bool = True,
    close: bool = False,
    save: bool = False,
    filepath: str | Path | None = None,
    dpi: int = 300,
) -> tuple[Figure, Axes]:
    """
    Visualize a graph.

    Parameters
    ----------
    G
        Input graph.
    ax
        If not None, plot on this pre-existing axes instance.
    figsize
        If `ax` is None, create new figure with size `(width, height)`.
    bgcolor
        Background color of the figure.
    node_color
        Color(s) of the nodes.
    node_size
        Size(s) of the nodes. If 0, then skip plotting the nodes.
    node_alpha
        Opacity of the nodes. If you passed RGBa values to `node_color`, set
        `node_alpha=None` to use the alpha channel in `node_color`.
    node_edgecolor
        Color(s) of the nodes' markers' borders.
    node_zorder
        The zorder to plot nodes. Edges are always 1, so set `node_zorder=0`
        to plot nodes beneath edges.
    edge_color
        Color(s) of the edges' lines.
    edge_linewidth
        Width(s) of the edges' lines. If 0, then skip plotting the edges.
    edge_alpha
        Opacity of the edges. If you passed RGBa values to `edge_color`, set
        `edge_alpha=None` to use the alpha channel in `edge_color`.
    bbox
        Bounding box as `(left, bottom, right, top)`. If None, calculate it
        from spatial extents of plotted geometries.
    show
        If True, call `pyplot.show()` to show the figure.
    close
        If True, call `pyplot.close()` to close the figure.
    save
        If True, save the figure to disk at `filepath`.
    filepath
        The path to the file if `save` is True. File format is determined from
        the extension. If None, save at `settings.imgs_folder/image.png`.
    dpi
        The resolution of saved file if `save` is True.

    Returns
    -------
    fig, ax
        The resulting matplotlib figure and axes objects.
    """
    _verify_mpl()
    max_node_size = max(node_size) if isinstance(node_size, Sequence) else node_size
    max_edge_lw = max(edge_linewidth) if isinstance(edge_linewidth, Sequence) else edge_linewidth
    if max_node_size <= 0 and max_edge_lw <= 0:  # pragma: no cover
        msg = "Either `node_size` or `edge_linewidth` must be > 0 to plot something."
        raise ValueError(msg)

    # create fig, ax as needed
    msg = "Begin plotting the graph..."
    utils.log(msg, level=lg.INFO)
    fig, ax = _get_fig_ax(ax=ax, figsize=figsize, bgcolor=bgcolor, polar=False)

    edge_segments: list[np.ndarray] | None = None
    node_xs: np.ndarray | None = None
    node_ys: np.ndarray | None = None

    if max_edge_lw > 0:
        # build per-edge coordinate sequences directly from G; matplotlib's
        # LineCollection draws them without ever materializing a GeoDataFrame
        from matplotlib.collections import LineCollection  # noqa: PLC0415

        edge_segments = []
        node_xy = {n: (G.nodes[n]["x"], G.nodes[n]["y"]) for n in G.nodes}
        # honor an explicit edge geometry attribute when present
        for u, v, data in G.edges(data=True):
            geom = data.get("geometry")
            if geom is not None and hasattr(geom, "coords"):
                edge_segments.append(np.asarray(geom.coords, dtype=np.float64))
            else:
                edge_segments.append(np.asarray([node_xy[u], node_xy[v]], dtype=np.float64))
        lc = LineCollection(
            edge_segments, colors=edge_color, linewidths=edge_linewidth,
            alpha=edge_alpha, zorder=1,
        )
        ax.add_collection(lc)

    if max_node_size > 0:
        node_xs = np.fromiter((G.nodes[n]["x"] for n in G.nodes), dtype=np.float64,
                              count=len(G.nodes))
        node_ys = np.fromiter((G.nodes[n]["y"] for n in G.nodes), dtype=np.float64,
                              count=len(G.nodes))
        # accept dict-keyed node_color from get_node_colors_by_attr by aligning
        # to G.nodes order
        if isinstance(node_color, dict):
            node_color = [node_color.get(n, "none") for n in G.nodes]
        ax.scatter(
            x=node_xs, y=node_ys, s=node_size, c=node_color,
            alpha=node_alpha, edgecolor=node_edgecolor, zorder=node_zorder,
        )

    # determine spatial extents
    padding = 0.0
    if bbox is None:
        if edge_segments is not None and edge_segments:
            stacked = np.concatenate(edge_segments, axis=0)
            left, bottom = stacked.min(axis=0)
            right, top = stacked.max(axis=0)
        elif node_xs is not None and node_ys is not None:
            left, bottom = float(node_xs.min()), float(node_ys.min())
            right, top = float(node_xs.max()), float(node_ys.max())
        else:  # pragma: no cover
            left, bottom, right, top = 0.0, 0.0, 0.0, 0.0
        bbox = float(left), float(bottom), float(right), float(top)
        padding = 0.02  # pad 2% to not cut off peripheral nodes' circles

    # configure axes appearance, save/show figure as specified, and return
    ax = _config_ax(ax, G.graph["crs"], bbox, padding)
    fig, ax = _save_and_show(
        fig=fig,
        ax=ax,
        show=show,
        close=close,
        save=save,
        filepath=filepath,
        dpi=dpi,
    )
    msg = "Finished plotting the graph"
    utils.log(msg, level=lg.INFO)
    return fig, ax


def plot_graph_route(
    G: nx.MultiDiGraph,
    route: list[int],
    *,
    route_color: str = "r",
    route_linewidth: float = 4,
    route_alpha: float = 0.5,
    orig_dest_size: float = 100,
    ax: Axes | None = None,
    **pg_kwargs: Any,  # noqa: ANN401
) -> tuple[Figure, Axes]:
    """
    Visualize a path along a graph.

    Parameters
    ----------
    G
        Input graph.
    route
        A path of node IDs.
    route_color
        The color of the route.
    route_linewidth
        Width of the route's line.
    route_alpha
        Opacity of the route's line.
    orig_dest_size
        Size of the origin and destination nodes.
    ax
        If not None, plot on this pre-existing axes instance.
    **pg_kwargs
        Keyword arguments to pass to `plot_graph`.

    Returns
    -------
    fig, ax
        The resulting matplotlib figure and axes objects.
    """
    _verify_mpl()
    if ax is None:
        # plot the graph but not the route, and override any user show/close
        # args for now: we'll do that later
        overrides = {"show", "save", "close"}
        kwargs = {k: v for k, v in pg_kwargs.items() if k not in overrides}
        fig, ax = plot_graph(G, show=False, save=False, close=False, **kwargs)
    else:
        fig = ax.figure  # type: ignore[assignment]

    # scatterplot origin and destination points (first/last nodes in route)
    od_x = (G.nodes[route[0]]["x"], G.nodes[route[-1]]["x"])
    od_y = (G.nodes[route[0]]["y"], G.nodes[route[-1]]["y"])
    ax.scatter(od_x, od_y, s=orig_dest_size, c=route_color, alpha=route_alpha, edgecolor="none")

    # assemble the route edge geometries' x and y coords then plot the line
    x = []
    y = []
    for u, v in zip(route[:-1], route[1:]):
        # if there are parallel edges, select the shortest in length
        data = min(G.get_edge_data(u, v).values(), key=lambda d: d["length"])
        if "geometry" in data:
            # if geometry attribute exists, add all its coords to list
            xs, ys = data["geometry"].xy
            x.extend(xs)
            y.extend(ys)
        else:
            # otherwise, the edge is a straight line from node to node
            x.extend((G.nodes[u]["x"], G.nodes[v]["x"]))
            y.extend((G.nodes[u]["y"], G.nodes[v]["y"]))
    ax.plot(x, y, c=route_color, lw=route_linewidth, alpha=route_alpha)

    # save and show the figure as specified, passing relevant kwargs
    sas_kwargs = {"show", "close", "save", "filepath", "dpi"}
    kwargs = {k: v for k, v in pg_kwargs.items() if k in sas_kwargs}
    fig, ax = _save_and_show(fig=fig, ax=ax, **kwargs)
    return fig, ax


def plot_graph_routes(
    G: nx.MultiDiGraph,
    routes: Iterable[list[int]],
    *,
    route_colors: str | Iterable[str] = "r",
    route_linewidths: float | Iterable[float] = 4,
    **pgr_kwargs: Any,  # noqa: ANN401
) -> tuple[Figure, Axes]:
    """
    Visualize multiple paths along a graph.

    Parameters
    ----------
    G
        Input graph.
    routes
        Paths of node IDs.
    route_colors
        If string, the one color for all routes. Otherwise, the color for each
        route.
    route_linewidths
        If float, the one linewidth for all routes. Otherwise, the linewidth
        for each route.
    **pgr_kwargs
        Keyword arguments to pass to `plot_graph_route`.

    Returns
    -------
    fig, ax
        The resulting matplotlib figure and axes objects.
    """
    # make iterables lists (so we're guaranteed to be able to get their sizes)
    routes = list(routes)
    route_colors = (
        [route_colors] * len(routes) if isinstance(route_colors, str) else list(route_colors)
    )
    route_linewidths = (
        [route_linewidths] * len(routes)
        if not isinstance(route_linewidths, Iterable)
        else list(route_linewidths)
    )

    # check for valid arguments
    if not all(isinstance(r, list) for r in routes):  # pragma: no cover
        msg = "`routes` must be an iterable of route lists."
        raise TypeError(msg)
    if len(routes) == 0:  # pragma: no cover
        msg = "You must pass at least 1 route."
        raise ValueError(msg)
    if not (len(routes) == len(route_colors) == len(route_linewidths)):  # pragma: no cover
        msg = "`route_colors` and `route_linewidths` must have same lengths as `routes`."
        raise ValueError(msg)

    # plot the graph and the first route
    overrides = {"route", "route_color", "route_linewidth", "show", "save", "close"}
    kwargs = {k: v for k, v in pgr_kwargs.items() if k not in overrides}
    fig, ax = plot_graph_route(
        G,
        route=routes[0],
        route_color=route_colors[0],
        route_linewidth=route_linewidths[0],
        show=False,
        save=False,
        close=False,
        **kwargs,
    )

    # plot the subsequent routes on top of existing ax
    overrides.update({"ax"})
    kwargs = {k: v for k, v in pgr_kwargs.items() if k not in overrides}
    r_rc_rlw = zip(routes[1:], route_colors[1:], route_linewidths[1:])
    for route, route_color, route_linewidth in r_rc_rlw:
        fig, ax = plot_graph_route(
            G,
            route=route,
            route_color=route_color,
            route_linewidth=route_linewidth,
            show=False,
            save=False,
            close=False,
            ax=ax,
            **kwargs,
        )

    # save and show the figure as specified, passing relevant kwargs
    sas_kwargs = {"show", "close", "save", "filepath", "dpi"}
    kwargs = {k: v for k, v in pgr_kwargs.items() if k in sas_kwargs}
    fig, ax = _save_and_show(fig=fig, ax=ax, **kwargs)
    return fig, ax


def plot_figure_ground(
    G: nx.MultiDiGraph,
    *,
    dist: float = 805,
    street_widths: dict[str, float] | None = None,
    default_width: float = 4,
    color: str = "w",
    **pg_kwargs: Any,  # noqa: ANN401
) -> tuple[Figure, Axes]:
    """
    Plot a figure-ground diagram of a street network.

    Parameters
    ----------
    G
        An unprojected graph.
    dist
        How many meters to extend plot's bounding box from the graph's center
        point. Default corresponds to a square mile bounding box.
    street_widths
        Dict keys are street types (ie, OSM "highway" tags) and values are the
        widths to plot them, in pixels.
    default_width
        Fallback width, in pixels, for any street type not in `street_widths`.
    color
        The color of the streets.
    **pg_kwargs
        Keyword arguments to pass to `plot_graph`.

    Returns
    -------
    fig, ax
        The resulting matplotlib figure and axes objects.
    """
    _verify_mpl()

    # if user did not pass in custom street widths, define default values
    if street_widths is None:
        street_widths = {
            "footway": 1.5,
            "steps": 1.5,
            "pedestrian": 1.5,
            "service": 1.5,
            "path": 1.5,
            "track": 1.5,
            "motorway": 6,
        }

    # we need an undirected graph to find every edge incident on a node
    Gu = convert.to_undirected(G)

    # for each edge, get a linewidth according to street type
    edge_linewidths = []
    for _, _, d in Gu.edges(keys=False, data=True):
        street_type = d["highway"][0] if isinstance(d["highway"], list) else d["highway"]
        if street_type in street_widths:
            edge_linewidths.append(street_widths[street_type])
        else:
            edge_linewidths.append(default_width)

    # smooth the street segment joints
    # for each node, get a node size according to the narrowest incident edge
    node_widths: dict[int, float] = {}
    for node in Gu.nodes:
        # first, identify all the highway types of this node's incident edges
        ie_data = (Gu.get_edge_data(node, nbr) for nbr in Gu.neighbors(node))
        edge_types = [d[min(d)]["highway"] for d in ie_data]
        if len(edge_types) == 0:
            # if node has no incident edges, make size zero
            node_widths[node] = 0
        else:
            # flatten the list of edge types
            et_flat = []
            for et in edge_types:
                if isinstance(et, list):
                    et_flat.extend(et)
                else:
                    et_flat.append(et)

            # look up corresponding width for each edge type in flat list
            edge_widths = [street_widths.get(et, default_width) for et in et_flat]

            # node diameter should = largest edge width to make joints smooth
            # mpl circle marker sizes are in area, so use diameter squared
            circle_diameter = max(edge_widths)
            circle_area = circle_diameter**2
            node_widths[node] = circle_area

    # assign the node size to each node in the graph
    node_sizes: list[float] | float = [node_widths[node] for node in Gu.nodes]

    # define the view extents from the centroid of all node points
    node_xs = np.fromiter((Gu.nodes[n]["x"] for n in Gu.nodes), dtype=np.float64,
                          count=len(Gu.nodes))
    node_ys = np.fromiter((Gu.nodes[n]["y"] for n in Gu.nodes), dtype=np.float64,
                          count=len(Gu.nodes))
    node_pts = shapely.points(node_xs, node_ys)
    centroid = shapely.union_all(node_pts).centroid
    latlon_point = (centroid.y, centroid.x)
    bbox = utils_geo.bbox_from_point(latlon_point, dist=dist, project_utm=False)

    # plot the figure
    overrides = {"bbox", "node_size", "node_color", "edge_linewidth"}
    kwargs = {k: v for k, v in pg_kwargs.items() if k not in overrides}
    fig, ax = plot_graph(
        G=Gu,
        bbox=bbox,
        node_size=node_sizes,
        node_color=color,
        edge_color=color,
        edge_linewidth=edge_linewidths,
        **kwargs,
    )
    return fig, ax


def plot_footprints(  # noqa: PLR0913
    table: pa.Table,
    *,
    ax: Axes | None = None,
    figsize: tuple[float, float] = (8, 8),
    color: str = "orange",
    edge_color: str = "none",
    edge_linewidth: float = 0,
    alpha: float | None = None,
    bgcolor: str = "#111111",
    bbox: tuple[float, float, float, float] | None = None,
    show: bool = True,
    close: bool = False,
    save: bool = False,
    filepath: str | Path | None = None,
    dpi: int = 600,
) -> tuple[Figure, Axes]:
    """
    Visualize an Arrow table of geospatial features' footprints.

    Parameters
    ----------
    table
        Arrow table with a ``geometry`` column (geoarrow.wkb).
        Non-Polygon/MultiPolygon rows are ignored.
    ax
        If not None, plot on this pre-existing axes instance.
    figsize
        If `ax` is None, create new figure with size `(width, height)`.
    color
        Color of the footprints.
    edge_color
        Color of the footprints' edges.
    edge_linewidth
        Width of the footprints' edges.
    alpha
        Opacity of the footprints' edges.
    bgcolor
        Background color of the figure.
    bbox
        Bounding box as `(left, bottom, right, top)`. If None, calculate it
        from the spatial extents of the geometries in `gdf`.
    show
        If True, call `pyplot.show()` to show the figure.
    close
        If True, call `pyplot.close()` to close the figure.
    save
        If True, save the figure to disk at `filepath`.
    filepath
        The path to the file if `save` is True. File format is determined from
        the extension. If None, save at `settings.imgs_folder/image.png`.
    dpi
        The resolution of saved file if `save` is True.

    Returns
    -------
    fig, ax
        The resulting matplotlib figure and axes objects.
    """
    _verify_mpl()
    from matplotlib.collections import PatchCollection  # noqa: PLC0415
    from matplotlib.patches import PathPatch  # noqa: PLC0415
    from matplotlib.path import Path as MplPath  # noqa: PLC0415

    fig, ax = _get_fig_ax(ax=ax, figsize=figsize, bgcolor=bgcolor, polar=False)

    # decode WKB → shapely, retain only Polygon / MultiPolygon
    wkb_col = table.column("geometry").to_pylist()
    geoms = shapely.from_wkb(wkb_col)
    keep_types = {"Polygon", "MultiPolygon"}
    geoms = [g for g in geoms if g is not None and g.geom_type in keep_types]
    if not geoms:  # pragma: no cover
        msg = "No Polygon/MultiPolygon geometries to plot."
        raise ValueError(msg)

    patches: list[PathPatch] = []
    minx = miny = float("inf")
    maxx = maxy = float("-inf")
    for geom in geoms:
        polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
        for poly in polys:
            ext = np.asarray(poly.exterior.coords, dtype=np.float64)
            verts = [ext]
            codes = [
                np.concatenate(([MplPath.MOVETO], np.full(len(ext) - 1, MplPath.LINETO))),
            ]
            for ring in poly.interiors:
                hole = np.asarray(ring.coords, dtype=np.float64)
                verts.append(hole)
                codes.append(
                    np.concatenate(([MplPath.MOVETO], np.full(len(hole) - 1, MplPath.LINETO))),
                )
            path = MplPath(np.concatenate(verts), np.concatenate(codes))
            patches.append(PathPatch(path))
            bx, by, bX, bY = poly.bounds
            minx, miny = min(minx, bx), min(miny, by)
            maxx, maxy = max(maxx, bX), max(maxy, bY)

    coll = PatchCollection(
        patches, facecolor=color, edgecolor=edge_color,
        linewidth=edge_linewidth, alpha=alpha,
    )
    ax.add_collection(coll)

    if bbox is None:
        bbox = (minx, miny, maxx, maxy)

    # recover CRS from the geometry field metadata
    geom_meta = table.schema.field("geometry").metadata or {}
    ext_meta = geom_meta.get(b"ARROW:extension:metadata", b"")
    crs = None
    if b'"crs":"' in ext_meta:
        s = ext_meta.find(b'"crs":"') + len(b'"crs":"')
        e = ext_meta.find(b'"', s)
        crs_token = ext_meta[s:e].decode("ascii")
        crs = crs_token if crs_token else None

    ax = _config_ax(ax, crs, bbox, 0)
    fig, ax = _save_and_show(
        fig=fig,
        ax=ax,
        show=show,
        close=close,
        save=save,
        filepath=filepath,
        dpi=dpi,
    )
    return fig, ax


def plot_orientation(  # noqa: PLR0913
    G: nx.MultiGraph | nx.MultiDiGraph,
    *,
    num_bins: int = 36,
    min_length: float = 0,
    weight: str | None = None,
    ax: PolarAxes | None = None,
    figsize: tuple[float, float] = (5, 5),
    area: bool = True,
    color: str = "#003366",
    edgecolor: str = "k",
    linewidth: float = 0.5,
    alpha: float = 0.7,
    title: str | None = None,
    title_y: float = 1.05,
    title_font: dict[str, Any] | None = None,
    xtick_font: dict[str, Any] | None = None,
) -> tuple[Figure, PolarAxes]:
    """
    Plot a polar histogram of a spatial network's edge bearings.

    Ignores self-loop edges as their bearings are undefined. If `G` is a
    MultiGraph, all edge bearings will be bidirectional (ie, two reciprocal
    bearings per undirected edge). If `G` is a MultiDiGraph, all edge bearings
    will be directional (ie, one bearing per directed edge). See also the
    `bearings` module.

    For more info see: Boeing, G. 2019. "Urban Spatial Order: Street Network
    Orientation, Configuration, and Entropy." Applied Network Science, 4 (1),
    67. https://doi.org/10.1007/s41109-019-0189-1

    Parameters
    ----------
    G
        Unprojected graph with `bearing` attributes on each edge.
    num_bins
        Number of bins. For example, if `num_bins=36` is provided, then each
        bin will represent 10 degrees around the compass.
    min_length
        Ignore edges with "length" attribute values less than `min_length`.
    weight
        If not None, weight the edges' bearings by this (non-null) edge
        attribute.
    ax
        If not None, plot on this pre-existing axes instance (must have
        projection=polar).
    figsize
        If `ax` is None, create new figure with size `(width, height)`.
    area
        If True, set bar length so area is proportional to frequency.
        Otherwise, set bar length so height is proportional to frequency.
    color
        Color of the histogram bars.
    edgecolor
        Color of the histogram bar edges.
    linewidth
        Width of the histogram bar edges.
    alpha
        Opacity of the histogram bars.
    title
        The figure's title.
    title_y
        The y position to place `title`.
    title_font
        The title's `fontdict` to pass to matplotlib.
    xtick_font
        The xtick labels' `fontdict` to pass to matplotlib.

    Returns
    -------
    fig, ax
        The resulting matplotlib figure and polar axes objects.
    """
    _verify_mpl()

    if title_font is None:
        title_font = {"family": "DejaVu Sans", "size": 24, "weight": "bold"}
    if xtick_font is None:
        xtick_font = {
            "family": "DejaVu Sans",
            "size": 10,
            "weight": "bold",
            "alpha": 1.0,
            "zorder": 3,
        }

    # get the bearing distribution's bin counts and center values in degrees
    bin_counts, bin_centers = bearing._bearings_distribution(
        G,
        num_bins,
        min_length=min_length,
        weight=weight,
    )

    # positions: where to center each bar
    positions = np.deg2rad(bin_centers)

    # width: make bars fill the circumference without gaps or overlaps
    width = 2 * np.pi / num_bins

    # radius: how long to make each bar. set bar length so either the bar area
    # (ie, via sqrt) or the bar height is proportional to the bin's frequency
    bin_frequency = bin_counts / bin_counts.sum()
    radius = np.sqrt(bin_frequency) if area else bin_frequency

    # create PolarAxes (if not passed-in) then set N at top and go clockwise
    fig, ax = _get_fig_ax(ax=ax, figsize=figsize, bgcolor=None, polar=True)
    ax.set_theta_zero_location("N")
    ax.set_theta_direction("clockwise")
    ax.set_ylim(top=radius.max())

    # configure the y-ticks and remove their labels
    ax.set_yticks(np.linspace(0, radius.max(), 5))
    ax.set_yticklabels(labels="")

    # configure the x-ticks and their labels
    xticklabels = ["N", "", "E", "", "S", "", "W", ""]
    ax.set_xticks(ax.get_xticks())
    ax.set_xticklabels(labels=xticklabels, fontdict=xtick_font)
    ax.tick_params(axis="x", which="major", pad=-2)

    # draw the bars
    ax.bar(
        positions,
        height=radius,
        width=width,
        align="center",
        bottom=0,
        zorder=2,
        color=color,
        edgecolor=edgecolor,
        linewidth=linewidth,
        alpha=alpha,
    )

    if title:
        ax.set_title(title, y=title_y, fontdict=title_font)
    fig.tight_layout()
    return fig, ax


def _get_colors_by_value(
    vals: dict[Any, Any],
    num_bins: int | None,
    cmap: str,
    start: float,
    stop: float,
    na_color: str,
    equal_size: bool,  # noqa: FBT001
) -> dict[Any, str]:
    """
    Map colors to the entries of a ``{id: value}`` dict.

    Parameters
    ----------
    vals
        Mapping from node/edge ID to numeric attribute value (or ``None``
        / ``NaN`` for missing).
    num_bins
        If None, linearly map a color across the value range. Otherwise,
        bin values into ``num_bins`` and assign one color per bin.
    cmap
        Matplotlib colormap name.
    start, stop
        Colorspace start/stop in ``[0, 1]``.
    na_color
        Color for entries with null/NaN values.
    equal_size
        Ignored when ``num_bins`` is None; otherwise True → equal-frequency
        quantile bins, False → equal-width bins.

    Returns
    -------
    color_map
        Mapping ``{id: color_hex_string}``.
    """
    _verify_mpl()

    if len(vals) == 0:
        msg = "There are no attribute values."
        raise ValueError(msg)

    keys = list(vals.keys())
    raw = np.array([vals[k] for k in keys], dtype=object)
    # null mask for missing/NaN values
    null_mask = np.array([
        v is None or (isinstance(v, float) and np.isnan(v))
        for v in raw
    ])

    if null_mask.all():
        return dict.fromkeys(keys, na_color)

    numeric = np.array(
        [np.nan if null_mask[i] else float(raw[i]) for i in range(len(raw))],
        dtype=np.float64,
    )

    if num_bins is None:
        valid = numeric[~null_mask]
        vals_min = float(valid.min())
        vals_max = float(valid.max())
        full_range = (vals_max - vals_min) / (stop - start) if stop != start else 1.0
        full_min = vals_min - full_range * start
        full_max = full_min + full_range

        normalizer = colors.Normalize(full_min, full_max)
        scalar_mapper = cm.ScalarMappable(normalizer, colormaps[cmap])
        out: dict[Any, str] = {}
        for i, key in enumerate(keys):
            if null_mask[i]:
                out[key] = na_color
            else:
                out[key] = colors.to_hex(scalar_mapper.to_rgba(numeric[i]))
        return out

    # binned mode
    valid_vals = numeric[~null_mask]
    if equal_size:
        # equal-frequency quantile edges
        edges = np.quantile(valid_vals, np.linspace(0, 1, num_bins + 1))
    else:
        edges = np.linspace(valid_vals.min(), valid_vals.max(), num_bins + 1)
    # np.digitize returns 1..num_bins for in-range values, 0/num_bins+1 for out-of-range
    bin_ids = np.digitize(numeric, edges[1:-1], right=False)
    bin_ids = np.clip(bin_ids, 0, num_bins - 1)
    bin_colors = get_colors(num_bins, cmap=cmap, start=start, stop=stop)
    return {
        key: na_color if null_mask[i] else bin_colors[int(bin_ids[i])]
        for i, key in enumerate(keys)
    }


def _save_and_show(
    fig: Figure,
    ax: Axes,
    *,
    show: bool = True,
    close: bool = True,
    save: bool = False,
    filepath: str | Path | None = None,
    dpi: int = 300,
) -> tuple[Figure, Axes]:
    """
    Save a figure to disk and/or show it, as specified by arguments.

    Parameters
    ----------
    fig
        The figure.
    ax
        The axes instance.
    show
        If True, call `pyplot.show()` to show the figure.
    close
        If True, call `pyplot.close()` to close the figure.
    save
        If True, save the figure to disk at `filepath`.
    filepath
        The path to the file if `save` is True. File format is determined from
        the extension. If None, save at `settings.imgs_folder/image.png`.
    dpi
        The resolution of saved file if `save` is True.

    Returns
    -------
    fig, ax
        The matplotlib figure and axes objects.
    """
    fig.canvas.draw()
    fig.canvas.flush_events()

    if save:
        # default filepath, if none provided
        fp = Path(settings.imgs_folder) / "image.png" if filepath is None else Path(filepath)

        # if save folder does not already exist, create it
        fp.parent.mkdir(parents=True, exist_ok=True)

        # get the file extension and figure facecolor
        ext = fp.suffix.strip(".")
        fc = fig.get_facecolor()

        if ext == "svg":
            # if the file format is svg, prep the fig/ax for saving
            ax.axis("off")
            ax.set_position((0, 0, 1, 1))
            ax.patch.set_alpha(0)
            fig.patch.set_alpha(0)
            fig.savefig(fp, bbox_inches=0, format=ext, facecolor=fc, transparent=True)
        else:
            # constrain saved figure's extent to interior of the axes
            extent = ax.bbox.transformed(fig.dpi_scale_trans.inverted())

            # temporarily turn figure frame on to save with facecolor
            fig.set_frameon(True)
            fig.savefig(fp, dpi=dpi, bbox_inches=extent, format=ext, facecolor=fc, transparent=True)
            fig.set_frameon(False)  # and turn it back off again

        msg = f"Saved figure to disk at {str(fp)!r}"
        utils.log(msg, level=lg.INFO)

    if show:
        plt.show()

    if close:
        plt.close()

    return fig, ax


def _config_ax(ax: Axes, crs: Any, bbox: tuple[float, float, float, float], padding: float) -> Axes:  # noqa: ANN401
    """
    Configure a matplotlib axes instance for display.

    Parameters
    ----------
    ax
        The axes instance.
    crs
        The coordinate reference system of the plotted geometries.
    bbox
        Bounding box as `(left, bottom, right, top)`.
    padding
        Relative padding to add around `bbox`.

    Returns
    -------
    ax
        The configured matplotlib axes object.
    """
    # set the axes view limits to bbox + relative padding
    left, bottom, right, top = bbox
    padding_ns = (top - bottom) * padding
    padding_ew = (right - left) * padding
    ax.set_ylim((bottom - padding_ns, top + padding_ns))
    ax.set_xlim((left - padding_ew, right + padding_ew))

    # set margins to zero, point ticks inward, turn off ax border and x/y axis
    # so there is no space around the plot
    ax.margins(0)
    ax.tick_params(which="both", direction="in")
    _ = [s.set_visible(False) for s in ax.spines.values()]  # type: ignore[func-returns-value]
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)

    # set aspect ratio
    if projection.is_projected(crs):
        # if projected, make equal aspect ratio
        ax.set_aspect("equal")
    else:
        # if not projected, conform aspect ratio to not stretch plot
        cos_lat = np.cos(np.deg2rad((bottom + top) / 2))
        ax.set_aspect(1 / cos_lat)

    return ax


# if polar = False, return Axes
@overload
def _get_fig_ax(
    ax: Axes | None,
    figsize: tuple[float, float],
    bgcolor: str | None,
    polar: Literal[False],
) -> tuple[Figure, Axes]: ...


# if polar = True, return PolarAxes
@overload
def _get_fig_ax(
    ax: Axes | None,
    figsize: tuple[float, float],
    bgcolor: str | None,
    polar: Literal[True],
) -> tuple[Figure, PolarAxes]: ...


def _get_fig_ax(
    ax: Axes | None,
    figsize: tuple[float, float],
    bgcolor: str | None,
    polar: bool,  # noqa: FBT001
) -> tuple[Figure, Axes | PolarAxes]:
    """
    Generate a matplotlib Figure and (Polar)Axes or return existing ones.

    Parameters
    ----------
    ax
        If not None, plot on this pre-existing axes instance.
    figsize
        If `ax` is None, create new figure with size `(width, height)`.
    bgcolor
        Background color of figure.
    polar
        If True, generate a `PolarAxes` instead of an `Axes` instance.

    Returns
    -------
    fig, ax
        The resulting matplotlib figure and axes objects.
    """
    if ax is None:
        if polar:
            # make PolarAxes
            fig, ax = plt.subplots(figsize=figsize, subplot_kw={"projection": "polar"})
        else:
            # make regular Axes
            fig, ax = plt.subplots(figsize=figsize, facecolor=bgcolor, frameon=False)
            ax.set_facecolor(bgcolor)
    else:
        fig = ax.figure  # type: ignore[assignment]

    return fig, ax


def _verify_mpl() -> None:
    """Verify that matplotlib is installed and imported."""
    if not mpl_available:  # pragma: no cover
        msg = "matplotlib must be installed as an optional dependency for visualization."
        raise ImportError(msg)
