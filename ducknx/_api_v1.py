# ruff: noqa: PLC0414
"""
Expose the public package API.

Public functions live in their topical submodules; this module
re-exports the most common ones at the package root for the
``dx.function_name(...)`` shorthand. Plot functions are routed via a
lazy ``__getattr__`` so importing ``ducknx`` does not require
``matplotlib``.
"""

from __future__ import annotations

from typing import Any

from .bearing import add_edge_bearings as add_edge_bearings
from .bearing import orientation_entropy as orientation_entropy
from .convert import graph_from_arrow as graph_from_arrow
from .convert import graph_to_arrow as graph_to_arrow
from .distance import nearest_edges as nearest_edges
from .distance import nearest_nodes as nearest_nodes
from .elevation import add_edge_grades as add_edge_grades
from .elevation import add_node_elevations_google as add_node_elevations_google
from .elevation import add_node_elevations_raster as add_node_elevations_raster
from .features import features_from_address as features_from_address
from .features import features_from_bbox as features_from_bbox
from .features import features_from_place as features_from_place
from .features import features_from_point as features_from_point
from .features import features_from_polygon as features_from_polygon
from .geocoder import geocode as geocode
from .geocoder import geocode_to_arrow as geocode_to_arrow
from .graph import graph_from_address as graph_from_address
from .graph import graph_from_bbox as graph_from_bbox
from .graph import graph_from_place as graph_from_place
from .graph import graph_from_point as graph_from_point
from .graph import graph_from_polygon as graph_from_polygon
from .io import load_graphml as load_graphml
from .io import save_graph_geopackage as save_graph_geopackage
from .io import save_graph_xml as save_graph_xml
from .io import save_graphml as save_graphml
from .projection import project_graph as project_graph
from .routing import add_edge_speeds as add_edge_speeds
from .routing import add_edge_travel_times as add_edge_travel_times
from .routing import k_shortest_paths as k_shortest_paths
from .routing import shortest_path as shortest_path
from .simplification import consolidate_intersections as consolidate_intersections
from .simplification import simplify_graph as simplify_graph
from .stats import basic_stats as basic_stats
from .utils import log as log
from .utils import ts as ts

_PLOT_NAMES = {
    "plot_figure_ground",
    "plot_footprints",
    "plot_graph",
    "plot_graph_route",
    "plot_graph_routes",
    "plot_orientation",
}


def __getattr__(name: str) -> Any:  # noqa: ANN401
    """Lazy import for plot functions so ducknx stays import-light."""
    if name in _PLOT_NAMES:
        from . import plot  # noqa: PLC0415

        return getattr(plot, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
