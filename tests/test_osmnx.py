#!/usr/bin/env python
# ruff: noqa: F841, PLR2004, S101
"""Test suite for the package."""

from __future__ import annotations

# use agg backend so you don't need a display on CI
# do this first before pyplot is imported by anything
import matplotlib as mpl

mpl.use("Agg")

import logging as lg
from collections import OrderedDict
from pathlib import Path

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import pytest
from lxml import etree
from shapely import Point
from shapely import Polygon
from shapely import wkt
from typeguard import suppress_type_checks

import ducknx as dx

dx.settings.log_console = True
dx.settings.log_file = True
dx.settings.use_cache = True
dx.settings.data_folder = ".temp/data"
dx.settings.logs_folder = ".temp/logs"
dx.settings.imgs_folder = ".temp/imgs"
dx.settings.cache_folder = ".temp/cache"

# define queries to use throughout tests
location_point = (37.791427, -122.410018)
polar_point_south = (-84.5501149, -64.1500283)
polar_point_north = (85.0511092, -30.4142117)

address = "Transamerica Pyramid, 600 Montgomery Street, San Francisco, California, USA"
place1 = {"city": "Piedmont", "state": "California", "country": "USA"}
polygon_wkt = (
    "POLYGON ((-122.262 37.869, -122.255 37.869, -122.255 37.874, "
    "-122.262 37.874, -122.262 37.869))"
)
polygon = dx.utils_geo.buffer_geometry(geom=wkt.loads(polygon_wkt), dist=1)


@pytest.mark.xdist_group(name="group1")
def test_logging() -> None:
    """Test the logger."""
    dx.utils.log("test a fake default message")
    dx.utils.log("test a fake debug", level=lg.DEBUG)
    dx.utils.log("test a fake info", level=lg.INFO)
    dx.utils.log("test a fake warning", level=lg.WARNING)
    dx.utils.log("test a fake error", level=lg.ERROR)

    dx.utils.citation(style="apa")
    dx.utils.citation(style="bibtex")
    dx.utils.citation(style="ieee")
    dx.utils.ts(style="iso8601")
    dx.utils.ts(style="date")
    dx.utils.ts(style="time")


@pytest.mark.xdist_group(name="group1")
def test_exceptions() -> None:
    """Test the custom errors."""
    message = "testing exception"

    with pytest.raises(dx._errors.ResponseStatusCodeError):
        raise dx._errors.ResponseStatusCodeError(message)

    with pytest.raises(dx._errors.InsufficientResponseError):
        raise dx._errors.InsufficientResponseError(message)

    with pytest.raises(dx._errors.GraphSimplificationError):
        raise dx._errors.GraphSimplificationError(message)


@pytest.mark.xdist_group(name="group1")
def test_geocoder() -> None:
    """Test retrieving elements by place name and OSM ID."""
    city = dx.geocode_to_gdf("R2999176", by_osmid=True)
    city = dx.geocode_to_gdf(place1, which_result=1)
    city_projected = dx.projection.project_gdf(city, to_crs="epsg:3395")

    # test geocoding a bad query: should raise exception
    with pytest.raises(dx._errors.InsufficientResponseError):
        _ = dx.geocode("!@#$%^&*")

    with pytest.raises(dx._errors.InsufficientResponseError):
        _ = dx.geocode_to_gdf(query="AAAZZZ")

    # fails to geocode to a (Multi)Polygon
    with pytest.raises(TypeError):
        _ = dx.geocode_to_gdf("Civic Center, San Francisco, California, USA")


@pytest.mark.xdist_group(name="group1")
def test_stats() -> None:
    """Test generating graph stats."""
    # create graph, add a new node, add bearings, project it
    G = dx.graph_from_place(place1, network_type="all")
    G.add_node(0, x=location_point[1], y=location_point[0], street_count=0)
    G_proj = dx.project_graph(G)
    G_proj = dx.distance.add_edge_lengths(G_proj, edges=tuple(G_proj.edges)[0:3])

    # calculate stats
    cspn = dx.stats.count_streets_per_node(G)
    stats = dx.basic_stats(G)
    stats = dx.basic_stats(G, area=1000)
    stats = dx.basic_stats(G_proj, area=1000, clean_int_tol=15)

    # test cleaning and rebuilding graph
    G_clean = dx.consolidate_intersections(G_proj, tolerance=10, rebuild_graph=True, dead_ends=True)
    G_clean = dx.consolidate_intersections(
        G_proj,
        tolerance=10,
        rebuild_graph=True,
        reconnect_edges=False,
    )
    G_clean = dx.consolidate_intersections(G_proj, tolerance=10, rebuild_graph=False)
    G_clean = dx.consolidate_intersections(G_proj, tolerance=50000, rebuild_graph=True)

    # try consolidating an empty graph
    G = nx.MultiDiGraph(crs="epsg:4326")
    G_clean = dx.consolidate_intersections(G, rebuild_graph=True)
    G_clean = dx.consolidate_intersections(G, rebuild_graph=False)

    # test passing dict of tolerances to consolidate_intersections
    tols: dict[int, float]
    # every node present
    tols = dict.fromkeys(G_proj.nodes, 5)
    G_clean = dx.consolidate_intersections(G_proj, tolerance=tols, rebuild_graph=True)
    # one node missing
    tols.popitem()
    G_clean = dx.consolidate_intersections(G_proj, tolerance=tols, rebuild_graph=True)
    # one node 0
    tols[next(iter(tols))] = 0
    G_clean = dx.consolidate_intersections(G_proj, tolerance=tols, rebuild_graph=True)


@pytest.mark.xdist_group(name="group1")
def test_bearings() -> None:
    """Test bearings and orientation entropy."""
    G = dx.graph_from_place(place1, network_type="all")
    G.add_node(0, x=location_point[1], y=location_point[0], street_count=0)
    _ = dx.bearing.calculate_bearing(0, 0, 1, 1)
    G = dx.add_edge_bearings(G)
    G_proj = dx.project_graph(G)

    # calculate entropy
    Gu = dx.convert.to_undirected(G)
    entropy = dx.bearing.orientation_entropy(Gu, weight="length")
    fig, ax = dx.plot.plot_orientation(Gu, area=True, title="Title")
    fig, ax = dx.plot.plot_orientation(Gu, ax=ax, area=False, title="Title")

    # test support of edge bearings for directed and undirected graphs
    G = nx.MultiDiGraph(crs="epsg:4326")
    G.add_node("point_1", x=0.0, y=0.0)
    G.add_node("point_2", x=0.0, y=1.0)  # latitude increases northward
    G.add_edge("point_1", "point_2", weight=2.0)
    G = dx.distance.add_edge_lengths(G)
    G = dx.add_edge_bearings(G)
    with pytest.warns(UserWarning, match="edge bearings will be directional"):
        bearings, weights = dx.bearing._extract_edge_bearings(G, min_length=0, weight=None)
    assert list(bearings) == [0.0]  # north
    assert list(weights) == [1.0]
    bearings, weights = dx.bearing._extract_edge_bearings(
        dx.convert.to_undirected(G),
        min_length=0,
        weight="weight",
    )
    assert list(bearings) == [0.0, 180.0]  # north and south
    assert list(weights) == [2.0, 2.0]

    # test _bearings_distribution split bin implementation
    bin_counts, bin_centers = dx.bearing._bearings_distribution(
        G,
        num_bins=1,
        min_length=0,
        weight=None,
    )
    assert list(bin_counts) == [1.0]
    assert list(bin_centers) == [0.0]
    bin_counts, bin_centers = dx.bearing._bearings_distribution(
        G,
        num_bins=2,
        min_length=0,
        weight=None,
    )
    assert list(bin_counts) == [1.0, 0.0]
    assert list(bin_centers) == [0.0, 180.0]


@pytest.mark.xdist_group(name="group1")
def test_osm_xml() -> None:
    """Test working with .osm XML data."""
    # test OSM xml saving
    G = dx.graph_from_point(location_point, dist=500, network_type="drive", simplify=False)
    fp = Path(dx.settings.data_folder) / "graph.osm"
    dx.io.save_graph_xml(G, filepath=fp, way_tag_aggs={"lanes": "sum"})

    # validate saved XML against XSD schema
    xsd_filepath = "./tests/input_data/osm_schema.xsd"
    parser = etree.XMLParser(schema=etree.XMLSchema(file=xsd_filepath))
    _ = etree.parse(fp, parser=parser)

    # test roundabout handling
    default_all_oneway = dx.settings.all_oneway
    dx.settings.all_oneway = True
    point = (39.0290346, -84.4696884)
    G = dx.graph_from_point(point, dist=500, dist_type="bbox", network_type="drive", simplify=False)
    dx.io.save_graph_xml(G)
    _ = etree.parse(fp, parser=parser)

    # raise error if trying to save a simplified graph
    with pytest.raises(dx._errors.GraphSimplificationError):
        dx.io.save_graph_xml(dx.simplification.simplify_graph(G))

    # save a projected/consolidated graph as OSM XML
    Gc = dx.simplification.consolidate_intersections(dx.projection.project_graph(G))
    nx.set_node_attributes(Gc, 0, name="uid")
    dx.io.save_graph_xml(Gc, fp)  # issues UserWarning
    _ = etree.parse(fp, parser=parser)

    # restore settings
    dx.settings.all_oneway = default_all_oneway


@pytest.mark.xdist_group(name="group1")
def test_elevation() -> None:
    """Test working with elevation data."""
    G = dx.graph_from_address(address=address, dist=500, dist_type="bbox", network_type="bike")

    # add node elevations from Google (fails without API key)
    with pytest.raises(dx._errors.InsufficientResponseError):
        _ = dx.elevation.add_node_elevations_google(G, api_key="", batch_size=350)

    # add node elevations from Open Topo Data (works without API key)
    dx.settings.elevation_url_template = (
        "https://api.opentopodata.org/v1/aster30m?locations={locations}&key={key}"
    )
    _ = dx.elevation.add_node_elevations_google(G, batch_size=100, pause=1)

    # same thing again, to hit the cache
    _ = dx.elevation.add_node_elevations_google(G, batch_size=100, pause=0)

    # add node elevations from a single raster file (some nodes will be null)
    rasters = list(Path("tests/input_data").glob("elevation*.tif"))
    G = dx.elevation.add_node_elevations_raster(G, rasters[0], cpus=1)
    assert pd.notna(pd.Series(dict(G.nodes(data="elevation")))).any()

    # add node elevations from multiple raster files (no nodes should be null)
    G = dx.elevation.add_node_elevations_raster(G, rasters)
    assert pd.notna(pd.Series(dict(G.nodes(data="elevation")))).all()

    # consolidate nodes with elevation (by default will aggregate via mean)
    G = dx.simplification.consolidate_intersections(G)

    # add edge grades and their absolute values
    G = dx.add_edge_grades(G, add_absolute=True)


@pytest.mark.xdist_group(name="group1")
def test_routing() -> None:
    """Test working with speed, travel time, and routing."""
    G = dx.graph_from_address(address=address, dist=500, dist_type="bbox", network_type="bike")

    # give each edge speed and travel time attributes
    G = dx.add_edge_speeds(G)
    G = dx.add_edge_speeds(G, hwy_speeds={"motorway": 100})
    G = dx.add_edge_travel_times(G)

    # test value cleaning
    assert dx.routing._clean_maxspeed("100,2") == 100.2
    assert dx.routing._clean_maxspeed("100.2") == 100.2
    assert dx.routing._clean_maxspeed("100 km/h") == 100.0
    assert dx.routing._clean_maxspeed("100 mph") == pytest.approx(160.934)
    assert dx.routing._clean_maxspeed("60|100") == 80
    assert dx.routing._clean_maxspeed("60|100 mph") == pytest.approx(128.7472)
    assert dx.routing._clean_maxspeed("signal") is None
    assert dx.routing._clean_maxspeed("100;70") is None
    assert dx.routing._clean_maxspeed("FR:urban") == 50.0

    # test collapsing multiple mph values to single kph value
    assert dx.routing._collapse_multiple_maxspeed_values(["25 mph", "30 mph"], np.mean) == 44.25685

    # test collapsing invalid values: should return None
    assert dx.routing._collapse_multiple_maxspeed_values(["mph", "kph"], np.mean) is None

    orig_x = np.array([-122.404771])
    dest_x = np.array([-122.401429])
    orig_y = np.array([37.794302])
    dest_y = np.array([37.794987])
    orig_node = int(dx.distance.nearest_nodes(G, orig_x, orig_y)[0])
    dest_node = int(dx.distance.nearest_nodes(G, dest_x, dest_y)[0])

    # test non-numeric weight, should raise ValueError
    with pytest.raises(ValueError, match="contains non-numeric values"):
        route1 = dx.shortest_path(G, orig_node, dest_node, weight="highway")

    # mismatch iterable and non-iterable orig/dest, should raise TypeError
    msg = "must either both be iterable or neither must be iterable"
    with pytest.raises(TypeError, match=msg):
        route2 = dx.shortest_path(G, orig_node, [dest_node])  # type: ignore[call-overload]

    # mismatch lengths of orig/dest, should raise ValueError
    msg = "must be of equal length"
    with pytest.raises(ValueError, match=msg):
        route2 = dx.shortest_path(G, [orig_node] * 2, [dest_node] * 3)

    # test missing weight (should raise warning)
    route3 = dx.shortest_path(G, orig_node, dest_node, weight="time")
    # test good weight
    route4 = dx.routing.shortest_path(G, orig_node, dest_node, weight="travel_time")
    route5 = dx.shortest_path(G, orig_node, dest_node, weight="travel_time")
    assert route5 is not None

    route_edges = dx.routing.route_to_gdf(G, route5, weight="travel_time")

    fig, ax = dx.plot_graph_route(G, route5, save=True)

    # test multiple origins-destinations
    n = 5
    nodes = np.array(G.nodes)
    origs = [int(x) for x in np.random.default_rng().choice(nodes, size=n, replace=True)]
    dests = [int(x) for x in np.random.default_rng().choice(nodes, size=n, replace=True)]
    paths1 = dx.shortest_path(G, origs, dests, weight="length", cpus=1)
    paths2 = dx.shortest_path(G, origs, dests, weight="length", cpus=2)
    paths3 = dx.shortest_path(G, origs, dests, weight="length", cpus=None)
    assert paths1 == paths2 == paths3

    # test k shortest paths
    routes = dx.routing.k_shortest_paths(G, orig_node, dest_node, k=2, weight="travel_time")
    fig, ax = dx.plot_graph_routes(G, list(routes))

    # test great circle and euclidean distance calculators
    assert dx.distance.great_circle(0, 0, 1, 1) == pytest.approx(157249.6034105)
    assert dx.distance.euclidean(0, 0, 1, 1) == pytest.approx(1.4142135)


@pytest.mark.xdist_group(name="group1")
def test_plots() -> None:
    """Test visualization methods."""
    G = dx.graph_from_point(location_point, dist=500, network_type="drive")
    Gp = dx.project_graph(G)
    G = dx.project_graph(G, to_latlong=True)

    # test getting colors
    co1 = dx.plot.get_colors(n=5, cmap="plasma", start=0.1, stop=0.9, alpha=0.5)
    co2 = dx.plot.get_colors(n=5, cmap="plasma", start=0.1, stop=0.9, alpha=None)
    nc = dx.plot.get_node_colors_by_attr(G, "x")
    ec = dx.plot.get_edge_colors_by_attr(G, "length", num_bins=5)

    # plot and save to disk
    filepath = Path(dx.settings.data_folder) / "test.svg"
    fig, ax = dx.plot_graph(G, show=False, save=True, close=True, filepath=filepath)
    fig, ax = dx.plot_graph(Gp, edge_linewidth=0, figsize=(5, 5), bgcolor="y")
    fig, ax = dx.plot_graph(
        Gp,
        ax=ax,
        dpi=180,
        node_color="k",
        node_size=5,
        node_alpha=0.1,
        node_edgecolor="b",
        node_zorder=5,
        edge_color="r",
        edge_linewidth=2,
        edge_alpha=0.1,
        show=False,
        save=True,
        close=True,
    )

    # figure-ground plots
    fig, ax = dx.plot_figure_ground(G=G)


@pytest.mark.xdist_group(name="group1")
def test_nearest() -> None:
    """Test nearest node/edge searching."""
    # get graph and x/y coords to search
    G = dx.graph_from_point(location_point, dist=500, network_type="drive", simplify=False)
    Gp = dx.project_graph(G)
    points = dx.utils_geo.sample_points(dx.convert.to_undirected(Gp), 5)
    X = points.x.to_numpy()
    Y = points.y.to_numpy()

    # get nearest nodes
    _ = dx.distance.nearest_nodes(G, X, Y, return_dist=True)
    _ = dx.distance.nearest_nodes(G, X, Y, return_dist=False)
    nn0, dist0 = dx.distance.nearest_nodes(G, X[0], Y[0], return_dist=True)
    nn1 = dx.distance.nearest_nodes(Gp, X[0], Y[0], return_dist=False)

    # get nearest edge
    _ = dx.distance.nearest_edges(Gp, X, Y, return_dist=False)
    _ = dx.distance.nearest_edges(Gp, X, Y, return_dist=True)
    _ = dx.distance.nearest_edges(Gp, X[0], Y[0], return_dist=False)
    _ = dx.distance.nearest_edges(Gp, X[0], Y[0], return_dist=True)


@pytest.mark.xdist_group(name="group1")
def test_endpoints() -> None:
    """Test different API endpoints."""
    default_requests_timeout = dx.settings.requests_timeout
    default_key = dx.settings.nominatim_key
    default_nominatim_url = dx.settings.nominatim_url

    dx.settings.requests_timeout = 1

    params: OrderedDict[str, int | str] = OrderedDict()
    params["format"] = "json"
    params["address_details"] = 0

    # Bad Address - should return an empty response
    params["q"] = "AAAAAAAAAAA"
    response_json = dx._nominatim._nominatim_request(params=params, request_type="search")

    # Good Address - should return a valid response with a valid osm_id
    params["q"] = "Newcastle A186 Westgate Rd"
    response_json = dx._nominatim._nominatim_request(params=params, request_type="search")

    # Lookup
    params = OrderedDict()
    params["format"] = "json"
    params["address_details"] = 0
    params["osm_ids"] = "W68876073"

    # good call
    response_json = dx._nominatim._nominatim_request(params=params, request_type="lookup")

    # bad call
    with pytest.raises(
        dx._errors.InsufficientResponseError,
        match="Nominatim API did not return a list of results",
    ):
        response_json = dx._nominatim._nominatim_request(params=params, request_type="search")

    # query must be a str if by_osmid=True
    with pytest.raises(TypeError, match="`query` must be a string if `by_osmid` is True"):
        dx.geocode_to_gdf(query={"City": "Boston"}, by_osmid=True)

    # Invalid nominatim query type
    with pytest.raises(ValueError, match="Nominatim `request_type` must be"):
        response_json = dx._nominatim._nominatim_request(params=params, request_type="xyz")

    # Searching on public nominatim should work even if a (bad) key was provided
    dx.settings.nominatim_key = "NOT_A_KEY"
    response_json = dx._nominatim._nominatim_request(params=params, request_type="lookup")

    dx.settings.nominatim_key = default_key
    dx.settings.nominatim_url = default_nominatim_url
    dx.settings.requests_timeout = default_requests_timeout


@pytest.mark.xdist_group(name="group1")
def test_save_load() -> None:  # noqa: PLR0915
    """Test saving/loading graphs to/from disk."""
    G = dx.graph_from_point(location_point, dist=500, network_type="drive")

    # save/load geopackage and convert graph to/from node/edge GeoDataFrames
    dx.save_graph_geopackage(G, directed=False)
    fp = ".temp/data/graph-dir.gpkg"
    dx.save_graph_geopackage(G, filepath=fp, directed=True)
    gdf_nodes1 = gpd.read_file(fp, layer="nodes").set_index("osmid")
    gdf_edges1 = gpd.read_file(fp, layer="edges").set_index(["u", "v", "key"])
    G2 = dx.graph_from_gdfs(gdf_nodes1, gdf_edges1)
    G2 = dx.graph_from_gdfs(gdf_nodes1, gdf_edges1, graph_attrs=G.graph)
    gdf_nodes2, gdf_edges2 = dx.graph_to_gdfs(G2)
    _ = list(dx.utils_geo.interpolate_points(gdf_edges2["geometry"].iloc[0], 0.001))
    assert set(gdf_nodes1.index) == set(gdf_nodes2.index) == set(G.nodes) == set(G2.nodes)
    assert set(gdf_edges1.index) == set(gdf_edges2.index) == set(G.edges) == set(G2.edges)

    # test code branches that should raise exceptions
    with pytest.raises(ValueError, match="You must request nodes or edges or both"):
        dx.graph_to_gdfs(G2, nodes=False, edges=False)
    with pytest.raises(ValueError, match="Invalid literal for boolean"):
        dx.io._convert_bool_string("T")

    # create random boolean graph/node/edge attributes
    attr_name = "test_bool"
    G.graph[attr_name] = False
    bools = np.random.default_rng().integers(low=0, high=2, size=len(G.nodes))
    node_attrs = {n: bool(b) for n, b in zip(G.nodes, bools)}
    nx.set_node_attributes(G, node_attrs, attr_name)
    bools = np.random.default_rng().integers(low=0, high=2, size=len(G.edges))
    edge_attrs = {n: bool(b) for n, b in zip(G.edges, bools)}
    nx.set_edge_attributes(G, edge_attrs, attr_name)

    # create list, set, and dict attributes for nodes and edges
    rand_ints_nodes = np.random.default_rng().integers(low=0, high=10, size=len(G.nodes))
    rand_ints_edges = np.random.default_rng().integers(low=0, high=10, size=len(G.edges))
    list_node_attrs = {n: [n, int(r)] for n, r in zip(G.nodes, rand_ints_nodes)}
    nx.set_node_attributes(G, list_node_attrs, "test_list")
    list_edge_attrs = {e: [e, int(r)] for e, r in zip(G.edges, rand_ints_edges)}
    nx.set_edge_attributes(G, list_edge_attrs, "test_list")
    set_node_attrs = {n: {n, int(r)} for n, r in zip(G.nodes, rand_ints_nodes)}
    nx.set_node_attributes(G, set_node_attrs, "test_set")
    set_edge_attrs = {e: {e, int(r)} for e, r in zip(G.edges, rand_ints_edges)}
    nx.set_edge_attributes(G, set_edge_attrs, "test_set")
    dict_node_attrs = {n: {n: int(r)} for n, r in zip(G.nodes, rand_ints_nodes)}
    nx.set_node_attributes(G, dict_node_attrs, "test_dict")
    dict_edge_attrs = {e: {e: int(r)} for e, r in zip(G.edges, rand_ints_edges)}
    nx.set_edge_attributes(G, dict_edge_attrs, "test_dict")

    # save/load graph as graphml file
    dx.save_graphml(G, gephi=True)
    dx.save_graphml(G, gephi=False)
    dx.save_graphml(G, gephi=False, filepath=fp)
    G2 = dx.load_graphml(
        fp,
        graph_dtypes={attr_name: dx.io._convert_bool_string},
        node_dtypes={attr_name: dx.io._convert_bool_string},
        edge_dtypes={attr_name: dx.io._convert_bool_string},
    )

    # verify everything in G is equivalent in G2
    assert tuple(G.graph.keys()) == tuple(G2.graph.keys())
    assert tuple(G.graph.values()) == tuple(G2.graph.values())
    z = zip(G.nodes(data=True), G2.nodes(data=True))
    for (n1, d1), (n2, d2) in z:
        assert n1 == n2
        assert tuple(d1.keys()) == tuple(d2.keys())
        assert tuple(d1.values()) == tuple(d2.values())
    z = zip(G.edges(keys=True, data=True), G2.edges(keys=True, data=True))
    for (u1, v1, k1, d1), (u2, v2, k2, d2) in z:
        assert u1 == u2
        assert v1 == v2
        assert k1 == k2
        assert tuple(d1.keys()) == tuple(d2.keys())
        assert tuple(d1.values()) == tuple(d2.values())

    # test custom data types
    nd = {"osmid": str}
    ed = {"length": str, "osmid": float}
    G2 = dx.load_graphml(fp, node_dtypes=nd, edge_dtypes=ed)

    # test loading graphml from a file stream
    graphml = Path("tests/input_data/short.graphml").read_text(encoding="utf-8")
    G = dx.load_graphml(graphml_str=graphml, node_dtypes=nd, edge_dtypes=ed)


@pytest.mark.xdist_group(name="group2")
def test_graph_from() -> None:
    """Test downloading graphs from Overpass."""
    # test subdividing a large geometry (raises a UserWarning)
    bbox = dx.utils_geo.bbox_from_point((0, 0), dist=1e5, project_utm=True)
    poly = dx.utils_geo.bbox_to_poly(bbox)
    _ = dx.utils_geo._consolidate_subdivide_geometry(poly)

    # graph from bounding box
    _ = dx.utils_geo.bbox_from_point(location_point, dist=1000, project_utm=True, return_crs=True)
    bbox = dx.utils_geo.bbox_from_point(location_point, dist=500)
    G = dx.graph_from_bbox(bbox, network_type="drive")
    G = dx.graph_from_bbox(bbox, network_type="drive_service", truncate_by_edge=True)

    # truncate graph by bounding box
    bbox = dx.utils_geo.bbox_from_point(location_point, dist=400)
    G = dx.truncate.truncate_graph_bbox(G, bbox)
    G = dx.truncate.largest_component(G, strongly=True)

    # graph from address
    G = dx.graph_from_address(address=address, dist=500, dist_type="bbox", network_type="bike")

    # graph from list of places
    G = dx.graph_from_place([place1], which_result=[None], network_type="all")

    # graph from polygon
    G = dx.graph_from_polygon(polygon, network_type="walk", truncate_by_edge=True, simplify=False)
    G = dx.simplify_graph(
        G,
        node_attrs_include=["junction", "ref"],
        edge_attrs_differ=["osmid"],
        remove_rings=False,
        track_merged=True,
    )

    # test custom query filter
    cf = (
        '["highway"]'
        '["area"!~"yes"]'
        '["highway"!~"motor|proposed|construction|abandoned|platform|raceway"]'
        '["foot"!~"no"]'
        '["service"!~"private"]'
        '["access"!~"private"]'
    )
    G = dx.graph_from_point(
        location_point,
        dist=500,
        custom_filter=cf,
        dist_type="bbox",
        network_type="all_public",
    )

    # test union of multiple custom filters
    cf_union = ['["highway"~"tertiary"]', '["railway"~"tram"]']
    G = dx.graph_from_point(location_point, dist=500, custom_filter=cf_union, retain_all=True)

    G = dx.graph_from_point(
        location_point,
        dist=500,
        dist_type="network",
        network_type="all",
    )


@pytest.mark.xdist_group(name="group3")
def test_features() -> None:
    """Test downloading features from Overpass."""
    bbox = dx.utils_geo.bbox_from_point(location_point, dist=500)
    tags1: dict[str, bool | str | list[str]] = {"landuse": True, "building": True, "highway": True}

    with pytest.raises(ValueError, match="The geometry of `polygon` is invalid."):
        dx.features.features_from_polygon(Polygon(((0, 0), (0, 0), (0, 0), (0, 0))), tags={})
    with suppress_type_checks(), pytest.raises(TypeError):
        dx.features.features_from_polygon(Point(0, 0), tags={})

    # features_from_bbox - bounding box query to return no data
    with pytest.raises(dx._errors.InsufficientResponseError):
        gdf = dx.features_from_bbox(bbox=(-2.001, -2.001, -2.000, -2.000), tags={"building": True})

    # features_from_bbox - successful
    gdf = dx.features_from_bbox(bbox, tags=tags1)
    fig, ax = dx.plot_footprints(gdf)
    fig, ax = dx.plot_footprints(gdf, ax=ax, bbox=(0, 0, 10, 10))

    # features_from_bbox - test < -80 deg latitude
    tags2: dict[str, bool | str | list[str]] = {"natural": True, "amenity": True}
    bbox = dx.utils_geo.bbox_from_point(polar_point_south, dist=500)
    gdf = dx.features_from_bbox(bbox, tags=tags2)

    # features_from_bbox - test > 84 deg latitude
    bbox = dx.utils_geo.bbox_from_point(polar_point_north, dist=500)
    gdf = dx.features_from_bbox(bbox, tags=tags2)

    # features_from_point - tests multipolygon creation
    gdf = dx.utils_geo.bbox_from_point(location_point, dist=500)

    # features_from_place - includes test of list of places
    tags3: dict[str, bool | str | list[str]] = {
        "amenity": True,
        "landuse": ["retail", "commercial"],
        "highway": "bus_stop",
    }
    gdf = dx.features_from_place(place1, tags=tags3)
    gdf = dx.features_from_place([place1], which_result=[None], tags=tags3)

    # features_from_polygon
    polygon = dx.geocode_to_gdf(place1).geometry.iloc[0]
    dx.features_from_polygon(polygon, tags3)

    # features_from_address
    gdf = dx.features_from_address(address, tags=tags3, dist=1000)
