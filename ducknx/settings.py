"""
Global settings that can be configured by the user.

all_oneway : bool
    Only use if subsequently saving graph to an OSM XML file via the
    `save_graph_xml` function. If True, forces all ways to be added as one-way
    ways, preserving the original order of the nodes in the OSM way. This also
    retains the original OSM way's oneway tag's string value as edge attribute
    values, rather than converting them to True/False bool values. Default is
    `False`.
bidirectional_network_types : list[str]
    Network types for which a fully bidirectional graph will be created.
    Default is `["walk"]`.
cache_folder : str | Path
    Path to folder to save/load HTTP response cache files, if the `use_cache`
    setting is True. Default is `"./cache"`.
data_folder : str | Path
    Path to folder to save/load graph files by default. Default is `"./data"`.
default_access : str
    Filter for the OSM "access" tag. Default is `'["access"!~"private"]'`.
    Note that also filtering out "access=no" ways prevents including
    transit-only bridges (e.g., Tilikum Crossing) from appearing in drivable
    road network (e.g., `'["access"!~"private|no"]'`). However, some drivable
    tollroads have "access=no" plus a "access:conditional" tag to clarify when
    it is accessible, so we can't filter out all "access=no" ways by default.
    Best to be permissive here then remove complicated combinations of tags
    programatically after the full graph is downloaded and constructed.
default_crs : str
    Default coordinate reference system to set when creating graphs. Default
    is `"epsg:4326"`.
elevation_url_template : str
    Endpoint of the Google Maps Elevation API (or equivalent), containing
    up to two parameters, in order: `locations` and `key`. Default is:
    `"https://maps.googleapis.com/maps/api/elevation/json?locations={locations}&key={key}"`.
    As alternative free examples, the Open Topo Data API would be:
    `"https://api.opentopodata.org/v1/aster30m?locations={locations}"`
    and the Open-Elevation API would be:
    `"https://api.open-elevation.com/api/v1/lookup?locations={locations}"`.
http_accept_language : str
    HTTP header accept-language. Default is `"en"`. Note that Nominatim's
    default language is "en" and it may sort its results' importance scores
    differently if a different language is specified.
http_referer : str
    HTTP header referer. Default is
    `"ducknx Python package (https://github.com/gboeing/osmnx)"`.
http_user_agent : str
    HTTP header user-agent. Default is
    `"ducknx Python package (https://github.com/gboeing/osmnx)"`.
imgs_folder : str | Path
    Path to folder in which to save plotted images by default. Default is
    `"./images"`.
log_file : bool
    If True, save log output to a file in `logs_folder`. Default is `False`.
log_filename : str
    Name of the log file, without file extension. Default is `"ducknx"`.
log_console : bool
    If True, print log output to the console (terminal window). Default is
    `False`.
log_level : int
    One of Python's `logger.level` constants. Default is `logging.INFO`.
log_name : str
    Name of the logger. Default is `"ducknx"`.
logs_folder : str | Path
    Path to folder in which to save log files. Default is `"./logs"`.
max_query_area_size : float
    Maximum area for any part of the geometry in meters: any polygon bigger
    than this will get divided up for multiple queries. Default is
    `2500000000`.
nominatim_key : str | None
    Your Nominatim API key, if you are using an API instance that requires
    one. Default is `None`.
nominatim_url : str
    The base API url to use for Nominatim queries. Default is
    `"https://nominatim.openstreetmap.org/"`.
pbf_file_path : str | Path | None
    Path to a local OSM PBF file to read data from. All graph and feature
    queries will read from this local file using DuckDB. Must be set to a
    valid PBF file path before using any graph or feature extraction functions.
    Default is `None` (must be configured by user).
requests_kwargs : dict[str, Any]
    Optional keyword args to pass to the requests package when connecting
    to APIs, for example to configure authentication or provide a path to
    a local certificate file. More info on options such as auth, cert,
    verify, and proxies can be found in the requests package advanced docs.
    Default is `{}`.
requests_timeout : int
    The timeout interval in seconds for HTTP requests. Default is `180`.
use_cache : bool
    If True, cache HTTP responses locally in `cache_folder` instead of calling
    API repeatedly for the same request. Default is `True`.
useful_tags_node : list[str]
    OSM "node" tags to add as graph node attributes, when present in the data
    retrieved from OSM. Default is `["highway", "junction", "railway", "ref"]`.
useful_tags_way : list[str]
    OSM "way" tags to add as graph edge attributes, when present in the data
    retrieved from OSM. Default is `["access", "area", "bridge", "est_width",
    "highway", "junction", "landuse", "lanes", "maxspeed", "name", "oneway",
    "ref", "service", "tunnel", "width"]`.
"""

from __future__ import annotations

import logging as lg
from typing import TYPE_CHECKING
from typing import Any

if TYPE_CHECKING:
    from pathlib import Path

all_oneway: bool = False
bidirectional_network_types: list[str] = ["walk"]
cache_folder: str | Path = "./cache"
data_folder: str | Path = "./data"
default_access: str = '["access"!~"private"]'
default_crs: str = "epsg:4326"
elevation_url_template: str = (
    "https://maps.googleapis.com/maps/api/elevation/json?locations={locations}&key={key}"
)
http_accept_language: str = "en"
http_referer: str = "ducknx Python package (https://github.com/gboeing/osmnx)"
http_user_agent: str = "ducknx Python package (https://github.com/gboeing/osmnx)"
imgs_folder: str | Path = "./images"
log_console: bool = False
log_file: bool = False
log_filename: str = "ducknx"
log_level: int = lg.INFO
log_name: str = "ducknx"
logs_folder: str | Path = "./logs"
max_query_area_size: float = 50 * 1000 * 50 * 1000
nominatim_key: str | None = None
nominatim_url: str = "https://nominatim.openstreetmap.org/"
pbf_file_path: str | Path | None = None
requests_kwargs: dict[str, Any] = {}
requests_timeout: float = 180
use_cache: bool = True
useful_tags_node: list[str] = ["highway", "junction", "railway", "ref"]
useful_tags_way: list[str] = [
    "access",
    "area",
    "bridge",
    "est_width",
    "highway",
    "junction",
    "landuse",
    "lanes",
    "maxspeed",
    "name",
    "oneway",
    "ref",
    "service",
    "tunnel",
    "width",
]
