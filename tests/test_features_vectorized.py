"""Tests for vectorized _create_gdf_from_dfs."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
from shapely import Point
from shapely import Polygon
from shapely import wkb

from ducknx import features


def _make_node_df(nodes: list[tuple[int, dict, Point]]) -> pd.DataFrame:
    """Create a nodes DataFrame matching DuckDB output format."""
    rows = []
    for nid, tags, geom in nodes:
        rows.append({"id": nid, "tags": tags, "geometry": wkb.dumps(geom)})
    return pd.DataFrame(rows)


def _make_empty_df(*cols: str) -> pd.DataFrame:
    """Create an empty DataFrame with given columns."""
    return pd.DataFrame(columns=list(cols))


def test_create_gdf_nodes_only() -> None:
    """Test GeoDataFrame creation with only nodes."""
    nodes_df = _make_node_df([
        (1, {"amenity": "cafe", "name": "Test Cafe"}, Point(0, 0)),
        (2, {"amenity": "bank"}, Point(1, 1)),
    ])
    ways_df = _make_empty_df("id", "tags", "refs", "geometry", "is_polygon")
    relations_df = _make_empty_df("id", "tags", "geometry")

    bbox_poly = Polygon([(-10, -10), (10, -10), (10, 10), (-10, 10)])
    tags = {"amenity": True}

    gdf = features._create_gdf_from_dfs(nodes_df, ways_df, relations_df, bbox_poly, tags)

    assert isinstance(gdf, gpd.GeoDataFrame)
    assert len(gdf) == 2
    assert ("node", 1) in gdf.index
    assert ("node", 2) in gdf.index


def test_create_gdf_preserves_tags() -> None:
    """Test that tags are correctly expanded into columns."""
    nodes_df = _make_node_df([
        (1, {"amenity": "cafe", "name": "Joe's Place"}, Point(0, 0)),
    ])
    ways_df = _make_empty_df("id", "tags", "refs", "geometry", "is_polygon")
    relations_df = _make_empty_df("id", "tags", "geometry")

    bbox_poly = Polygon([(-10, -10), (10, -10), (10, 10), (-10, 10)])
    tags = {"amenity": True}

    gdf = features._create_gdf_from_dfs(nodes_df, ways_df, relations_df, bbox_poly, tags)

    assert gdf.loc[("node", 1), "name"] == "Joe's Place"
    assert gdf.loc[("node", 1), "amenity"] == "cafe"
