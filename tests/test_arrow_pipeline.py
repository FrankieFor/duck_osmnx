"""Tests for Arrow pipeline in PBF reader."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from shapely import Polygon

from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import settings

PBF_PATH = Path("berlin-latest.osm.pbf")
BBOX_POLYGON = Polygon([
    (13.38, 52.51), (13.39, 52.51), (13.39, 52.52), (13.38, 52.52), (13.38, 52.51)
])


@pytest.fixture(autouse=True)
def _setup_pbf():
    """Configure PBF path and reset connection."""
    if not PBF_PATH.exists():
        pytest.skip("Berlin PBF not available")
    settings.pbf_file_path = str(PBF_PATH)
    _duckdb.close()
    yield
    _duckdb.close()


def test_network_returns_arrow_tables() -> None:
    """Test that _read_pbf_network_duckdb returns Arrow tables."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    assert isinstance(nodes, pa.Table), f"Expected Arrow Table, got {type(nodes)}"
    assert isinstance(ways, pa.Table), f"Expected Arrow Table, got {type(ways)}"
    assert "id" in nodes.column_names
    assert "y" in nodes.column_names
    assert "x" in nodes.column_names
    assert "osmid" in ways.column_names
    assert "refs" in ways.column_names


def test_features_returns_arrow_tables() -> None:
    """Test that _read_pbf_features_duckdb returns Arrow tables."""
    nodes, ways, rels = _pbf_reader._read_pbf_features_duckdb(
        BBOX_POLYGON, {"building": True}, PBF_PATH,
    )
    assert isinstance(nodes, pa.Table), f"Expected Arrow Table, got {type(nodes)}"
    assert isinstance(ways, pa.Table), f"Expected Arrow Table, got {type(ways)}"
    assert isinstance(rels, pa.Table), f"Expected Arrow Table, got {type(rels)}"


def test_network_arrow_numpy_extraction() -> None:
    """Test zero-copy numpy extraction from Arrow tables."""
    nodes, ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    # Numeric columns should convert to numpy without copy
    y_arr = nodes.column("y").to_numpy()
    x_arr = nodes.column("x").to_numpy()
    assert y_arr.dtype.kind == "f"
    assert x_arr.dtype.kind == "f"
    assert len(y_arr) == len(nodes)
