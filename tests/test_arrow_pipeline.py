"""Tests for Arrow pipeline and DuckDB polygon classification."""

from __future__ import annotations

from pathlib import Path
from collections.abc import Generator

import pandas as pd
import pyarrow as pa
import pytest
from shapely import Polygon

from ducknx import _duckdb
from ducknx import _pbf_reader
from ducknx import settings

PBF_PATH = Path("berlin-latest.osm.pbf")
BBOX_POLYGON = Polygon([
    (13.38, 52.51),
    (13.39, 52.51),
    (13.39, 52.52),
    (13.38, 52.52),
    (13.38, 52.51),
])


@pytest.fixture(autouse=True)
def _setup_pbf() -> Generator[None]:
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
    nodes, _ways = _pbf_reader._read_pbf_network_duckdb(
        BBOX_POLYGON, "drive", None, PBF_PATH,
    )
    y_arr = nodes.column("y").to_numpy()
    x_arr = nodes.column("x").to_numpy()
    assert y_arr.dtype.kind == "f"
    assert x_arr.dtype.kind == "f"
    assert len(y_arr) == len(nodes)


def test_duckdb_polygon_classification_matches_python() -> None:
    """Test that DuckDB CASE polygon logic matches Python _should_be_polygon."""
    from ducknx.features import _should_be_polygon  # noqa: PLC0415

    _nodes, ways, _rels = _pbf_reader._read_pbf_features_duckdb(
        BBOX_POLYGON,
        {"building": True},
        PBF_PATH,
    )
    ways_pd = ways.to_pandas() if not isinstance(ways, pd.DataFrame) else ways

    if ways_pd.empty or len(ways_pd) == 0:
        pytest.skip("No ways found")

    mismatches = []
    for _, row in ways_pd.iterrows():
        tags = dict(row["tags"]) if row["tags"] else {}
        sql_says_polygon = bool(row["is_polygon"])
        python_says_polygon = _should_be_polygon(tags)
        if sql_says_polygon != python_says_polygon:
            mismatches.append({
                "id": row["id"],
                "tags": tags,
                "sql": sql_says_polygon,
                "python": python_says_polygon,
            })

    assert len(mismatches) == 0, (
        f"{len(mismatches)} mismatches between SQL and Python polygon classification. "
        f"First 5: {mismatches[:5]}"
    )


def test_build_polygon_case_sql_generates_valid_sql() -> None:
    """Test that _build_polygon_case_sql returns a non-empty CASE expression."""
    case_sql = _pbf_reader._build_polygon_case_sql()
    assert case_sql.strip().startswith("CASE")
    assert "ELSE FALSE" in case_sql
    assert "END" in case_sql
    # Should contain rules for known polygon tags
    assert "'building'" in case_sql
    assert "'amenity'" in case_sql
    assert "'landuse'" in case_sql


def test_build_polygon_case_sql_handles_blocklist() -> None:
    """Test that blocklist rules produce correct NOT IN clauses."""
    case_sql = _pbf_reader._build_polygon_case_sql()
    # aeroway has blocklist with "taxiway"
    assert "'taxiway'" in case_sql
    assert "NOT IN" in case_sql


def test_build_polygon_case_sql_handles_passlist() -> None:
    """Test that passlist rules produce correct IN clauses."""
    case_sql = _pbf_reader._build_polygon_case_sql()
    # highway has passlist with "elevator", "escape", etc.
    assert "'elevator'" in case_sql


def test_build_polygon_case_sql_area_no_first() -> None:
    """Test that area=no check comes before other rules."""
    case_sql = _pbf_reader._build_polygon_case_sql()
    area_no_pos = case_sql.find("tags['area'] = 'no'")
    # The first WHEN should be the area=no check
    first_when_pos = case_sql.find("WHEN")
    assert area_no_pos > first_when_pos  # area=no is in the first WHEN
    # Verify it is the first condition after CASE
    before_area_no = case_sql[first_when_pos:area_no_pos]
    assert "THEN TRUE" not in before_area_no


def test_polygon_classification_with_mixed_tags() -> None:
    """Test polygon classification with various tag combinations."""
    from ducknx.features import _should_be_polygon  # noqa: PLC0415

    _nodes, ways, _rels = _pbf_reader._read_pbf_features_duckdb(
        BBOX_POLYGON,
        {"building": True, "highway": True},
        PBF_PATH,
    )
    ways_pd = ways.to_pandas() if not isinstance(ways, pd.DataFrame) else ways

    if ways_pd.empty or len(ways_pd) == 0:
        pytest.skip("No ways found")

    # All building ways that are closed should be polygons
    for _, row in ways_pd.iterrows():
        tags = dict(row["tags"]) if row["tags"] else {}
        if "building" in tags and tags.get("area") != "no":
            sql_says = bool(row["is_polygon"])
            # If it was classified as polygon, it should be correct
            if sql_says:
                assert _should_be_polygon(tags), (
                    f"Way {row['id']} classified as polygon by SQL but not by Python: {tags}"
                )
