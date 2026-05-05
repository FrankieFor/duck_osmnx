"""Tests for Arrow-native features table builder."""

from __future__ import annotations

import pyarrow as pa
import pytest
from shapely import Point
from shapely import Polygon
from shapely import wkb

from ducknx import features


def _make_nodes_table(rows: list[tuple[int, dict, Point]]) -> pa.Table:
    """Build a nodes Arrow table matching DuckDB output schema."""
    if not rows:
        return pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "tags": pa.array(
                    [],
                    type=pa.map_(pa.string(), pa.string()),
                ),
                "geometry": pa.array([], type=pa.binary()),
            },
        )
    ids = pa.array([r[0] for r in rows], type=pa.int64())
    tags = pa.array(
        [list(r[1].items()) for r in rows],
        type=pa.map_(pa.string(), pa.string()),
    )
    geom = pa.array([wkb.dumps(r[2]) for r in rows], type=pa.binary())
    return pa.table({"id": ids, "tags": tags, "geometry": geom})


def _empty_ways() -> pa.Table:
    """Empty ways Arrow table matching DuckDB schema."""
    return pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "tags": pa.array([], type=pa.map_(pa.string(), pa.string())),
            "refs": pa.array([], type=pa.list_(pa.int64())),
            "geometry": pa.array([], type=pa.binary()),
            "is_polygon": pa.array([], type=pa.bool_()),
        },
    )


def _empty_relations() -> pa.Table:
    """Empty relations Arrow table matching DuckDB schema."""
    return pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "tags": pa.array([], type=pa.map_(pa.string(), pa.string())),
            "geometry": pa.array([], type=pa.binary()),
        },
    )


def test_build_table_nodes_only() -> None:
    """Arrow table is built from nodes-only input."""
    nodes = _make_nodes_table([
        (1, {"amenity": "cafe", "name": "Test Cafe"}, Point(0, 0)),
        (2, {"amenity": "bank"}, Point(1, 1)),
    ])
    bbox_poly = Polygon([(-10, -10), (10, -10), (10, 10), (-10, 10)])
    tags = {"amenity": True}

    tbl = features._build_table(nodes, _empty_ways(), _empty_relations(), bbox_poly, tags)

    assert isinstance(tbl, pa.Table)
    assert tbl.num_rows == 2
    assert tbl.column_names == ["element", "id", "tags", "geometry"]
    elements = tbl.column("element").to_pylist()
    ids = tbl.column("id").to_pylist()
    assert all(e == "node" for e in elements)
    assert sorted(ids) == [1, 2]
    # geometry field carries geoarrow.wkb extension metadata
    geom_meta = tbl.schema.field("geometry").metadata or {}
    assert geom_meta.get(b"ARROW:extension:name") == b"geoarrow.wkb"


def test_build_table_preserves_tags_as_map() -> None:
    """Tags survive as a map<string,string> column, not exploded per key."""
    nodes = _make_nodes_table([
        (1, {"amenity": "cafe", "name": "Joe's Place"}, Point(0, 0)),
    ])
    bbox_poly = Polygon([(-10, -10), (10, -10), (10, 10), (-10, 10)])

    tbl = features._build_table(nodes, _empty_ways(), _empty_relations(), bbox_poly, {"amenity": True})

    # tags column is a single map column, not a sparse explosion of tag keys
    assert pa.types.is_map(tbl.schema.field("tags").type)
    assert "name" not in tbl.column_names
    assert "amenity" not in tbl.column_names
    # values are present and queryable
    row_tags = dict(tbl.column("tags")[0].as_py())
    assert row_tags == {"amenity": "cafe", "name": "Joe's Place"}


def test_build_table_filters_non_matching_tags() -> None:
    """Tag filter excludes rows whose tags don't match the query."""
    nodes = _make_nodes_table([
        (1, {"amenity": "cafe"}, Point(0, 0)),
        (2, {"shop": "bakery"}, Point(1, 1)),
    ])
    bbox_poly = Polygon([(-10, -10), (10, -10), (10, 10), (-10, 10)])

    tbl = features._build_table(nodes, _empty_ways(), _empty_relations(), bbox_poly, {"amenity": True})

    assert tbl.num_rows == 1
    assert tbl.column("id").to_pylist() == [1]


def test_build_table_raises_when_empty() -> None:
    """Empty inputs raise InsufficientResponseError."""
    from ducknx._errors import InsufficientResponseError

    with pytest.raises(InsufficientResponseError):
        features._build_table(
            pa.table({"id": pa.array([], type=pa.int64()),
                     "tags": pa.array([], type=pa.map_(pa.string(), pa.string())),
                     "geometry": pa.array([], type=pa.binary())}),
            _empty_ways(),
            _empty_relations(),
            Polygon([(-1, -1), (1, -1), (1, 1), (-1, 1)]),
            {"amenity": True},
        )
