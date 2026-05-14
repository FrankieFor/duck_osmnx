"""Round-trip shapely -> Rust cluster_assign on tricky polygon shapes.

Skipped automatically when ``ducknx_core`` isn't installed so the default
``pip install ducknx`` path stays green.
"""

from __future__ import annotations

import numpy as np
import pytest
import shapely

pytest.importorskip("ducknx_core")
from ducknx_core import cluster_assign  # noqa: E402


def test_simple_square() -> None:
    sq = shapely.box(0, 0, 1, 1)
    wkb = shapely.to_wkb(sq)
    xs = np.array([0.5, 2.0], dtype=np.float64)
    ys = np.array([0.5, 2.0], dtype=np.float64)
    out = cluster_assign(xs, ys, [wkb])
    assert out[0] == 0
    assert out[1] == -1


def test_polygon_with_hole() -> None:
    outer = shapely.box(0, 0, 10, 10)
    inner = shapely.box(4, 4, 6, 6)
    donut = outer.difference(inner)
    wkb = shapely.to_wkb(donut)
    xs = np.array([1.0, 5.0], dtype=np.float64)
    ys = np.array([1.0, 5.0], dtype=np.float64)
    out = cluster_assign(xs, ys, [wkb])
    assert out[0] == 0    # inside ring
    assert out[1] == -1   # inside the hole


def test_multipolygon() -> None:
    a = shapely.box(0, 0, 1, 1)
    b = shapely.box(10, 10, 11, 11)
    mp = shapely.MultiPolygon([a, b])
    wkb = shapely.to_wkb(mp)
    xs = np.array([0.5, 10.5, 5.0], dtype=np.float64)
    ys = np.array([0.5, 10.5, 5.0], dtype=np.float64)
    out = cluster_assign(xs, ys, [wkb])
    assert out[0] == 0
    assert out[1] == 0
    assert out[2] == -1
